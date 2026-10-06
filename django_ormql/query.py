import logging

from django.conf import settings
from django.core.exceptions import FieldError
from django.db import models
from django.db.models import (
    F,
    OrderBy,
    Value,
)
from sqlglot import (
    Dialect,
    Generator,
    ParseError,
    Tokenizer,
    TokenType,
    expressions,
    parse_one,
)
from sqlglot.errors import ANSI_RESET, ANSI_UNDERLINE

from .exceptions import QueryError, QueryNotSupported
from .expressions import (
    _describe_expression,
    expression_is_aggregate,
    expression_to_django,
    expression_to_name,
)

logger = logging.getLogger(__name__)


class OrmqlDialect(Dialect):
    DPIPE_IS_STRING_CONCAT = True
    QUOTE_START = "'"
    QUOTE_END = "'"
    IDENTIFIER_START = "`"
    IDENTIFIER_END = "`"

    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']
        IDENTIFIERS = ["`"]

        KEYWORDS = {
            "==": TokenType.EQ,
            "::": TokenType.DCOLON,
            ">=": TokenType.GTE,
            "<=": TokenType.LTE,
            "<>": TokenType.NEQ,
            "!=": TokenType.NEQ,
            "||": TokenType.DPIPE,
            "->": TokenType.ARROW,
            "->>": TokenType.DARROW,
            "ALL": TokenType.ALL,
            "AND": TokenType.AND,
            "ASC": TokenType.ASC,
            "AS": TokenType.ALIAS,
            "BETWEEN": TokenType.BETWEEN,
            "CASE": TokenType.CASE,
            "CURRENT_DATE": TokenType.CURRENT_DATE,
            "CURRENT_TIME": TokenType.CURRENT_TIME,
            "CURRENT_TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
            "DESC": TokenType.DESC,
            "DISTINCT": TokenType.DISTINCT,
            "ELSE": TokenType.ELSE,
            "END": TokenType.END,
            "EXISTS": TokenType.EXISTS,
            "FALSE": TokenType.FALSE,
            "FILTER": TokenType.FILTER,
            "FIRST": TokenType.FIRST,
            "FROM": TokenType.FROM,
            "GROUP BY": TokenType.GROUP_BY,
            "HAVING": TokenType.HAVING,
            "ILIKE": TokenType.ILIKE,
            "IN": TokenType.IN,
            "IS": TokenType.IS,
            "ISNULL": TokenType.ISNULL,
            "LIKE": TokenType.LIKE,
            "LIMIT": TokenType.LIMIT,
            "NOT": TokenType.NOT,
            "NOTNULL": TokenType.NOTNULL,
            "NULL": TokenType.NULL,
            "OFFSET": TokenType.OFFSET,
            "OR": TokenType.OR,
            "ORDER BY": TokenType.ORDER_BY,
            "SELECT": TokenType.SELECT,
            "THEN": TokenType.THEN,
            "TRUE": TokenType.TRUE,
            "UNION": TokenType.UNION,
            "WHEN": TokenType.WHEN,
            "WHERE": TokenType.WHERE,
            # TYPES
            "BOOL": TokenType.BOOLEAN,
            "BOOLEAN": TokenType.BOOLEAN,
            "INT": TokenType.INT,
            "BIGINT": TokenType.BIGINT,
            "DECIMAL": TokenType.DECIMAL,
            "FLOAT": TokenType.FLOAT,
            "DOUBLE": TokenType.DOUBLE,
            "JSONB": TokenType.JSONB,
            "TEXT": TokenType.TEXT,
            "TIME": TokenType.TIME,
            "DATE": TokenType.DATE,
            "DATETIME": TokenType.DATETIME,
        }

    class Generator(Generator):
        pass


class Result:
    """Iterable query result carrying per-query column metadata.

    Iterating yields row dicts just like the previous evaluate() generator, so
    existing `for row in result:` / `list(result)` callers keep working. The
    `columns` attribute is a list of {"name", "type", "nullable"} dicts using
    the sql_type vocabulary from INTERNAL_TYPE_TO_SQL_TYPE.
    """

    def __init__(self, rows, columns):
        self._rows = rows
        self.columns = columns

    def __iter__(self):
        return iter(self._rows)


class Query:
    def __init__(self, sql, tables, placeholders, timezone, default_limit):
        self.sql = sql
        self.tables = tables
        self.timezone = timezone
        self.placeholders = placeholders or {}
        self.default_limit = default_limit

    def _expression_to_django(self, expression, **kwargs):
        # expected kwargs depending on context:
        # table = kwargs["table"]
        # aggregate_names = kwargs["aggregate_names"]
        # parent_table_stack = kwargs.get("parent_table_stack", [])
        kwargs["timezone"] = self.timezone
        kwargs["placeholders"] = self.placeholders
        kwargs["subquery_builder"] = self._select_to_qs
        return expression_to_django(expression, self._expression_to_django, **kwargs)

    def _where_to_django(self, node, **kwargs):
        return self._expression_to_django(node, **kwargs)

    def _select_to_qs(self, root, parent_table_stack):
        if not isinstance(root, expressions.Select):
            raise QueryNotSupported("Only SELECT queries are supported")

        table = root.args["from_"].this
        if not isinstance(table, expressions.Table):
            raise QueryNotSupported("Unsupported FROM statement")
        if table.args.get("alias"):
            raise QueryNotSupported("Table alias not supported")
        if table.args.get("db"):
            raise QueryNotSupported("Database names not supported")
        if root.args.get("joins"):
            raise QueryNotSupported("SELECT from multiple tables not supported")

        if table.this.this not in self.tables:
            raise QueryNotSupported(f"Table {table.this} not found")

        table = self.tables[table.this.this]

        qs = table.base_qs

        if parent_table_stack:
            qs = qs.order_by()

        if root.args.get("where"):
            qs = qs.filter(
                self._where_to_django(
                    root.args["where"].this,
                    table=table,
                    aggregate_names=[],
                    parent_table_stack=parent_table_stack,
                )
            )

        group_args = []
        if root.args.get("group"):
            for i, e in enumerate(root.args["group"]):
                django_e = self._expression_to_django(
                    e,
                    table=table,
                    aggregate_names=[],
                    parent_table_stack=parent_table_stack,
                )
                group_args.append(django_e)

        values_args = {}
        values_names = {}
        aggregations = {}
        name_to_aggregation = {}
        column_types = {}
        for i, e in enumerate(root.args["expressions"]):
            if isinstance(e, expressions.Star):
                raise QueryNotSupported("SELECT * is not supported")
            else:
                n = expression_to_name(e)
                while n in values_names or n in aggregations:
                    n += "_"

                # TODO We sould validate that everything selected in a GROUP BY query is either an aggregate, part of
                # the grouping, or a literal. However, I have not found a safe way to validate yet and it's not a big deal.
                django_e = self._expression_to_django(
                    e,
                    table=table,
                    aggregate_names=[],
                    parent_table_stack=parent_table_stack,
                )
                if expression_is_aggregate(e):
                    # We do not use the alias names given by the user, first to ensure uniqueness, but also Django has
                    # had some SQL injection vulns recently that affected user-chosen annotate targets. We'll remap
                    # ourselves later.
                    aggregations[f"expr{i}"] = django_e
                    values_names[f"expr{i}"] = n
                    name_to_aggregation[n] = f"expr{i}"
                else:
                    values_args[f"expr{i}"] = self._expression_to_django(
                        e,
                        table=table,
                        aggregate_names=[],
                        parent_table_stack=parent_table_stack,
                    )
                    values_names[f"expr{i}"] = n
                column_types[f"expr{i}"] = _describe_expression(
                    django_e, query=qs.query
                )

        if root.args.get("distinct"):
            qs = qs.distinct()

        order_by = []
        if root.args.get("order"):
            for i, ordered in enumerate(root.args["order"].args["expressions"]):
                if (
                    isinstance(ordered.this, expressions.Column)
                    and isinstance(ordered.this.this, expressions.Identifier)
                    and ordered.this.this.this in name_to_aggregation
                ):
                    order_by.append(
                        OrderBy(
                            F(name_to_aggregation[ordered.this.this.this]),
                            descending=ordered.args["desc"],
                            nulls_first=True if ordered.args["nulls_first"] else None,
                            nulls_last=True
                            if not ordered.args["nulls_first"]
                            else None,
                        )
                    )
                else:
                    order_by.append(
                        OrderBy(
                            self._expression_to_django(
                                ordered.this, table=table, aggregate_names=[]
                            ),
                            descending=ordered.args["desc"],
                            nulls_first=True if ordered.args["nulls_first"] else None,
                            nulls_last=True
                            if not ordered.args["nulls_first"]
                            else None,
                        )
                    )

        if parent_table_stack:
            if len(values_args) + len(aggregations) != 1:
                raise QueryError("Subquery must return exactly 1 column")

        if group_args and not aggregations:
            # Django will not do proper group by without any aggregations, so we need to do trickery
            aggregations = {"_grp_trick": models.Count("*")}

        if aggregations:
            if group_args:
                qs = (
                    qs.order_by()
                    .annotate(
                        **{f"grp{i}": v for i, v in enumerate(group_args)},
                        **values_args,
                    )
                    .values(
                        *[f"grp{i}" for i, v in enumerate(group_args)],
                        *values_args.keys(),
                    )
                    .annotate(**aggregations)
                )

                if root.args.get("having"):
                    qs = qs.filter(
                        self._where_to_django(
                            root.args["having"].this,
                            table=table,
                            aggregate_names=name_to_aggregation,
                        )
                    )
            elif parent_table_stack:
                # Django can't use .aggregate() in subqueries, we need to do trickery
                qs = (
                    qs.annotate(_agg_trick=Value("1"))
                    .values("_agg_trick")
                    .annotate(**aggregations)
                    .values(list(aggregations.keys())[0])
                )
            else:
                if values_args:
                    raise QueryNotSupported(
                        "You can currently not mix aggregate and non-aggregate columns if you "
                        "do not use GROUP BY."
                    )
                qs = qs.aggregate(**aggregations)
        else:
            qs = qs.values(**values_args)

        if not isinstance(qs, dict):
            if order_by:
                qs = qs.order_by(*order_by)

            offset = root.args.get("offset")
            limit = root.args.get("limit")
            if offset:
                if not isinstance(offset.expression, expressions.Literal):
                    raise QueryNotSupported("OFFSET may only contain literal numbers")
                offset = int(offset.expression.this)

            if limit:
                if not isinstance(limit.expression, expressions.Literal):
                    raise QueryNotSupported("LIMIT may only contain literal numbers")
                limit = int(root.args["limit"].expression.this)
            else:
                limit = self.default_limit

            if offset is not None and limit is not None:
                qs = qs[offset : offset + limit]
            elif offset is not None or limit is not None:
                qs = qs[offset:limit]

        return qs, values_names, column_types

    def _flatten_unions(self, root):
        if isinstance(root, expressions.Select):
            return [root]
        elif isinstance(root, expressions.Subquery) and isinstance(
            root.this, expressions.Select
        ):
            return [root.this]
        elif isinstance(root, expressions.Union) and not root.args["distinct"]:
            if (
                root.args.get("limit")
                or root.args.get("order")
                or root.args.get("offset")
            ):
                raise QueryError(
                    "ORDER, LIMIT and OFFSET modifiers are not supported on UNION queries"
                )
            return self._flatten_unions(root.left) + self._flatten_unions(root.right)
        else:
            raise QueryNotSupported(
                "Only SELECT and SELECT ... UNION ALL queries are supported"
            )

    def parse(self):
        try:
            ast = parse_one(self.sql, dialect=OrmqlDialect)
        except ParseError as e:
            msg = str(e).replace(ANSI_UNDERLINE, "").replace(ANSI_RESET, "")
            raise QueryNotSupported(msg) from e

        if settings.DEBUG:
            print(f"Parsed statement: {ast!r}")

        try:
            queries = self._flatten_unions(ast)
            results = [self._select_to_qs(query, []) for query in queries]
            if (
                len(
                    {
                        len(values_names.keys())
                        for qs, values_names, column_types in results
                    }
                )
                != 1
            ):
                raise QueryError(
                    "All parts of UNION query must return same number of columns"
                )
        except QueryError:
            raise
        except FieldError as e:
            raise QueryError("Invalid combination of types") from e
        except Exception as e:
            raise QueryError("Query parsing failed") from e
        return (
            [qs for qs, values_names, column_types in results],
            results[0][1],
            results[0][2],
        )

    def evaluate(self):
        querysets, values_names, column_types = self.parse()
        columns = [
            {
                "name": values_names[k],
                "type": column_types.get(k, ("", None))[0],
                "nullable": column_types.get(k, ("", None))[1],
                "hint": column_types.get(k, ("", None))[2],
            }
            for k in values_names
        ]

        def _iter():
            for qs in querysets:
                if isinstance(qs, dict):
                    yield {values_names[k]: v for k, v in qs.items()}
                else:
                    try:
                        if settings.DEBUG:
                            print(f"Generated statement: {qs.query!s}")
                        for row in qs:
                            yield {
                                values_names[k]: v
                                for k, v in row.items()
                                if k in values_names
                            }
                    except (FieldError, ValueError) as e:
                        raise QueryError("Invalid combination of types") from e

        return Result(_iter(), columns)
