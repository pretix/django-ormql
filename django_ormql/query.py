import logging

from django.db import models
from django.db.models import (
    F,
    Value,
    Q,
    ExpressionWrapper,
    BooleanField,
    aggregates,
    OrderBy,
    functions,
    lookups,
    OuterRef,
)
from django.db.models.fields.json import KeyTransform
from django.db.models.functions import Cast
from sqlglot import parse_one, Dialect, Tokenizer, TokenType, Generator, ParseError
from sqlglot import expressions

from . import db_func
from .db_func import NumericAwareCase
from .exceptions import QueryNotSupported, QueryError

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


boolean_expression_nodes = {
    expressions.EQ: db_func.Equal,
    expressions.NEQ: db_func.NotEqual,
    expressions.GT: db_func.GreaterThan,
    expressions.GTE: db_func.GreaterEqualThan,
    expressions.LT: db_func.LowerThan,
    expressions.LTE: db_func.LowerEqualThan,
    expressions.Is: db_func.Is,
    expressions.Like: db_func.Like,
    expressions.ILike: lambda a, b: db_func.Like(
        functions.Upper(a), functions.Upper(b)
    ),
}

math_binary_nodes = {
    expressions.Mul: db_func.Mul,
    expressions.Add: db_func.Add,
    expressions.Sub: db_func.Sub,
    expressions.Div: db_func.Div,
    expressions.Mod: db_func.Mod,
}

aggregate_nodes = {
    expressions.Avg: aggregates.Avg,
    expressions.Count: aggregates.Count,
    expressions.Max: aggregates.Max,
    expressions.Min: aggregates.Min,
    expressions.Stddev: aggregates.StdDev,
    expressions.Variance: aggregates.Variance,
    expressions.Sum: aggregates.Sum,
}

function_nodes = {
    expressions.Coalesce: functions.Coalesce,
    expressions.Concat: functions.Concat,
    expressions.Greatest: functions.Greatest,
    expressions.Least: functions.Least,
    expressions.Abs: functions.Abs,
    expressions.Ceil: functions.Ceil,
    expressions.Floor: functions.Floor,
    expressions.Mod: functions.Mod,
    expressions.Left: functions.Left,
    expressions.Right: functions.Right,
    expressions.Length: functions.Length,
    expressions.Lower: functions.Lower,
    expressions.Upper: functions.Upper,
    expressions.SubstringIndex: functions.StrIndex,
}

types = {
    expressions.DataType.Type.BIGDECIMAL: models.DecimalField(
        max_digits=20, decimal_places=2
    ),  # TODO variable?
    expressions.DataType.Type.DECIMAL: models.DecimalField(
        max_digits=20, decimal_places=2
    ),  # TODO variable?
    expressions.DataType.Type.BIGINT: models.BigIntegerField(),
    expressions.DataType.Type.BIGSERIAL: models.BigIntegerField(),
    expressions.DataType.Type.INT: models.IntegerField(),
    expressions.DataType.Type.BOOLEAN: models.BooleanField(),
    expressions.DataType.Type.JSON: models.JSONField(),
    expressions.DataType.Type.JSONB: models.JSONField(),
    expressions.DataType.Type.DOUBLE: models.FloatField(),
    expressions.DataType.Type.FLOAT: models.FloatField(),
    expressions.DataType.Type.TEXT: models.TextField(),
    expressions.DataType.Type.TIME: models.TimeField(),
    expressions.DataType.Type.TIMESTAMPTZ: models.DateTimeField(),
    expressions.DataType.Type.DATETIME: models.DateTimeField(),
    expressions.DataType.Type.DATE: models.DateField(),
}


class Query:
    def __init__(self, sql, tables, placeholders, timezone):
        self.sql = sql
        self.tables = tables
        self.timezone = timezone
        self.placeholders = placeholders or {}

    def _to_column_path(self, expression):
        """
        Hack an expression like part1.part2.part3.part4.part5 to [part1, part2, part3, part4, part5]
        """
        if isinstance(expression, expressions.Dot):
            return [
                *self._to_column_path(expression.this),
                expression.expression.this,
            ]
        elif isinstance(expression, expressions.Column):
            return [
                x.this
                for x in [
                    expression.args.get("catalog"),
                    expression.args.get("db"),
                    expression.args.get("table"),
                    expression.args.get("this"),
                ]
                if x
            ]
        else:
            raise TypeError("Invalid type")

    def _expression_to_django(self, expression, **kwargs):
        table = kwargs["table"]
        aggregate_names = kwargs["aggregate_names"]
        parent_table_stack = kwargs.get("parent_table_stack", [])
        if isinstance(expression, (expressions.Column, expressions.Dot)):
            cp = self._to_column_path(expression)
            if len(cp) == 1 and aggregate_names and cp[0] in aggregate_names:
                return F(aggregate_names[cp[0]])
            return table.resolve_column_path(cp)
        elif (
            isinstance(expression, expressions.Anonymous)
            and expression.this.lower() == "outer"
        ):

            def _resolve(e, parent_stack, depth):
                if isinstance(e, expressions.Anonymous) and e.this.lower() == "outer":
                    if not parent_stack:
                        raise QueryError("OUTER nested too far")
                    return _resolve(e.expressions[0], parent_stack[:-1], depth + 1)
                elif isinstance(e, (expressions.Column, expressions.Dot)):
                    if not parent_stack:
                        raise QueryError("OUTER nested too far")
                    return self._to_column_path(e), parent_stack[-1], depth
                else:
                    raise QueryNotSupported("Invalid argument to OUTER()")

            cp, lookup_table, depth = _resolve(
                expression.expressions[0], parent_table_stack, 1
            )
            p = lookup_table.resolve_column_path(cp)
            if isinstance(p, F):
                p = p.name
            else:
                raise QueryNotSupported(f"Cannot use '{cp}' in OUTER()")
            for i in range(depth):
                p = OuterRef(p)
            return p
        elif isinstance(expression, expressions.Alias):
            return self._expression_to_django(expression.this, **kwargs)
        elif isinstance(expression, expressions.Literal):
            return Value(expression.to_py())
        elif isinstance(expression, expressions.Boolean):
            return Value(expression.this)
        elif isinstance(expression, expressions.Star):
            return "*"
        elif isinstance(expression, expressions.Cast):
            return Cast(
                self._expression_to_django(expression.this, **kwargs),
                output_field=types[expression.to.this],
            )
        elif isinstance(expression, expressions.Extract):
            if isinstance(expression.this, expressions.Var):
                lookup_name = expression.this.this.lower()
            else:
                lookup_name = expression.this.to_py()
            if lookup_name not in (
                "year",
                "iso_year",
                "quarter",
                "month",
                "day",
                "week",
                "week_day",
                "iso_week_day",
                "hour",
                "minute",
                "second",
            ):
                raise QueryNotSupported(f"Unsupported extract value '{lookup_name}'")
            return functions.Extract(
                self._expression_to_django(expression.expression, **kwargs),
                lookup_name=lookup_name,
                tzinfo=self.timezone,
            )
        elif (
            isinstance(expression, expressions.Anonymous)
            and expression.this.lower() == "datetrunc"
        ):
            if len(expression.expressions) != 2:
                raise QueryError("Function datetrunc takes exactly two arguments")
            try:
                lookup_name = expression.expressions[0].to_py()
                if lookup_name not in (
                    "year",
                    "quarter",
                    "month",
                    "day",
                    "week",
                    "hour",
                    "minute",
                    "second",
                ):
                    raise QueryNotSupported(
                        f"Unsupported truncation type '{lookup_name}'"
                    )
            except ValueError:
                raise QueryNotSupported("Unsupported truncation type")
            return functions.Trunc(
                self._expression_to_django(expression.expressions[1], **kwargs),
                lookup_name,
                tzinfo=self.timezone,
            )
        elif type(expression) in function_nodes:
            if expression.args.get("this"):
                args = [self._expression_to_django(expression.this, **kwargs)]
            else:
                args = []
            if expression.args.get("expression"):
                args += [self._expression_to_django(expression.expression, **kwargs)]
            args += [
                self._expression_to_django(e, **kwargs) for e in expression.expressions
            ]
            cls = function_nodes[type(expression)]
            if (cls.arity and cls.arity != len(args)) or any(
                v is not None
                and k
                not in (
                    "this",
                    "expression",
                    "expressions",
                    "ignore_nulls",
                    "safe",
                    "coalesce",
                )
                for k, v in expression.args.items()
            ):
                raise QueryNotSupported(
                    f"Wrong number of arguments for function {expression.sql()}"
                )
            return cls(*args)
        elif isinstance(expression, expressions.Round):
            args = [
                self._expression_to_django(expression.this, **kwargs),
            ]
            if expression.args.get("decimals"):
                args.append(
                    self._expression_to_django(expression.args["decimals"], **kwargs)
                )
            if any(
                v is not None and k not in ("this", "decimals")
                for k, v in expression.args.items()
            ):
                raise QueryNotSupported(
                    f"Wrong number of arguments for function {expression.sql()}"
                )
            return functions.Round(*args)
        elif isinstance(expression, expressions.Pad):
            args = [
                self._expression_to_django(expression.this, **kwargs),
                self._expression_to_django(expression.expression, **kwargs),
            ]
            if expression.args.get("fill_pattern"):
                args.append(
                    self._expression_to_django(
                        expression.args["fill_pattern"], **kwargs
                    )
                )
            if any(
                v is not None
                and k not in ("this", "expression", "fill_pattern", "is_left")
                for k, v in expression.args.items()
            ):
                raise QueryNotSupported(
                    f"Wrong number of arguments for function {expression.sql()}"
                )
            if expression.args["is_left"]:
                return functions.LPad(*args)
            else:
                return functions.RPad(*args)
        elif isinstance(expression, expressions.StrPosition):
            if not expression.args.get("substr"):
                raise QueryNotSupported(
                    f"Wrong number of arguments for function {expression.sql()}"
                )
            args = [
                self._expression_to_django(expression.this, **kwargs),
                self._expression_to_django(expression.args["substr"], **kwargs),
            ]
            if any(
                v is not None and k not in ("this", "substr")
                for k, v in expression.args.items()
            ):
                raise QueryNotSupported(
                    f"Wrong number of arguments for function {expression.sql()}"
                )
            return functions.StrIndex(*args)
        elif isinstance(expression, expressions.Substring):
            if not expression.args.get("start"):
                raise QueryNotSupported(
                    f"Wrong number of arguments for function {expression.sql()}"
                )
            args = [
                self._expression_to_django(expression.this, **kwargs),
                self._expression_to_django(expression.args["start"], **kwargs),
            ]
            if expression.args.get("length"):
                args.append(
                    self._expression_to_django(expression.args["length"], **kwargs)
                )
            if any(
                v is not None and k not in ("this", "start", "length")
                for k, v in expression.args.items()
            ):
                raise QueryNotSupported(
                    f"Wrong number of arguments for function {expression.sql()}"
                )
            return functions.Substr(*args)
        elif isinstance(expression, expressions.Replace):
            args = [
                self._expression_to_django(expression.this, **kwargs),
                self._expression_to_django(expression.expression, **kwargs),
            ]
            if expression.args.get("replacement"):
                args.append(
                    self._expression_to_django(expression.args["replacement"], **kwargs)
                )
            return functions.Replace(*args)
        elif isinstance(expression, expressions.DPipe):
            return functions.Concat(
                self._expression_to_django(expression.this, **kwargs),
                self._expression_to_django(expression.expression, **kwargs),
            )
        elif (
            isinstance(expression, expressions.Filter)
            and type(expression.this) in aggregate_nodes
        ):
            if isinstance(expression.this.this, expressions.Distinct):
                args = [
                    self._expression_to_django(e, **kwargs)
                    for e in expression.this.this.expressions
                ]
                distinct = True
            else:
                args = [self._expression_to_django(expression.this.this, **kwargs)]
                distinct = False
            if len(args) > 1:
                raise QueryNotSupported(
                    "Multiple arguments to aggregate expression not supported"
                )
            return aggregate_nodes[type(expression.this)](
                *args,
                distinct=distinct,
                filter=self._expression_to_django(expression.expression.this, **kwargs),
            )
        elif type(expression) in aggregate_nodes:
            if isinstance(expression.this, expressions.Distinct):
                args = [
                    self._expression_to_django(e, **kwargs)
                    for e in expression.this.expressions
                ]
                distinct = True
            else:
                args = [self._expression_to_django(expression.this, **kwargs)]
                distinct = False
            if len(args) > 1:
                raise QueryNotSupported(
                    "Multiple arguments to aggregate expression not supported"
                )
            return aggregate_nodes[type(expression)](*args, distinct=distinct)
        elif type(expression) in math_binary_nodes:
            lhs = self._expression_to_django(expression.left, **kwargs)
            rhs = self._expression_to_django(expression.right, **kwargs)
            return math_binary_nodes[type(expression)](
                lhs,
                rhs,
            )
        elif isinstance(expression, expressions.Order):
            raise QueryNotSupported("ORDER not supported in expression")
        elif isinstance(expression, expressions.Null):
            # TODO do we need to guess output_field better?
            return Value(None, output_field=models.TextField(null=True))
        elif isinstance(expression, (expressions.NullSafeEQ, expressions.NullSafeNEQ)):
            raise QueryNotSupported("IS (NOT) DISTINCT not supported")
        elif isinstance(expression, expressions.Paren):
            return self._expression_to_django(expression.this, **kwargs)
        elif isinstance(expression, expressions.Neg):
            return -self._expression_to_django(expression.this, **kwargs)
        elif isinstance(
            expression,
            (
                expressions.BitwiseNot,
                expressions.BitwiseOr,
                expressions.BitwiseXor,
                expressions.BitwiseAnd,
                expressions.BitwiseCount,
                expressions.BitwiseLeftShift,
                expressions.BitwiseRightShift,
            ),
        ):
            raise QueryNotSupported("Bitwise operations not supported")
        elif type(expression) in boolean_expression_nodes:
            return ExpressionWrapper(
                boolean_expression_nodes[type(expression)](
                    self._expression_to_django(expression.left, **kwargs),
                    self._expression_to_django(expression.right, **kwargs),
                ),
                output_field=BooleanField(),
            )
        elif isinstance(expression, expressions.Between):
            return Q(
                ExpressionWrapper(
                    db_func.GreaterEqualThan(
                        self._expression_to_django(expression.this, **kwargs),
                        self._expression_to_django(expression.args["low"], **kwargs),
                    ),
                    output_field=BooleanField(),
                )
            ) & Q(
                ExpressionWrapper(
                    db_func.LowerEqualThan(
                        self._expression_to_django(expression.this, **kwargs),
                        self._expression_to_django(expression.args["high"], **kwargs),
                    ),
                    output_field=BooleanField(),
                )
            )
        elif isinstance(expression, expressions.In):
            if expression.args.get("query"):
                return ExpressionWrapper(
                    lookups.In(
                        self._expression_to_django(expression.this, **kwargs),
                        self._expression_to_django(expression.args["query"], **kwargs),
                    ),
                    output_field=BooleanField(),
                )
            else:
                return ExpressionWrapper(
                    lookups.In(
                        self._expression_to_django(expression.this, **kwargs),
                        [
                            self._expression_to_django(e, **kwargs)
                            for e in expression.expressions
                        ],
                    ),
                    output_field=BooleanField(),
                )
        elif isinstance(expression, expressions.And):
            return self._expression_to_django(
                expression.left, **kwargs
            ) & self._expression_to_django(expression.right, **kwargs)
        elif isinstance(expression, expressions.Or):
            return self._expression_to_django(
                expression.left, **kwargs
            ) | self._expression_to_django(expression.right, **kwargs)
        elif isinstance(expression, expressions.Not):
            return ~self._expression_to_django(expression.this, **kwargs)
        elif isinstance(expression, expressions.Case):
            default = None
            whens = []
            if expression.this:
                for w in expression.args.get("ifs", []):
                    whens.append(
                        models.When(
                            db_func.Equal(
                                self._expression_to_django(expression.this, **kwargs),
                                self._expression_to_django(w.this, **kwargs),
                            ),
                            then=self._expression_to_django(w.args["true"], **kwargs),
                        )
                    )
            else:
                for w in expression.args.get("ifs", []):
                    whens.append(
                        models.When(
                            self._expression_to_django(w.this, **kwargs),
                            then=self._expression_to_django(w.args["true"], **kwargs),
                        )
                    )
            if expression.args.get("default"):
                default = self._expression_to_django(
                    expression.args["default"], **kwargs
                )
            return NumericAwareCase(*whens, default=default)
        elif isinstance(expression, expressions.CurrentDate):
            return functions.TruncDate(functions.Now(), tzinfo=self.timezone)
        elif isinstance(expression, expressions.CurrentTime):
            return functions.TruncTime(functions.Now(), tzinfo=self.timezone)
        elif isinstance(expression, expressions.CurrentTimestamp):
            return functions.Now()
        elif isinstance(expression, expressions.Subquery):
            if not isinstance(expression.this, expressions.Select):
                raise QueryNotSupported("Only SELECT subqueries are supported")
            qs, _ = self._select_to_qs(
                expression.this, parent_table_stack=parent_table_stack + [table]
            )
            return db_func.AutoTypedSubquery(
                qs,
            )
        elif isinstance(expression, expressions.Exists):
            if not isinstance(expression.this, expressions.Select):
                raise QueryNotSupported("Only SELECT subqueries are supported")
            qs, _ = self._select_to_qs(
                expression.this, parent_table_stack=parent_table_stack + [table]
            )
            return models.Exists(qs)
        elif isinstance(expression, expressions.Placeholder):
            if expression.name == "?":
                raise QueryError("Placeholder must be named")
            if expression.name not in self.placeholders:
                raise QueryError(f"Placeholder '{expression.name}' not filled")
            return Value(self.placeholders[expression.name])
        elif isinstance(expression, expressions.JSONExtract):
            return KeyTransform(
                expression.expression.this.this,
                self._expression_to_django(expression.this, **kwargs),
            )
        else:
            raise QueryNotSupported(f"Unsupported expression: {expression.sql()}")

    def _expression_to_name(self, expression):
        if isinstance(expression, (expressions.Column, expressions.Dot)):
            return ".".join(self._to_column_path(expression))
        elif isinstance(expression, expressions.Literal):
            return str(expression.this)
        elif isinstance(expression, expressions.Alias):
            return expression.output_name
        elif type(expression) in aggregate_nodes:
            return expression.sql()
        else:
            return expression.sql()

    def _where_to_django(self, node, **kwargs):
        return self._expression_to_django(node, **kwargs)

    def _select_to_qs(self, root, parent_table_stack):
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
        for i, e in enumerate(root.args["expressions"]):
            if isinstance(e, expressions.Star):
                raise QueryNotSupported("SELECT * is not supported")
            else:
                n = self._expression_to_name(e)
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
                if isinstance(django_e, aggregates.Aggregate):
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
                qs = qs.aggregate(**aggregations)
        else:
            qs = qs.values(**values_args)

        if not isinstance(qs, dict):
            if order_by:
                qs = qs.order_by(*order_by)

            if root.args.get("offset") and root.args.get("limit"):
                if not isinstance(root.args["limit"].expression, expressions.Literal):
                    raise QueryNotSupported("LIMIT may only contain literal numbers")
                if not isinstance(root.args["offset"].expression, expressions.Literal):
                    raise QueryNotSupported("OFFSET may only contain literal numbers")
                offset = int(root.args["offset"].expression.this)
                limit = int(root.args["limit"].expression.this)
                qs = qs[offset : offset + limit]
            elif root.args.get("limit"):
                if not isinstance(root.args["limit"].expression, expressions.Literal):
                    raise QueryNotSupported("LIMIT may only contain literal numbers")
                limit = int(root.args["limit"].expression.this)
                qs = qs[:limit]
            elif root.args.get("offset"):
                if not isinstance(root.args["offset"].expression, expressions.Literal):
                    raise QueryNotSupported("OFFSET may only contain literal numbers")
                offset = int(root.args["offset"].expression.this)
                qs = qs[offset:]

        return qs, values_names

    def evaluate(self):
        try:
            ast = parse_one(self.sql, dialect=OrmqlDialect)
        except ParseError as e:
            raise QueryNotSupported(str(e)) from e

        print(f"Parsed statement: {ast!r}")

        if not isinstance(ast, expressions.Select):
            raise QueryNotSupported("Only SELECT queries are supported")

        qs, values_names = self._select_to_qs(ast, [])

        if isinstance(qs, dict):
            yield {values_names[k]: v for k, v in qs.items()}
        else:
            print(f"Generated statement: {qs.query!s}")
            for row in qs:
                yield {values_names[k]: v for k, v in row.items() if k in values_names}
