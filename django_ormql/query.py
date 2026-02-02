from django.db import models
from django.db.models import F, Value, Q, ExpressionWrapper, BooleanField, aggregates, OrderBy
from django.db.models.functions import Cast
from sqlglot import parse_one, Dialect, Tokenizer, TokenType, Generator
from sqlglot import expressions

from . import db_func
from .exceptions import QueryNotSupported


class OrmqlDialect(Dialect):
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
            "REGEXP": TokenType.RLIKE,
            "SELECT": TokenType.SELECT,
            "SOME": TokenType.SOME,
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
            # "JSON": TokenType.JSON,
            # "JSONB": TokenType.JSONB,
            "TEXT": TokenType.TEXT,
            "TIME": TokenType.TIME,
            "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,
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
    expressions.LTE: db_func.LowerEqualThan
}

math_binary_nodes = {
    expressions.Mul: db_func.Mul,
    expressions.Add: db_func.Add,
    expressions.Sub: db_func.Sub,
    expressions.Div: db_func.Div,
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

types = {
    expressions.DataType.Type.BIGDECIMAL: models.DecimalField(max_digits=20, decimal_places=2),  # TODO variable?
    expressions.DataType.Type.DECIMAL: models.DecimalField(max_digits=20, decimal_places=2),  # TODO variable?
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
    expressions.DataType.Type.DATE: models.DateTimeField(),
}


def to_column_path(expression):
    """
    Hack an expression like part1.part2.part3.part4.part5 to [part1, part2, part3, part4, part5]
    """
    if isinstance(expression, expressions.Dot):
        return [
            *to_column_path(expression.this),
            expression.expression.this,
        ]
    elif isinstance(expression, expressions.Column):
        return [x.this for x in [
            expression.args.get("catalog"),
            expression.args.get("db"),
            expression.args.get("table"),
            expression.args.get("this"),
        ] if x]
    else:
        raise TypeError("Invalid type")


def expression_to_django(expression, table, aggregate_names=None):
    if isinstance(expression, (expressions.Column, expressions.Dot)):
        cp = to_column_path(expression)
        if len(cp) == 1 and aggregate_names and cp[0] in aggregate_names:
            return F(aggregate_names[cp[0]])
        return F(table.resolve_column_path(cp))
    elif isinstance(expression, expressions.Alias):
        return expression_to_django(expression.this, table)
    elif isinstance(expression, expressions.Literal):
        return Value(expression.this)
    elif isinstance(expression, expressions.Cast):
        return Cast(
            expression_to_django(expression.this, table, aggregate_names),
            output_field=types[expression.to.this],
        )
    elif type(expression) in aggregate_nodes:
        return aggregate_nodes[type(expression)](expression_to_django(expression.this, table))
    elif type(expression) in math_binary_nodes:
        # TODO: Auto-set output_field if e.g. INT*DECIMAL is used
        return math_binary_nodes[type(expression)](
            expression_to_django(expression.left, table),
            expression_to_django(expression.right, table),
        )
    else:
        # TODO: mod, neg, ...
        # TODO: CASE ... WHEN
        raise NotImplementedError(f"TODO implement expression type {type(expression)}")


def expression_to_name(expression):
    if isinstance(expression, (expressions.Column, expressions.Dot)):
        return ".".join(to_column_path(expression))
    elif isinstance(expression, expressions.Literal):
        return str(expression.this)
    elif isinstance(expression, expressions.Alias):
        return expression.output_name
    elif type(expression) in aggregate_nodes:
        return "agg_result"
    else:
        raise NotImplementedError(f"TODO implement expression type {type(expression)}")


def where_to_django(node, table, aggregate_names):
    if isinstance(node, expressions.And):
        return where_to_django(node.left, table, aggregate_names) & where_to_django(node.right, table, aggregate_names)
    elif isinstance(node, expressions.Or):
        return where_to_django(node.left, table, aggregate_names) | where_to_django(node.right, table, aggregate_names)
    elif isinstance(node, expressions.Not):
        return ~where_to_django(node.this, table, aggregate_names)
    elif type(node) in boolean_expression_nodes:
        return Q(
            ExpressionWrapper(
                boolean_expression_nodes[type(node)](
                    expression_to_django(node.left, table, aggregate_names),
                    expression_to_django(node.right, table, aggregate_names),
                ),
                output_field=BooleanField()
            )
        )
    else:
        return expression_to_django(node, table)
    # TODO: LIKE, ILIKE, IS, MATCH, ...


def sql_to_queryset(sql, tables):
    ast = parse_one(sql, dialect=OrmqlDialect)
    print(repr(ast))

    if not isinstance(ast, expressions.Select):
        raise QueryNotSupported("Only SELECT queries are supported")

    table = ast.args["from_"].this
    if not isinstance(table, expressions.Table):
        raise QueryNotSupported("Unsupported FROM statement")
    if ast.args.get("joins"):
        raise QueryNotSupported("SELECT from multiple tables not supported")

    if table.this.this not in tables:
        raise QueryNotSupported(f"Table {table.this} not found")

    table = tables[table.this.this]

    qs = table.base_qs

    if ast.args.get("where"):
        qs = qs.filter(where_to_django(ast.args["where"].this, table, []))

    group_args = []
    if ast.args.get("group"):
        for i, e in enumerate(ast.args["group"]):
            django_e = expression_to_django(e, table)
            group_args.append(django_e)

    # TODO: DISTINCT

    values_args = {}
    values_names = {}
    aggregations = {}
    name_to_aggregation = {}
    for i, e in enumerate(ast.args["expressions"]):
        if isinstance(e, expressions.Star):
            raise QueryNotSupported("SELECT * is not supported")
        else:
            n = expression_to_name(e)
            while n in values_names or n in aggregations:
                n += "_"

            # TODO validate that everything is an aggregate, part of the grouping, or a literal
            django_e = expression_to_django(e, table)
            if isinstance(django_e, aggregates.Aggregate):
                aggregations[f"expr{i}"] = django_e
                values_names[f"expr{i}"] = n
                name_to_aggregation[n] = f"expr{i}"
            else:
                values_args[f"expr{i}"] = expression_to_django(e, table)
                values_names[f"expr{i}"] = n

    order_by = []
    if ast.args.get("order"):
        for i, ordered in enumerate(ast.args["order"].args["expressions"]):
            if isinstance(ordered.this, expressions.Column) and isinstance(ordered.this.this, expressions.Identifier) and ordered.this.this.this in name_to_aggregation:
                order_by.append(
                    OrderBy(
                        F(name_to_aggregation[ordered.this.this.this]),
                        descending=ordered.args["desc"],
                        nulls_first=ordered.args["nulls_first"],
                    )
                )
            else:
                order_by.append(
                    OrderBy(
                        expression_to_django(ordered.this, table),
                        descending=ordered.args["desc"],
                        nulls_first=ordered.args["nulls_first"],
                    )
                )

    if aggregations:
        if group_args:  # todo what happens if we have group without aggregates?
            qs = qs.order_by().annotate(**{
                f"grp{i}": v for i, v in enumerate(group_args)
            }, **values_args).values(*[
                f"grp{i}" for i, v in enumerate(group_args)
            ], *values_args.keys()).annotate(
                **aggregations
            )

            if ast.args.get("having"):
                qs = qs.filter(where_to_django(ast.args["having"].this, table, name_to_aggregation))
        else:
            qs = qs.aggregate(**aggregations)
    else:
        qs = qs.values(**values_args)

    if order_by:
        qs = qs.order_by(*order_by)

    if isinstance(qs, dict):
        yield {
            values_names[k]: v
            for k, v in qs.items()
        }
    else:
        if ast.args.get("offset") and ast.args.get("limit"):
            offset = int(ast.args.get("offset").expression.this)
            limit = int(ast.args.get("limit").expression.this)
            qs = qs[offset:offset + limit]
        elif ast.args.get("limit"):
            limit = int(ast.args.get("limit").expression.this)
            qs = qs[:limit]
        elif ast.args.get("offset"):
            offset = int(ast.args.get("offset").expression.this)
            qs = qs[offset:]

        print(qs.query)
        for row in qs:
            yield {
                values_names[k]: v
                for k, v in row.items()
                if k in values_names
            }
