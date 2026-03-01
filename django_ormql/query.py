from django.db import models
from django.db.models import F, Value, Q, ExpressionWrapper, BooleanField, aggregates, OrderBy, functions, lookups
from django.db.models.functions import Cast
from sqlglot import parse_one, Dialect, Tokenizer, TokenType, Generator, ParseError
from sqlglot import expressions

from . import db_func
from .exceptions import QueryNotSupported


class OrmqlDialect(Dialect):
    class Tokenizer(Tokenizer):
        QUOTES = ["'", '"']  # todo tests
        IDENTIFIERS = ["`"]  # todo tests

        KEYWORDS = {
            "==": TokenType.EQ,
            "::": TokenType.DCOLON,  # todo tests
            ">=": TokenType.GTE,
            "<=": TokenType.LTE,
            "<>": TokenType.NEQ,
            "!=": TokenType.NEQ,
            "AND": TokenType.AND,
            "ASC": TokenType.ASC,
            "AS": TokenType.ALIAS,
            "BETWEEN": TokenType.BETWEEN,
            "CASE": TokenType.CASE,  # todo tests
            "CURRENT_DATE": TokenType.CURRENT_DATE,  # todo tests
            "CURRENT_TIME": TokenType.CURRENT_TIME,  # todo tests
            "CURRENT_TIMESTAMP": TokenType.CURRENT_TIMESTAMP,  # todo tests
            "DESC": TokenType.DESC,
            "DISTINCT": TokenType.DISTINCT,
            "ELSE": TokenType.ELSE,  # todo tests
            "END": TokenType.END,  # todo tests
            "EXISTS": TokenType.EXISTS,  # todo tests
            "FALSE": TokenType.FALSE,  # todo tests
            "FIRST": TokenType.FIRST,
            "FROM": TokenType.FROM,
            "GROUP BY": TokenType.GROUP_BY,
            "HAVING": TokenType.HAVING,
            "ILIKE": TokenType.ILIKE,  # todo tests
            "IN": TokenType.IN,  # todo tests
            "IS": TokenType.IS,
            "ISNULL": TokenType.ISNULL,
            "LIKE": TokenType.LIKE,  # todo tests
            "LIMIT": TokenType.LIMIT,
            "NOT": TokenType.NOT,
            "NOTNULL": TokenType.NOTNULL,
            "NULL": TokenType.NULL,
            "OFFSET": TokenType.OFFSET,
            "OR": TokenType.OR,
            "ORDER BY": TokenType.ORDER_BY,
            "SELECT": TokenType.SELECT,
            "SOME": TokenType.SOME,  # todo tests
            "THEN": TokenType.THEN,  # todo tests
            "TRUE": TokenType.TRUE,
            "WHEN": TokenType.WHEN,  # todo tests
            "WHERE": TokenType.WHERE,
            # TYPES
            "BOOL": TokenType.BOOLEAN,  # todo tests
            "BOOLEAN": TokenType.BOOLEAN,  # todo tests
            "INT": TokenType.INT,  # todo tests
            "BIGINT": TokenType.BIGINT,  # todo tests
            "DECIMAL": TokenType.DECIMAL,  # todo tests
            "FLOAT": TokenType.FLOAT,  # todo tests
            "DOUBLE": TokenType.DOUBLE,  # todo tests
            # "JSON": TokenType.JSON,
            # "JSONB": TokenType.JSONB,
            "TEXT": TokenType.TEXT,  # todo tests
            "TIME": TokenType.TIME,  # todo tests
            "TIMESTAMPTZ": TokenType.TIMESTAMPTZ,  # todo tests
            "DATE": TokenType.DATE,  # todo tests
            "DATETIME": TokenType.DATETIME,  # todo tests
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
    expressions.ILike: lambda a, b: db_func.Like(functions.Upper(a), functions.Upper(b)),
}

math_binary_nodes = {
    expressions.Mul: db_func.Mul,
    expressions.Add: db_func.Add,
    expressions.Sub: db_func.Sub,
    expressions.Div: db_func.Div,
    expressions.Mod: db_func.Mod,
}

aggregate_nodes = {
    expressions.Avg: aggregates.Avg,  # todo tests
    expressions.Count: aggregates.Count,  # todo tests
    expressions.Max: aggregates.Max,  # todo tests
    expressions.Min: aggregates.Min,  # todo tests
    expressions.Stddev: aggregates.StdDev,  # todo tests
    expressions.Variance: aggregates.Variance,  # todo tests
    expressions.Sum: aggregates.Sum,  # todo tests
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
        return Value(expression.to_py())
    elif isinstance(expression, expressions.Star):
        return "*"
    elif isinstance(expression, expressions.Cast):
        return Cast(
            expression_to_django(expression.this, table, aggregate_names),
            output_field=types[expression.to.this],
        )
    elif type(expression) in aggregate_nodes:
        # TODO: COUNT(*) FILTER …
        # TODO: COUNT(a, b)
        if isinstance(expression.this, expressions.Distinct):
            args = [expression_to_django(e, table) for e in expression.this.expressions]
            distinct = True
        else:
            args = [expression_to_django(expression.this, table)]
            distinct = False
        return aggregate_nodes[type(expression)](*args, distinct=distinct)
    elif type(expression) in math_binary_nodes:
        lhs = expression_to_django(expression.left, table)
        rhs = expression_to_django(expression.right, table)
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
        return expression_to_django(expression.this, table, aggregate_names)
    elif isinstance(expression, expressions.Neg):
        return -expression_to_django(expression.this, table, aggregate_names)
    elif isinstance(expression, (
            expressions.BitwiseNot,
            expressions.BitwiseOr,
            expressions.BitwiseXor,
            expressions.BitwiseAnd,
            expressions.BitwiseCount,
            expressions.BitwiseLeftShift,
            expressions.BitwiseRightShift,
    )):
        raise QueryNotSupported("Bitwise operations not supported")
    elif type(expression) in boolean_expression_nodes:
        return ExpressionWrapper(
            boolean_expression_nodes[type(expression)](
                expression_to_django(expression.left, table, aggregate_names),
                expression_to_django(expression.right, table, aggregate_names),
            ),
            output_field=BooleanField()
        )
    elif isinstance(expression, expressions.Between):
        return Q(ExpressionWrapper(
            db_func.GreaterEqualThan(
                expression_to_django(expression.this, table, aggregate_names),
                expression_to_django(expression.args["low"], table, aggregate_names),
            ),
            output_field=BooleanField()
        )) & Q(ExpressionWrapper(
            db_func.LowerEqualThan(
                expression_to_django(expression.this, table, aggregate_names),
                expression_to_django(expression.args["high"], table, aggregate_names),
            ),
            output_field=BooleanField()
        ))
    elif isinstance(expression, expressions.In):
        return ExpressionWrapper(lookups.In(
            expression_to_django(expression.this, table, aggregate_names),
            [expression_to_django(e, table, aggregate_names) for e in expression.expressions],
        ), output_field=BooleanField())
    elif isinstance(expression, expressions.And):
        return expression_to_django(expression.left, table, aggregate_names) & expression_to_django(expression.right, table, aggregate_names)
    elif isinstance(expression, expressions.Or):
        return expression_to_django(expression.left, table, aggregate_names) | expression_to_django(expression.right, table, aggregate_names)
    elif isinstance(expression, expressions.Not):
        return ~expression_to_django(expression.this, table, aggregate_names)
    else:
        # TODO: CASE ... WHEN
        # TODO: better error message (expression.sql() not supported here)
        raise NotImplementedError(f"TODO implement expression type {type(expression)}")


def expression_to_name(expression):
    if isinstance(expression, (expressions.Column, expressions.Dot)):
        return ".".join(to_column_path(expression))
    elif isinstance(expression, expressions.Literal):
        return str(expression.this)
    elif isinstance(expression, expressions.Alias):
        return expression.output_name
    elif type(expression) in aggregate_nodes:
        return expression.sql()
    else:
        return expression.sql()


def where_to_django(node, table, aggregate_names):
    return expression_to_django(node, table, aggregate_names)


def sql_to_queryset(sql, tables):
    try:
        ast = parse_one(sql, dialect=OrmqlDialect)
    except ParseError as e:
        raise QueryNotSupported(str(e)) from e

    print(repr(ast))

    if not isinstance(ast, expressions.Select):
        raise QueryNotSupported("Only SELECT queries are supported")

    table = ast.args["from_"].this
    if not isinstance(table, expressions.Table):
        raise QueryNotSupported("Unsupported FROM statement")
    if table.args.get("alias"):
        raise QueryNotSupported("Table alias not supported")
    if table.args.get("db"):
        raise QueryNotSupported("Database names not supported")
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

    if ast.args.get("distinct"):
        qs = qs.distinct()

    order_by = []
    if ast.args.get("order"):
        for i, ordered in enumerate(ast.args["order"].args["expressions"]):
            if isinstance(ordered.this, expressions.Column) and isinstance(ordered.this.this,
                                                                           expressions.Identifier) and ordered.this.this.this in name_to_aggregation:
                order_by.append(
                    OrderBy(
                        F(name_to_aggregation[ordered.this.this.this]),
                        descending=ordered.args["desc"],
                        nulls_first=ordered.args["nulls_first"],
                        nulls_last=not ordered.args["nulls_first"],
                    )
                )
            else:
                order_by.append(
                    OrderBy(
                        expression_to_django(ordered.this, table),
                        descending=ordered.args["desc"],
                        nulls_first=ordered.args["nulls_first"],
                        nulls_last=not ordered.args["nulls_first"],
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
            if not isinstance(ast.args["limit"].expression, expressions.Literal):
                raise QueryNotSupported("LIMIT may only contain literal numbers")
            if not isinstance(ast.args["offset"].expression, expressions.Literal):
                raise QueryNotSupported("OFFSET may only contain literal numbers")
            offset = int(ast.args["offset"].expression.this)
            limit = int(ast.args["limit"].expression.this)
            qs = qs[offset:offset + limit]
        elif ast.args.get("limit"):
            if not isinstance(ast.args["limit"].expression, expressions.Literal):
                raise QueryNotSupported("LIMIT may only contain literal numbers")
            limit = int(ast.args["limit"].expression.this)
            qs = qs[:limit]
        elif ast.args.get("offset"):
            if not isinstance(ast.args["offset"].expression, expressions.Literal):
                raise QueryNotSupported("OFFSET may only contain literal numbers")
            offset = int(ast.args["offset"].expression.this)
            qs = qs[offset:]

        print(qs, qs.query)
        for row in qs:
            yield {
                values_names[k]: v
                for k, v in row.items()
                if k in values_names
            }
