import os
from datetime import date, datetime
from decimal import Decimal

from django.conf import settings
from django.db import models
from django.db.models import (
    BooleanField,
    ExpressionWrapper,
    F,
    OuterRef,
    Q,
    Value,
    aggregates,
    functions,
    lookups,
)
from django.db.models.fields.json import KeyTextTransform, KeyTransform
from sqlglot import expressions

from . import db_func
from .db_func import _patch_func
from .exceptions import QueryError, QueryNotSupported


class Expression:
    node_class = None
    sql_type = None

    def matches(self, expression):
        return isinstance(expression, self.node_class)

    def to_django(self, expression, recurse, **kwargs):
        pass

    def to_name(self, expression):
        return expression.sql()

    def to_sql_type(self, expression, recurse, **kwargs):
        return self.sql_type

    def is_aggregate(self, expression, recurse, **kwargs):
        for args in expression.args.values():
            if isinstance(args, list):
                if any(recurse(a) for a in args):
                    return True
            if recurse(args):
                return True
        return False


class FuncExpression(Expression):
    node_class = expressions.Anonymous
    func_name = None

    def matches(self, expression):
        return super().matches(expression) and expression.this.lower() == self.func_name


_expressions = []


def register(cls):
    _expressions.append(cls())
    return cls


def expression_to_django(expression, recurse, **kwargs):
    for e in _expressions:
        if e.matches(expression):
            return e.to_django(expression, recurse, **kwargs)
    raise QueryNotSupported(f"Unsupported expression: {expression.sql()}")


def expression_to_name(expression):
    for e in _expressions:
        if e.matches(expression):
            return e.to_name(expression)
    return expression.sql()


def expression_to_sql_type(expression, **kwargs):
    for e in _expressions:
        if e.matches(expression):
            return e.to_sql_type(expression, expression_to_sql_type, **kwargs)
    return None


def expression_is_aggregate(expression, **kwargs):
    for e in _expressions:
        if e.matches(expression):
            return e.is_aggregate(expression, expression_is_aggregate, **kwargs)
    return None


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


# Maps Django's Field.get_internal_type() to the ORMQL sql_type vocabulary
# emitted by BaseColumn.sql_type subclasses (see columns.py). Keeping one
# vocabulary means /tables/ introspection and per-query result metadata speak
# the same language.
INTERNAL_TYPE_TO_SQL_TYPE = {
    "DateField": "DATE",
    "DateTimeField": "DATETIME",
    "TimeField": "TIME",
    "DurationField": "DURATION",
    "IntegerField": "INT",
    "BigIntegerField": "INT",
    "SmallIntegerField": "INT",
    "PositiveIntegerField": "INT",
    "PositiveSmallIntegerField": "INT",
    "PositiveBigIntegerField": "INT",
    "AutoField": "INT",
    "BigAutoField": "INT",
    "SmallAutoField": "INT",
    "DecimalField": "DECIMAL",
    "FloatField": "FLOAT",
    "BooleanField": "BOOLEAN",
    "JSONField": "JSONB",
    "CharField": "TEXT",
    "TextField": "TEXT",
    "EmailField": "TEXT",
    "URLField": "TEXT",
    "SlugField": "TEXT",
    "UUIDField": "TEXT",
    "GenericIPAddressField": "TEXT",
    "FileField": "TEXT",
    "FilePathField": "TEXT",
    "ImageField": "TEXT",
    "BinaryField": "TEXT",
}


def _describe_expression(expr, query=None):
    """Extract (sql_type, nullable, hint) from a Django expression's output_field.
    Hint is dependent on type and currently only available for truncated datetimes.

    Aggregates and Cast() / typed Func() have `.output_field` directly. Plain
    F() references only know their field once resolved against a query; when
    `query` is supplied, we resolve on a cloned query (so we don't mutate the
    original with extra joins) and read `output_field` off the resolved node.

    Returns ("", None, None) for anything we can't pin down — the frontend treats
    that as "unknown, render opaquely" which matches the pre-metadata
    behavior.
    """
    out = None
    try:
        out = expr.output_field
    except:  # noqa
        out = None
    if out is None and query is not None:
        try:
            resolved = expr.resolve_expression(query=query.chain(), allow_joins=True)
            out = resolved.output_field
        except:  # noqa
            out = None
    if out is None:
        return "", None, None
    try:
        internal = out.get_internal_type()
    except:  # noqa
        return "", None, None
    sql_type = INTERNAL_TYPE_TO_SQL_TYPE.get(internal, "")
    nullable = getattr(out, "null", None)
    hint = None

    if isinstance(expr, functions.datetime.TruncBase):
        hint = {"truncated": expr.kind}

    return sql_type, nullable, hint


def _to_column_path(expression):
    """
    Hack an expression like part1.part2.part3.part4.part5 to [part1, part2, part3, part4, part5]
    """
    if isinstance(expression, expressions.Dot):
        return [
            *_to_column_path(expression.this),
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


@register
class Column(Expression):
    def matches(self, expression):
        return isinstance(expression, (expressions.Column, expressions.Dot))

    def to_django(self, expression, recurse, **kwargs):
        table = kwargs["table"]
        aggregate_names = kwargs["aggregate_names"]
        cp = _to_column_path(expression)
        if len(cp) == 1 and aggregate_names and cp[0] in aggregate_names:
            return F(aggregate_names[cp[0]])
        return table.resolve_column_path(cp)

    def to_name(self, expression):
        return ".".join(_to_column_path(expression))

    def to_sql_type(self, expression, recurse, **kwargs):
        table = kwargs["table"]
        aggregate_names = kwargs["aggregate_names"]
        cp = _to_column_path(expression)
        if len(cp) == 1 and aggregate_names and cp[0] in aggregate_names:
            return "FLOAT"  # we're type-guessing only for now, we don't care if INT or FLOAT or DECIMAL currently
        return table.resolve_column_type(cp)


@register
class Placeholder(Expression):
    node_class = expressions.Placeholder

    def to_django(self, expression, recurse, **kwargs):
        placeholders = kwargs["placeholders"]
        if expression.name == "?":
            raise QueryError("Placeholder must be named")
        if expression.name not in placeholders:
            raise QueryError(f"Placeholder '{expression.name}' not filled")
        return Value(placeholders[expression.name])

    def to_name(self, expression):
        return expression.name

    def to_sql_type(self, expression, recurse, **kwargs):
        placeholders = kwargs.get("placeholders")
        if placeholders is None:
            return None
        if expression.name == "?":
            raise QueryError("Placeholder must be named")
        if expression.name not in placeholders:
            raise QueryError(f"Placeholder '{expression.name}' not filled")
        v = placeholders[expression.name]
        if isinstance(v, str):
            return "TEXT"
        elif isinstance(v, int):
            return "INT"
        elif isinstance(v, float):
            return "FLOAT"
        elif isinstance(v, datetime):
            return "DATETIME"
        elif isinstance(v, date):
            return "DATE"
        elif isinstance(v, bool):
            return "BOOLEAN"
        elif isinstance(v, Decimal):
            return "DECIMAL"
        elif isinstance(v, (dict, list)):
            return "JSONB"
        return None


@register
class Subquery(Expression):
    node_class = expressions.Subquery

    def to_django(self, expression, recurse, **kwargs):
        parent_table_stack = kwargs.get("parent_table_stack", [])
        subquery_builder = kwargs["subquery_builder"]
        table = kwargs["table"]
        if not isinstance(expression.this, expressions.Select):
            raise QueryNotSupported("Only SELECT subqueries are supported")
        qs, _, _ = subquery_builder(
            expression.this, parent_table_stack=parent_table_stack + [table]
        )
        return db_func.AutoTypedSubquery(
            qs,
        )

    def to_sql_type(self, expression, recurse, **kwargs):
        return INTERNAL_TYPE_TO_SQL_TYPE[
            self.to_django(expression, recurse, **kwargs)
            ._resolve_output_field()
            .get_internal_type()
        ]

    def is_aggregate(self, expression, recurse, **kwargs):
        return False  # barrier between query and subquery


@register
class Select(Expression):
    node_class = expressions.Select

    def to_django(self, expression, recurse, **kwargs):
        return Subquery().to_django(
            expressions.Subquery(this=expression), recurse, **kwargs
        )

    def to_sql_type(self, expression, recurse, **kwargs):
        return Subquery().to_sql_type(
            expressions.Subquery(this=expression), recurse, **kwargs
        )


@register
class Exists(Expression):
    node_class = expressions.Exists
    sql_type = "BOOLEAN"

    def to_django(self, expression, recurse, **kwargs):
        parent_table_stack = kwargs.get("parent_table_stack", [])
        subquery_builder = kwargs["subquery_builder"]
        table = kwargs["table"]
        if not isinstance(expression.this, expressions.Select):
            raise QueryNotSupported("Only SELECT subqueries are supported")
        qs, _, _ = subquery_builder(
            expression.this, parent_table_stack=parent_table_stack + [table]
        )
        return models.Exists(
            qs,
        )

    def is_aggregate(self, expression, recurse, **kwargs):
        return False  # barrier between query and subquery


@register
class Alias(Expression):
    node_class = expressions.Alias

    def to_django(self, expression, recurse, **kwargs):
        return recurse(expression.this, **kwargs)

    def to_name(self, expression):
        return expression.output_name

    def to_sql_type(self, expression, recurse, **kwargs):
        return recurse(expression.this, **kwargs)


@register
class Literal(Expression):
    node_class = expressions.Literal

    def to_django(self, expression, recurse, **kwargs):
        return Value(expression.to_py())

    def to_name(self, expression):
        return str(expression.this)

    def to_sql_type(self, expression, recurse, **kwargs):
        v = expression.to_py()
        if isinstance(v, str):
            return "TEXT"
        elif isinstance(v, int):
            return "INT"
        elif isinstance(v, float):
            return "FLOAT"
        elif isinstance(v, datetime):
            return "DATETIME"
        elif isinstance(v, date):
            return "DATE"
        elif isinstance(v, bool):
            return "BOOLEAN"
        elif isinstance(v, Decimal):
            return "DECIMAL"
        elif isinstance(v, (dict, list)):
            return "JSONB"
        return None


@register
class Boolean(Expression):
    node_class = expressions.Boolean
    sql_type = "BOOLEAN"

    def to_django(self, expression, recurse, **kwargs):
        return Value(expression.this)


@register
class Star(Expression):
    node_class = expressions.Star
    sql_type = "TEXT"

    def to_django(self, expression, recurse, **kwargs):
        return "*"


@register
class Cast(Expression):
    node_class = expressions.Cast

    def to_django(self, expression, recurse, **kwargs):
        return models.functions.Cast(
            recurse(expression.this, **kwargs),
            output_field=types[expression.to.this],
        )

    def to_sql_type(self, expression, recurse, **kwargs):
        return INTERNAL_TYPE_TO_SQL_TYPE[types[expression.to.this].get_internal_type()]


@register
class Extract(Expression):
    node_class = expressions.Extract
    sql_type = "INT"

    def to_django(self, expression, recurse, **kwargs):
        timezone = kwargs["timezone"]
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
        if expression_to_sql_type(expression.expression, **kwargs) == "DATE":
            # tzinfo is only supported for datetimes, not dates
            timezone = None
            if lookup_name in ("hour", "minute", "second"):
                raise QueryNotSupported(
                    f"Unsupported extract value '{lookup_name}' for DATE"
                )
        elif expression_to_sql_type(expression.expression, **kwargs) == "TIME":
            # tzinfo is only supported for datetimes, not dates
            timezone = None
            if lookup_name not in ("hour", "minute", "second"):
                raise QueryNotSupported(
                    f"Unsupported extract value '{lookup_name}' for TIME"
                )
        return functions.Extract(
            recurse(expression.expression, **kwargs),
            lookup_name=lookup_name,
            tzinfo=timezone,
        )


@register
class Outer(FuncExpression):
    func_name = "outer"

    def to_django(self, expression, recurse, **kwargs):
        parent_table_stack = kwargs.get("parent_table_stack", [])

        def _resolve(e, parent_stack, depth):
            if isinstance(e, expressions.Anonymous) and e.this.lower() == "outer":
                if not parent_stack:
                    raise QueryError("OUTER nested too far")
                return _resolve(e.expressions[0], parent_stack[:-1], depth + 1)
            elif isinstance(e, (expressions.Column, expressions.Dot)):
                if not parent_stack:
                    raise QueryError("OUTER nested too far")
                return _to_column_path(e), parent_stack[-1], depth
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

    def to_sql_type(self, expression, recurse, **kwargs):
        parent_table_stack = kwargs.get("parent_table_stack", [])

        def _resolve(e, parent_stack, depth):
            if isinstance(e, expressions.Anonymous) and e.this.lower() == "outer":
                if not parent_stack:
                    raise QueryError("OUTER nested too far")
                return _resolve(e.expressions[0], parent_stack[:-1], depth + 1)
            elif isinstance(e, (expressions.Column, expressions.Dot)):
                if not parent_stack:
                    raise QueryError("OUTER nested too far")
                return _to_column_path(e), parent_stack[-1], depth
            else:
                raise QueryNotSupported("Invalid argument to OUTER()")

        cp, lookup_table, depth = _resolve(
            expression.expressions[0], parent_table_stack, 1
        )
        return lookup_table.resolve_column_type(cp)


@register
class Datetrunc(FuncExpression):
    func_name = "datetrunc"

    def to_django(self, expression, recurse, **kwargs):
        timezone = kwargs["timezone"]
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
                raise QueryNotSupported(f"Unsupported truncation type '{lookup_name}'")
        except ValueError:
            raise QueryNotSupported("Unsupported truncation type")
        if expression_to_sql_type(expression.expressions[1], **kwargs) == "DATE":
            # tzinfo is only supported for datetimes, not dates
            timezone = None
            if lookup_name in ("hour", "minute", "second"):
                raise QueryNotSupported(
                    f"Unsupported truncation type '{lookup_name}' for DATE"
                )
        elif expression_to_sql_type(expression.expressions[1], **kwargs) == "TIME":
            # tzinfo is only supported for datetimes, not times
            timezone = None
            if lookup_name not in ("hour", "minute", "second"):
                raise QueryNotSupported(
                    f"Unsupported truncation type '{lookup_name}' for TIME"
                )
        return functions.Trunc(
            recurse(expression.expressions[1], **kwargs),
            lookup_name,
            tzinfo=timezone,
        )

    def to_sql_type(self, expression, recurse, **kwargs):
        if recurse(expression.expressions[1], **kwargs) == "DATE":
            return "DATE"
        if recurse(expression.expressions[1], **kwargs) == "TIME":
            return "TIME"
        return "DATETIME"


class BaseFunction(Expression):
    def matches(self, expression):
        return type(expression) in self.function_nodes

    def to_django(self, expression, recurse, **kwargs):
        if expression.args.get("this"):
            args = [recurse(expression.this, **kwargs)]
        else:
            args = []
        if expression.args.get("expression"):
            args += [recurse(expression.expression, **kwargs)]
        args += [recurse(e, **kwargs) for e in expression.expressions]
        cls = self.function_nodes[type(expression)]
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


@register
class TextFunction(BaseFunction):
    function_nodes = {
        expressions.Concat: db_func.PatchedConcatPair,
        expressions.Left: _patch_func(functions.Left),
        expressions.Right: _patch_func(functions.Right),
        expressions.Lower: _patch_func(functions.Lower),
        expressions.Upper: _patch_func(functions.Upper),
    }
    sql_type = "TEXT"


@register
class TextToIntFunction(BaseFunction):
    function_nodes = {
        expressions.Length: _patch_func(functions.Length),
        expressions.SubstringIndex: _patch_func(functions.StrIndex),
    }
    sql_type = "INT"


@register
class NumericFunction(BaseFunction):
    function_nodes = {
        expressions.Greatest: _patch_func(functions.Greatest),
        expressions.Least: _patch_func(functions.Least),
        expressions.Abs: _patch_func(functions.Abs),
        expressions.Ceil: _patch_func(functions.Ceil),
        expressions.Floor: _patch_func(functions.Floor),
        expressions.Mod: _patch_func(functions.Mod),
    }
    sql_type = "FLOAT"  # we're type-guessing only for now, we don't care if INT or FLOAT or DECIMAL currently


@register
class CoalesceFunction(BaseFunction):
    function_nodes = {
        expressions.Coalesce: _patch_func(functions.Coalesce),
    }

    def to_sql_type(self, expression, recurse, **kwargs):
        if expression.args.get("this"):
            args = [recurse(expression.this, **kwargs)]
        else:
            args = []
        if expression.args.get("expression"):
            args += [recurse(expression.expression, **kwargs)]
        args += [recurse(e, **kwargs) for e in expression.expressions]
        return args[0]


@register
class Round(Expression):
    node_class = expressions.Round
    sql_type = "FLOAT"  # we're type-guessing only for now, we don't care if INT or FLOAT or DECIMAL currently

    def to_django(self, expression, recurse, **kwargs):
        args = [
            recurse(expression.this, **kwargs),
        ]
        if expression.args.get("decimals"):
            args.append(recurse(expression.args["decimals"], **kwargs))
        if any(
            v is not None and k not in ("this", "decimals")
            for k, v in expression.args.items()
        ):
            raise QueryNotSupported(
                f"Wrong number of arguments for function {expression.sql()}"
            )
        return functions.Round(*args)


@register
class Pad(Expression):
    node_class = expressions.Pad
    sql_type = "TEXT"

    def to_django(self, expression, recurse, **kwargs):
        args = [
            recurse(expression.this, **kwargs),
            recurse(expression.expression, **kwargs),
        ]
        if expression.args.get("fill_pattern"):
            args.append(recurse(expression.args["fill_pattern"], **kwargs))
        if any(
            v is not None and k not in ("this", "expression", "fill_pattern", "is_left")
            for k, v in expression.args.items()
        ):
            raise QueryNotSupported(
                f"Wrong number of arguments for function {expression.sql()}"
            )
        if expression.args["is_left"]:
            return functions.LPad(*args)
        else:
            return functions.RPad(*args)


@register
class StrPosition(Expression):
    node_class = expressions.StrPosition
    sql_type = "INT"

    def to_django(self, expression, recurse, **kwargs):
        if not expression.args.get("substr"):
            raise QueryNotSupported(
                f"Wrong number of arguments for function {expression.sql()}"
            )
        args = [
            recurse(expression.this, **kwargs),
            recurse(expression.args["substr"], **kwargs),
        ]
        if any(
            v is not None and k not in ("this", "substr")
            for k, v in expression.args.items()
        ):
            raise QueryNotSupported(
                f"Wrong number of arguments for function {expression.sql()}"
            )
        return functions.StrIndex(*args)


@register
class Substring(Expression):
    node_class = expressions.Substring
    sql_type = "TEXT"

    def to_django(self, expression, recurse, **kwargs):
        if not expression.args.get("start"):
            raise QueryNotSupported(
                f"Wrong number of arguments for function {expression.sql()}"
            )
        args = [
            recurse(expression.this, **kwargs),
            recurse(expression.args["start"], **kwargs),
        ]
        if expression.args.get("length"):
            args.append(recurse(expression.args["length"], **kwargs))
        if any(
            v is not None and k not in ("this", "start", "length")
            for k, v in expression.args.items()
        ):
            raise QueryNotSupported(
                f"Wrong number of arguments for function {expression.sql()}"
            )
        return functions.Substr(*args)


@register
class Replace(Expression):
    node_class = expressions.Replace
    sql_type = "TEXT"

    def to_django(self, expression, recurse, **kwargs):
        args = [
            recurse(expression.this, **kwargs),
            recurse(expression.expression, **kwargs),
        ]
        if expression.args.get("replacement"):
            args.append(recurse(expression.args["replacement"], **kwargs))
        return functions.Replace(*args)


@register
class DPipe(Expression):
    node_class = expressions.DPipe
    sql_type = "TEXT"

    def to_django(self, expression, recurse, **kwargs):
        return functions.Concat(
            recurse(expression.this, **kwargs),
            recurse(expression.expression, **kwargs),
        )


aggregate_nodes = {
    expressions.Avg: aggregates.Avg,
    expressions.Count: aggregates.Count,
    expressions.Max: aggregates.Max,
    expressions.Min: aggregates.Min,
    expressions.Stddev: aggregates.StdDev,
    expressions.Variance: aggregates.Variance,
    expressions.Sum: aggregates.Sum,
}


@register
class Filter(Expression):
    node_class = expressions.Filter
    sql_type = "FLOAT"  # we're type-guessing only for now, we don't care if INT or FLOAT or DECIMAL currently

    def matches(self, node):
        return super().matches(node) and type(node.this) in aggregate_nodes

    def to_django(self, expression, recurse, **kwargs):
        if isinstance(expression.this.this, expressions.Distinct):
            args = [recurse(e, **kwargs) for e in expression.this.this.expressions]
            distinct = True
        else:
            args = [recurse(expression.this.this, **kwargs)]
            distinct = False
        if len(args) > 1:
            raise QueryNotSupported(
                "Multiple arguments to aggregate expression not supported"
            )
        return aggregate_nodes[type(expression.this)](
            *args,
            distinct=distinct,
            filter=recurse(expression.expression.this, **kwargs),
        )

    def is_aggregate(self, expression, recurse, **kwargs):
        return True


@register
class Aggregate(Expression):
    sql_type = "FLOAT"  # we're type-guessing only for now, we don't care if INT or FLOAT or DECIMAL currently

    def matches(self, expression):
        return type(expression) in aggregate_nodes

    def to_django(self, expression, recurse, **kwargs):
        if isinstance(expression.this, expressions.Distinct):
            args = [recurse(e, **kwargs) for e in expression.this.expressions]
            distinct = True
        else:
            args = [recurse(expression.this, **kwargs)]
            distinct = False
        if len(args) > 1:
            raise QueryNotSupported(
                "Multiple arguments to aggregate expression not supported"
            )
        cls = aggregate_nodes[type(expression)]
        return cls(*args, distinct=distinct)

    def is_aggregate(self, expression, recurse, **kwargs):
        return True


math_binary_nodes = {
    expressions.Mul: db_func.Mul,
    expressions.Add: db_func.Add,
    expressions.Sub: db_func.Sub,
    expressions.Div: db_func.Div,
    expressions.Mod: db_func.Mod,
}


@register
class MathBinary(Expression):
    sql_type = "FLOAT"  # we're type-guessing only for now, we don't care if INT or FLOAT or DECIMAL currently

    def matches(self, expression):
        return type(expression) in math_binary_nodes

    def to_django(self, expression, recurse, **kwargs):
        lhs = recurse(expression.left, **kwargs)
        rhs = recurse(expression.right, **kwargs)
        return math_binary_nodes[type(expression)](
            lhs,
            rhs,
        )


@register
class Order(Expression):
    node_class = expressions.Order

    def to_django(self, expression, recurse, **kwargs):
        raise QueryNotSupported("ORDER not supported in expression")


@register
class Null(Expression):
    node_class = expressions.Null
    sql_type = "TEXT"

    def to_django(self, expression, recurse, **kwargs):
        # TODO do we need to guess output_field better?
        return Value(None, output_field=models.TextField(null=True))


@register
class NullSafeEQ(Expression):
    node_class = expressions.NullSafeEQ

    def to_django(self, expression, recurse, **kwargs):
        raise QueryNotSupported("IS (NOT) DISTINCT not supported")


@register
class NullSafeNEQ(Expression):
    node_class = expressions.NullSafeNEQ

    def to_django(self, expression, recurse, **kwargs):
        raise QueryNotSupported("IS (NOT) DISTINCT not supported")


@register
class Paren(Expression):
    node_class = expressions.Paren

    def to_django(self, expression, recurse, **kwargs):
        return recurse(expression.this, **kwargs)

    def to_sql_type(self, expression, recurse, **kwargs):
        return recurse(expression.this, **kwargs)


@register
class Neg(Expression):
    node_class = expressions.Neg

    def to_django(self, expression, recurse, **kwargs):
        return -recurse(expression.this, **kwargs)

    def to_sql_type(self, expression, recurse, **kwargs):
        return recurse(expression.this, **kwargs)


@register
class Bitwise(Expression):
    def matches(self, expression):
        return isinstance(
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
        )

    def to_django(self, expression, recurse, **kwargs):
        raise QueryNotSupported("Bitwise operations not supported")


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


@register
class BooleanOp(Expression):
    sql_type = "BOOLEAN"

    def matches(self, expression):
        return type(expression) in boolean_expression_nodes

    def to_django(self, expression, recurse, **kwargs):
        return ExpressionWrapper(
            boolean_expression_nodes[type(expression)](
                recurse(expression.left, **kwargs),
                recurse(expression.right, **kwargs),
            ),
            output_field=BooleanField(),
        )


@register
class Between(Expression):
    node_class = expressions.Between
    sql_type = "BOOLEAN"

    def to_django(self, expression, recurse, **kwargs):
        return Q(
            ExpressionWrapper(
                db_func.GreaterEqualThan(
                    recurse(expression.this, **kwargs),
                    recurse(expression.args["low"], **kwargs),
                ),
                output_field=BooleanField(),
            )
        ) & Q(
            ExpressionWrapper(
                db_func.LowerEqualThan(
                    recurse(expression.this, **kwargs),
                    recurse(expression.args["high"], **kwargs),
                ),
                output_field=BooleanField(),
            )
        )


@register
class In(Expression):
    node_class = expressions.In
    sql_type = "BOOLEAN"

    def to_django(self, expression, recurse, **kwargs):
        if expression.args.get("query"):
            return ExpressionWrapper(
                lookups.In(
                    recurse(expression.this, **kwargs),
                    recurse(expression.args["query"], **kwargs),
                ),
                output_field=BooleanField(),
            )
        else:
            return ExpressionWrapper(
                lookups.In(
                    recurse(expression.this, **kwargs),
                    [recurse(e, **kwargs) for e in expression.expressions],
                ),
                output_field=BooleanField(),
            )


@register
class And(Expression):
    node_class = expressions.And
    sql_type = "BOOLEAN"

    def to_django(self, expression, recurse, **kwargs):
        return recurse(expression.left, **kwargs) & recurse(expression.right, **kwargs)


@register
class Or(Expression):
    node_class = expressions.Or
    sql_type = "BOOLEAN"

    def to_django(self, expression, recurse, **kwargs):
        return recurse(expression.left, **kwargs) | recurse(expression.right, **kwargs)


@register
class Not(Expression):
    node_class = expressions.Not
    sql_type = "BOOLEAN"

    def to_django(self, expression, recurse, **kwargs):
        return ~recurse(expression.this, **kwargs)


@register
class Case(Expression):
    node_class = expressions.Case

    def to_django(self, expression, recurse, **kwargs):
        default = None
        whens = []
        if expression.this:
            for w in expression.args.get("ifs", []):
                whens.append(
                    models.When(
                        db_func.Equal(
                            recurse(expression.this, **kwargs),
                            recurse(w.this, **kwargs),
                        ),
                        then=recurse(w.args["true"], **kwargs),
                    )
                )
        else:
            for w in expression.args.get("ifs", []):
                whens.append(
                    models.When(
                        recurse(w.this, **kwargs),
                        then=recurse(w.args["true"], **kwargs),
                    )
                )
        if expression.args.get("default"):
            default = recurse(expression.args["default"], **kwargs)
        return db_func.NumericAwareCase(*whens, default=default)

    def to_sql_type(self, expression, recurse, **kwargs):
        if expression.this:
            for w in expression.args.get("ifs", []):
                return recurse(w.args["true"], **kwargs)
        else:
            for w in expression.args.get("ifs", []):
                return recurse(w.args["true"], **kwargs)
        if expression.args.get("default"):
            return recurse(expression.args["default"], **kwargs)


@register
class CurrentDate(Expression):
    node_class = expressions.CurrentDate
    sql_type = "DATE"

    def to_django(self, expression, recurse, **kwargs):
        timezone = kwargs["timezone"]
        return functions.TruncDate(functions.Now(), tzinfo=timezone)


@register
class CurrentTime(Expression):
    node_class = expressions.CurrentTime
    sql_type = "TIME"

    def to_django(self, expression, recurse, **kwargs):
        timezone = kwargs["timezone"]
        return functions.TruncTime(functions.Now(), tzinfo=timezone)


@register
class CurrentTimestamp(Expression):
    node_class = expressions.CurrentTimestamp
    sql_type = "DATETIME"

    def to_django(self, expression, recurse, **kwargs):
        return functions.Now()


@register
class JSONExtract(Expression):
    node_class = expressions.JSONExtract
    django_transform = KeyTransform
    sql_type = "JSONB"

    def to_django(self, expression, recurse, **kwargs):
        if isinstance(expression.expression, expressions.JSONPath):
            k = recurse(expression.this, **kwargs)
            for pathel in expression.expression.expressions:
                if isinstance(pathel, expressions.JSONPathRoot):
                    pass
                elif isinstance(pathel, expressions.JSONPathKey):
                    k = self.django_transform(
                        pathel.this,
                        k,
                    )
                else:
                    raise QueryNotSupported("Advanced JSON path is not supported")
            return k
        elif isinstance(expression.expression, expressions.Literal):
            return self.django_transform(
                expression.expression.this,
                recurse(expression.this, **kwargs),
            )
        elif isinstance(expression.expression, expressions.Column) or isinstance(
            expression.expression, expressions.Identifier
        ):
            return self.django_transform(
                expression.expression.this.this,
                recurse(expression.this, **kwargs),
            )
        else:
            raise QueryNotSupported(f"Unsupported JSON path: {expression.sql()}")


@register
class JSONExtractScalar(JSONExtract):
    node_class = expressions.JSONExtractScalar
    django_transform = KeyTextTransform
    sql_type = "TEXT"


@register
class Lambda(Expression):
    # We do not support lambdas, but the parser emits them for structurs like fun(field->path)
    # which we actually want to interpret as JSON access. Unfortunately, we need to recursively
    # insert our JSONExtract node at the innermost level if we have nested JSON extraction, which
    # makes this more complex.
    node_class = expressions.Lambda

    def _replace_recursively(self, root_field, expr):
        if not isinstance(
            expr, (expressions.JSONExtract, expressions.JSONExtractScalar)
        ):
            raise QueryNotSupported("Invalid usage of JSON lookup")
        if isinstance(expr.this, expressions.Column):
            return expr.__class__(
                this=expressions.JSONExtract(this=root_field, expression=expr.this),
                expression=expr.expression,
            )
        else:
            return expr.__class__(
                this=self._replace_recursively(root_field, expr.this),
                expression=expr.expression,
            )

    def to_django(self, expression, recurse, **kwargs):
        root_field = expressions.Column(this=expression.expressions[0])
        new_expr = self._replace_recursively(root_field, expression.this)
        return expression_to_django(new_expr, recurse, **kwargs)

    def to_sql_type(self, expression, recurse, **kwargs):
        root_field = expressions.Column(this=expression.expressions[0])
        new_expr = self._replace_recursively(root_field, expression.this)
        return expression_to_sql_type(new_expr, **kwargs)


@register
class TypeInfo(FuncExpression):
    func_name = "type_info"
    sql_type = "TEXT"

    def to_django(self, expression, recurse, **kwargs):
        if not settings.DEBUG and "PYTEST_CURRENT_TEST" not in os.environ:
            raise QueryNotSupported(
                "TYPE_INFO not supported in production as it is not a stable API"
            )
        return Value(
            expression_to_sql_type(expression.expressions[0], **kwargs),
            output_field=models.TextField(null=True),
        )

    def is_aggregate(self, expression, recurse, **kwargs):
        return False
