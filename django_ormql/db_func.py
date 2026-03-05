from django.core.exceptions import FieldError
from django.db.models import Func, fields, Value, ExpressionWrapper, Case, Subquery


class Equal(Func):
    arg_joiner = " = "
    arity = 2
    function = ""
    conditional = True


class NotEqual(Func):
    arg_joiner = " != "
    arity = 2
    function = ""
    conditional = True


class GreaterThan(Func):
    arg_joiner = " > "
    arity = 2
    function = ""


class GreaterEqualThan(Func):
    arg_joiner = " >= "
    arity = 2
    function = ""


class LowerEqualThan(Func):
    arg_joiner = " <= "
    arity = 2
    function = ""


class LowerThan(Func):
    arg_joiner = " < "
    arity = 2
    function = ""


class Is(Func):
    arg_joiner = " IS "
    arity = 2
    function = ""


class Like(Func):
    arg_joiner = " LIKE "
    arity = 2
    function = ""


class NumericResolveMixin:
    def _resolve_output_field(self):
        # Auto-resolve of INT*DECIMAL to DECIMAL etc
        source_types = set(
            type(source) for source in self.get_source_fields() if source is not None
        )
        if len(source_types) == 1:
            return list(source_types)[0]()
        elif source_types == {fields.DecimalField, fields.IntegerField}:
            return fields.DecimalField(
                max_digits=max(
                    f.max_digits
                    for f in self.get_source_fields()
                    if isinstance(f, fields.DecimalField)
                ),
                decimal_places=max(
                    f.decimal_places
                    for f in self.get_source_fields()
                    if isinstance(f, fields.DecimalField)
                ),
            )
        elif source_types == {fields.FloatField, fields.IntegerField}:
            return fields.FloatField()
        elif source_types == {fields.FloatField, fields.DecimalField}:
            return fields.DecimalField(
                max_digits=max(
                    f.max_digits
                    for f in self.get_source_fields()
                    if isinstance(f, fields.DecimalField)
                ),
                decimal_places=max(
                    f.decimal_places
                    for f in self.get_source_fields()
                    if isinstance(f, fields.DecimalField)
                ),
            )
        elif source_types == {
            fields.FloatField,
            fields.DecimalField,
            fields.IntegerField,
        }:
            return fields.DecimalField(
                max_digits=max(
                    f.max_digits
                    for f in self.get_source_fields()
                    if isinstance(f, fields.DecimalField)
                ),
                decimal_places=max(
                    f.decimal_places
                    for f in self.get_source_fields()
                    if isinstance(f, fields.DecimalField)
                ),
            )
        else:
            raise FieldError(
                "Expression contains mixed types: %s."
                % ", ".join(t.__name__ for t in source_types)
            )


class Add(NumericResolveMixin, Func):
    arg_joiner = " + "
    arity = 2
    function = ""


class Sub(NumericResolveMixin, Func):
    arg_joiner = " - "
    arity = 2
    function = ""


class Mul(NumericResolveMixin, Func):
    arg_joiner = " * "
    arity = 2
    function = ""


class Div(NumericResolveMixin, Func):
    arg_joiner = " / "
    arity = 2
    function = ""

    def __init__(self, *expressions, output_field=None, **extra):
        # We never want integer division
        super().__init__(
            expressions[0],
            ExpressionWrapper(
                expressions[1] * Value(1.0), output_field=fields.FloatField()
            ),
            output_field=output_field,
            **extra,
        )


class Mod(NumericResolveMixin, Func):
    arg_joiner = " %% "
    arity = 2
    function = ""


class NumericAwareCase(NumericResolveMixin, Case):
    pass


class AutoTypedSubquery(Subquery):
    pass
