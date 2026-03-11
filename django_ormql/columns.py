import copy
import inspect

from django.db.models import F, Expression, OuterRef, Subquery
from django.db.models.expressions import ResolvedOuterRef
from django.utils import tree
from django.utils.module_loading import import_string

from django_ormql.exceptions import QueryNotSupported


class BaseColumn:
    def __init__(self, **kwargs):
        self.source = kwargs.get("source")
        self._nullable = kwargs.get("nullable")
        self.enum_options = kwargs.get("enum_options", None)

    def bind(self, field_name, parent):
        self.field_name = field_name
        self.parent = parent
        if self.source is None:
            self.source = field_name

    def resolve_column_path(self, remaining_path):
        return F(self.source)

    @property
    def sql_type(self):
        return ""

    @property
    def nullable(self):
        return self._nullable

    @nullable.setter
    def nullable(self, v):
        self._nullable = v


class IntColumn(BaseColumn):
    sql_type = "INT"


class FloatColumn(BaseColumn):
    sql_type = "FLOAT"


class BooleanColumn(BaseColumn):
    sql_type = "BOOLEAN"


class TextColumn(BaseColumn):
    sql_type = "TEXT"


class DateColumn(BaseColumn):
    sql_type = "DATE"


class DateTimeColumn(BaseColumn):
    sql_type = "DATETIME"


class TimeColumn(BaseColumn):
    sql_type = "TIME"


class DurationColumn(BaseColumn):
    sql_type = "DURATION"


class DecimalColumn(BaseColumn):
    sql_type = "DECIMAL"


class JsonColumn(BaseColumn):
    sql_type = "JSONB"


class ModelColumn(BaseColumn):
    # Fallback if no type matches
    pass


class ForeignKeyColumn(BaseColumn):
    def __init__(self, related_table, **kwargs):
        self.related_table = related_table
        super().__init__(**kwargs)

    def _prefix_expression(self, expr, prefix):
        if isinstance(expr, tree.Node):
            children = []
            for e in expr.children:
                e = self._prefix_expression(e, prefix)
                if isinstance(e, F):
                    e = F(f"{prefix}__{expr}")
                children.append(e)
            expr = expr.create(children=children, connector=expr.connector, negated=expr.negated)
        elif isinstance(expr, Expression):
            args, kwargs = expr._constructor_args
            args = [self._prefix_expression(a, prefix) for a in args]
            kwargs = {k: self._prefix_expression(e, prefix) for k, e in kwargs.items()}
            expr = type(expr)(*args, **kwargs)
        elif isinstance(expr, tuple) and len(expr) == 2:
            # kwarg of Q()
            return f"{prefix}__{expr[0]}", expr[1]
        elif isinstance(expr, OuterRef):
            return OuterRef(f"{prefix}__{expr.name}")
        elif isinstance(expr, ResolvedOuterRef):
            return ResolvedOuterRef(f"{prefix}__{expr.name}")
        elif isinstance(expr, F):
            return F(f"{prefix}__{expr.name}")
        elif isinstance(expr, Subquery):
            expr = expr.copy()
            expr.query.where = self._prefix_expression(expr.query.where, self.source)
        return expr

    def bind(self, field_name, parent):
        from .tables import ModelTable

        super().bind(field_name, parent)
        if self.related_table == "self":
            self.related_table = parent.__class__
        elif isinstance(self.related_table, str):
            if "." in self.related_table:
                self.related_table = import_string(self.related_table)
            else:
                self.related_table = getattr(
                    inspect.getmodule(parent), self.related_table
                )
        elif not issubclass(self.related_table, ModelTable):
            raise TypeError("Related field does not point to table")

    def resolve_column_path(self, remaining_path):
        if len(remaining_path) > 20:
            raise QueryNotSupported("Upper limit of JOINs reached.")
        rt = self.related_table(is_related=True)
        if remaining_path:
            related_field = rt.resolve_column_path(remaining_path)

            if isinstance(related_field, ResolvedOuterRef):
                return ResolvedOuterRef("__".join([self.source, related_field.name]))

            elif isinstance(related_field, F):
                return F("__".join([self.source, related_field.name]))

            elif isinstance(related_field, (Expression, tree.Node)):
                return self._prefix_expression(related_field, self.source)

            elif isinstance(related_field, Subquery):
                expr = related_field.copy()
                expr.query.where = self._prefix_expression(
                    related_field.query.where, self.source
                )
                return expr

            else:
                raise TypeError(f"Unexpected type {type(related_field)}")
        else:
            return F("__".join([self.source, "pk"]))


class GeneratedColumn(BaseColumn):
    nullable = True

    def __init__(self, expr, **kwargs):
        self.expr = expr
        super().__init__(**kwargs)

    def resolve_column_path(self, remaining_path):
        return self.expr


def get_column_kwargs(model_field):
    """
    Creates a default instance of a basic non-relational field.
    """
    kwargs = {}

    # The following will only be used by ModelField classes.
    # Gets removed for everything else.
    kwargs["model_field"] = model_field

    if model_field.null:
        kwargs["nullable"] = True

    return kwargs
