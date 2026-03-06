import inspect

from django.db.models import F, Expression, OuterRef, Subquery
from django.utils import tree
from django.utils.module_loading import import_string

from django_ormql.exceptions import QueryNotSupported


class BaseColumn:
    def __init__(self, **kwargs):
        self.source = kwargs.get("source")

    def bind(self, field_name, parent):
        self.field_name = field_name
        self.parent = parent
        if self.source is None:
            self.source = field_name

    def resolve_column_path(self, remaining_path):
        return F(self.source)


class NumericColumn(BaseColumn):
    pass


class BooleanColumn(BaseColumn):
    pass


class TextColumn(BaseColumn):
    pass


class DateColumn(BaseColumn):
    pass


class DateTimeColumn(BaseColumn):
    pass


class TimeColumn(BaseColumn):
    pass


class DurationColumn(BaseColumn):
    pass


class DecimalColumn(BaseColumn):
    pass


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
            expr.children = children
        elif isinstance(expr, Expression):
            source_expressions = []
            for e in expr.get_source_expressions():
                e = self._prefix_expression(e, prefix)
                source_expressions.append(e)
            expr.set_source_expressions(source_expressions)
        elif isinstance(expr, tuple) and len(expr) == 2:
            # kwarg of Q()
            return f"{prefix}__{expr[0]}", expr[1]
        elif isinstance(expr, OuterRef):
            return OuterRef(f"{prefix}__{expr.name}")
        elif isinstance(expr, F):
            return F(f"{prefix}__{expr.name}")
        elif isinstance(expr, Subquery):
            self._prefix_expression(expr.query.where, self.source)
            return expr
        else:
            raise TypeError(f"Unexpected type {expr!r}")
        return expr

    def bind(self, field_name, parent):
        from .tables import ModelTable

        super().bind(field_name, parent)
        if self.related_table == "self":
            self.related_table = parent
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
        rt = self.related_table()
        if remaining_path:
            related_field = rt.resolve_column_path(remaining_path)

            if isinstance(related_field, F):
                return F("__".join([self.source, related_field.name]))

            elif isinstance(related_field, (Expression, tree.Node)):
                self._prefix_expression(related_field, self.source)
                return related_field

            elif isinstance(related_field, Subquery):
                self._prefix_expression(related_field.query.where, self.source)
                return related_field

            else:
                raise TypeError(f"Unexpected type {type(related_field)}")
        else:
            return F("__".join([self.source, "pk"]))


class GeneratedColumn(BaseColumn):
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
