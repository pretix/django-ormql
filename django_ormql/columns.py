class BaseColumn:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')
        pass

    def bind(self, field_name, parent):
        self.field_name = field_name
        self.parent = parent
        if self.source is None:
            self.source = field_name


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
        self.source = kwargs.get('source')


def get_column_kwargs(model_field):
    """
    Creates a default instance of a basic non-relational field.
    """
    kwargs = {}

    # The following will only be used by ModelField classes.
    # Gets removed for everything else.
    kwargs['model_field'] = model_field

    if model_field.null:
        kwargs['nullable'] = True

    return kwargs
