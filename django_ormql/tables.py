import copy

from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.utils.functional import cached_property

from . import model_utils
from .columns import (
    BaseColumn,
    get_column_kwargs,
    ModelColumn,
    BooleanColumn,
    DateColumn,
    DateTimeColumn,
    DecimalColumn,
    DurationColumn,
    TimeColumn,
    TextColumn,
    IntColumn,
    FloatColumn,
    JsonColumn,
)
from .exceptions import QueryError


class BaseTable:
    pass


class TableMetaclass(type):
    """
    This metaclass sets a dictionary named `_declared_columns` on the class.

    Any instances of `Field` included as attributes on either the class
    or on any of its superclasses will be included in the `_declared_columns` dictionary.
    """

    @classmethod
    def _get_declared_columns(cls, bases, attrs):
        columns = [
            (column_name, attrs.pop(column_name))
            for column_name, obj in list(attrs.items())
            if isinstance(obj, BaseColumn)
        ]
        # Ensures a base class column doesn't override cls attrs, and maintains
        # column precedence when inheriting multiple parents. e.g. if there is a
        # class C(A, B), and A and B both define 'column', use 'column' from A.
        known = set(attrs)

        def visit(name):
            known.add(name)
            return name

        base_columns = [
            (visit(name), f)
            for base in bases
            if hasattr(base, "_declared_columns")
            for name, f in base._declared_columns.items()
            if name not in known
        ]

        return dict(base_columns + columns)

    def __new__(cls, name, bases, attrs):
        attrs["_declared_columns"] = cls._get_declared_columns(bases, attrs)
        return super().__new__(cls, name, bases, attrs)


class Table(BaseTable, metaclass=TableMetaclass):
    pass


class ModelTable(Table):
    field_column_mapping = {
        models.AutoField: IntColumn,
        models.BigIntegerField: IntColumn,
        models.BooleanField: BooleanColumn,
        models.CharField: TextColumn,
        models.CommaSeparatedIntegerField: TextColumn,
        models.DateField: DateColumn,
        models.DateTimeField: DateTimeColumn,
        models.DecimalField: DecimalColumn,
        models.DurationField: DurationColumn,
        models.EmailField: TextColumn,
        models.Field: ModelColumn,
        models.FileField: NotImplementedError,
        models.FloatField: FloatColumn,
        models.ImageField: NotImplementedError,
        models.IntegerField: IntColumn,
        models.NullBooleanField: BooleanColumn,
        models.PositiveIntegerField: IntColumn,
        models.PositiveSmallIntegerField: IntColumn,
        models.SlugField: TextColumn,
        models.SmallIntegerField: IntColumn,
        models.TextField: TextColumn,
        models.TimeField: TimeColumn,
        models.URLField: TextColumn,
        models.UUIDField: TextColumn,
        models.GenericIPAddressField: TextColumn,
        models.FilePathField: NotImplementedError,
        models.JSONField: JsonColumn,
    }

    def __init__(self, *, base_qs=None, is_related=False):
        if base_qs is None:
            self.base_qs = self.Meta.model._default_manager.all()
        else:
            self.base_qs = base_qs
        self.is_related = is_related

    @cached_property
    def columns(self):
        """
        A dictionary of {column_name: column_instance}.
        """
        fields = model_utils.BindingDict(self)
        exclude_if_related = getattr(self.Meta, "exclude_if_related", [])
        for key, value in self.get_columns().items():
            if self.is_related and key in exclude_if_related:
                continue
            fields[key] = value
        return fields

    def get_columns(self):
        assert hasattr(self, "Meta"), (
            'Class {table_class} missing "Meta" attribute'.format(
                table_class=self.__class__.__name__
            )
        )
        assert hasattr(self.Meta, "model"), (
            'Class {table_class} missing "Meta.model" attribute'.format(
                table_class=self.__class__.__name__
            )
        )
        if model_utils.is_abstract_model(self.Meta.model):
            raise ValueError("Cannot use ModelSerializer with Abstract Models.")

        declared_columns = copy.deepcopy(self._declared_columns)
        model = getattr(self.Meta, "model")

        # Retrieve metadata about columns & relationships on the model class.
        info = model_utils.get_field_info(model)
        column_names = self.get_column_names(declared_columns, info)

        # Determine the columns that should be included on the table.
        columns = {}

        for column_name in column_names:
            # If the column is explicitly declared on the class then use that.
            if column_name in declared_columns:
                columns[column_name] = declared_columns[column_name]
                if (
                    columns[column_name].nullable is None
                    and column_name in info.forward_relations
                ):
                    columns[column_name].nullable = info.forward_relations[
                        column_name
                    ].model_field.null
                elif (
                    columns[column_name].nullable is None
                    and column_name in info.fields_and_pk
                ):
                    columns[column_name].nullable = info.fields_and_pk[column_name].null
                continue

            column_class, column_kwargs = self.build_column(
                column_name,
                info,
                model,
            )
            columns[column_name] = column_class(**column_kwargs)
        return columns

    def get_column_names(self, declared_columns, info):
        """
        Returns the list of all column names that should be created when
        instantiating this table class. This is based on the default
        set of columns, but also takes into account the `Meta.columns` or
        `Meta.exclude` options if they have been specified.
        """
        columns = getattr(self.Meta, "columns", None)
        if not isinstance(columns, (list, tuple)):
            raise TypeError(
                'The `columns` option must be a list or tuple or "__all__". '
                "Got %s." % type(columns).__name__
            )

        # Ensure that all declared columns have also been included in the
        # `Meta.columns` option.

        # Do not require any columns that are declared in a parent class,
        # in order to allow table subclasses to only include
        # a subset of columns.
        required_column_names = set(declared_columns)
        for cls in self.__class__.__bases__:
            required_column_names -= set(getattr(cls, "_declared_columns", []))

        for column_name in required_column_names:
            assert column_name in columns, (
                "The column '{column_name}' was declared on table "
                "{table_class}, but has not been included in the "
                "'columns' option.".format(
                    column_name=column_name, table_class=self.__class__.__name__
                )
            )
        return columns

    def build_column(self, column_name, info, model_class):
        """
        Return a two tuple of (cls, kwargs) to build a table column with.
        """
        if column_name in info.fields_and_pk:
            model_column = info.fields_and_pk[column_name]
            return self.build_standard_column(column_name, model_column)

        elif column_name in info.relations:
            raise ImproperlyConfigured(
                f"Relational column '{column_name}' needs to be defined explicitly."
            )

        raise ImproperlyConfigured(
            "Field name `%s` is not valid for model `%s` in `%s.%s`."
            % (
                column_name,
                model_class.__name__,
                self.__class__.__module__,
                self.__class__.__name__,
            )
        )

    def build_standard_column(self, column_name, model_column):
        column_mapping = model_utils.ClassLookupDict(self.field_column_mapping)

        column_class = column_mapping[model_column]
        column_kwargs = get_column_kwargs(model_column)

        # Special case to handle when a OneToOneField is also the primary key
        if model_column.one_to_one and model_column.primary_key:
            raise NotImplementedError(
                "Case of a OneToOneField as primary key not handled"
            )

        column_kwargs["nullable"] = model_column.null
        if model_column.choices:
            column_kwargs["enum_options"] = model_column.choices

        return column_class, column_kwargs

    def resolve_column_path(self, column_path):
        for c in column_path:
            if "__" in c:
                raise QueryError("Cannot use __ in column path")
        column_name = column_path[0]
        exclude_if_related = getattr(self.Meta, "exclude_if_related", [])
        if column_name not in self.columns:
            if column_name in exclude_if_related:
                raise QueryError(
                    f"Column '{column_path[0]}' cannot be queried on related tables."
                )
            raise QueryError(
                f"Column '{column_path[0]}' does not exist in table '{self.Meta.name}'."
            )

        column = self.columns[column_name]
        return column.resolve_column_path(column_path[1:])
