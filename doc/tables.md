# Defining tables

To make one of your models available as a table in ORMQL, you need to write a `ModelTable` class.
A minimal example could look like this:

```python
from django_ormql.tables import ModelTable
from myapp.models import Category


class CategoryTable(ModelTable):
    class Meta:
        name = "categories"
        model = Category
        columns = [
            "id",
            "title",
        ]
```

In this case, `name` is the name the table will have in ORMQL.
`columns` automatically creates columns for every named field on the model.

## Foreign keys

If you have a foreign key on your model that should be traversable in ORMQL through our auto-join functionality, it needs to be declared explicitly like this:

```python
from django_ormql.columns import ForeignKeyColumn


class ProductTable(ModelTable):
    category = ForeignKeyColumn(CategoryTable)

    class Meta:
        name = "products"
        model = Product
        columns = [
            "id",
            "category",
            "title",
            "price",
        ]
```

In case of circular definitions, you can also use `ForeignKeyColumn("ClassNameInSameFile")`, `ForeignKeyColumn("fully.dotted.class.Location")` or `ForeignKeyColumn("self")`.

## Renamed columns

To rename a column, you can explicitly specify column source.
In the following example, the `name` column in ORMQL references the `title` field on the Django model:

```python
from django_ormql.columns import ModelColumn


class CategoryTable(ModelTable):
    name = ModelColumn(source="title")

    class Meta:
        name = "categories"
        model = Category
        columns = [
            "id",
            "name",
        ]
```

## Dynamic columns

Columns can be generated from Django [query expressions](https://docs.djangoproject.com/en/6.0/ref/models/expressions/).
This can be useful to e.g. always apply a function, map an enum to new values, include a field from a related table, or even include a static column:

```python
from django_ormql.columns import GeneratedColumn


class OrderTable(ModelTable):
    validity = GeneratedColumn(
        Case(
            When(status__in=("new", "paid"), then=Value("valid")),
            default=Value("invalid"),
        )
    )
    email = GeneratedColumn(F("customer__email"))
    email_upper = GeneratedColumn(Upper(F("customer__email")))
    static_value = GeneratedColumn(Value(2))

    class Meta:
        name = "orders"
        model = Order
        columns = [
            "id",
            "created",
            "validity",
            "email",
            "email_upper",
            "static_value",
        ]
```

A `GeneratedColumn` may also contain a subquery, however in this case, `OuterRef` may currently only be used in the `.filter()` part, not anywhere else in the query.

## Columns excluded when related

You can mark colums to be excluded when the table is not queried directly but auto-joined through a related field:

```python
from django_ormql.columns import GeneratedColumn


class OrderTable(ModelTable):

    class Meta:
        name = "orders"
        model = Order
        columns = [
            "id",
            "created",
        ]
        exclude_if_related = [
            "email",
        ]
```