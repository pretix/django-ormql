# Running queries

To run queries, you need to create a "query engine" that holds the list of tables that should be available.
Every table needs to be registered with a base queryset:

```python
from django_ormql.engine import QueryEngine

engine = QueryEngine()
engine.register_table(CategoryTable(
    base_qs=Category.objects.filter(tenant=tenant)
))
engine.register_table(ProductTable(
    base_qs=Product.objects.filter(tenant=tenant)
))
```

Then, you can run the query like this:

```python
engine.query(
    """
    SELECT id FROM categories
    """
)
```

The result will be an iterable of dictionaries, of the form

```python
[
    {"id": 1},
    {"id": 2},
]
```

The keys in the dictionaries match the selected columns or aliases.
The value types depend on the queries columns.

You can pass parameters like this:

```python
engine.query(
    """
    SELECT id FROM categories
    WHERE id = :my_id
    """,
    placeholders={"my_id": 3}
)
```

Additionally, you can pass a timezone that is being used for all datetime math in the query, such as extracting parts of a date:

```python
import zoneinfo

engine.query(
    """
    SELECT id FROM categories
    """,
    timezone=zoneinfo.ZoneInfo("Europe/Berlin")
)
```

It does **not** affect the time zone of datetime objects returned in the query results.