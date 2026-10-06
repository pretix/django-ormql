import pytest

from django_ormql.query import Result


@pytest.mark.django_db
def test_result_is_iterable_wrapper(engine_t1):
    res = engine_t1.query("SELECT title FROM categories")
    assert isinstance(res, Result)
    rows = list(res)
    assert rows == [{"title": "Books"}, {"title": "DVDs"}]


@pytest.mark.django_db
def test_columns_simple_fields(engine_t1):
    res = engine_t1.query("SELECT title, price, publication_date FROM products")
    assert res.columns == [
        {"name": "title", "type": "TEXT", "nullable": False, "hint": None},
        {"name": "price", "type": "DECIMAL", "nullable": False, "hint": None},
        {"name": "publication_date", "type": "DATE", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_datetime_field(engine_t1):
    res = engine_t1.query("SELECT created FROM orders")
    assert res.columns == [
        {"name": "created", "type": "DATETIME", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_nullable(engine_t1):
    res = engine_t1.query("SELECT name FROM orderpositions")
    assert res.columns == [
        {"name": "name", "type": "TEXT", "nullable": True, "hint": None},
    ]


@pytest.mark.django_db
@pytest.mark.xfail(reason="Known bug, we cannot see the nullability of foreign keys")
def test_columns_nullable_foreignkey(engine_t1):
    # Customer is nullable on Order, but name not on customer
    res = engine_t1.query("SELECT customer FROM orders")
    assert res.columns == [
        {"name": "customer", "type": "TEXT", "nullable": True, "hint": None},
    ]
    res = engine_t1.query("SELECT customer.name FROM orders")
    assert res.columns == [
        {"name": "customer.name", "type": "TEXT", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_boolean_field(engine_t1):
    res = engine_t1.query("SELECT enabled FROM customers")
    assert res.columns == [
        {"name": "enabled", "type": "BOOLEAN", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_json_field(engine_t1):
    res = engine_t1.query("SELECT address FROM customers")
    assert res.columns == [
        {"name": "address", "type": "JSONB", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_json_field_extraction(engine_t1):
    res = engine_t1.query("SELECT address->city->state->code as code FROM customers")
    assert res.columns == [
        {"name": "code", "type": "JSONB", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_count_aggregate(engine_t1):
    res = engine_t1.query("SELECT COUNT(*) AS n FROM categories")
    assert res.columns == [
        {"name": "n", "type": "INT", "nullable": False, "hint": None}
    ]


@pytest.mark.django_db
def test_columns_sum_aggregate(engine_t1):
    res = engine_t1.query("SELECT SUM(price) AS total FROM products")
    assert len(res.columns) == 1
    assert res.columns[0]["name"] == "total"
    assert res.columns[0]["type"] == "DECIMAL"


@pytest.mark.django_db
def test_columns_group_by_with_aggregate(engine_t1):
    res = engine_t1.query(
        "SELECT category.title, COUNT(*) AS n FROM products GROUP BY category.title"
    )
    assert res.columns == [
        {"name": "category.title", "type": "TEXT", "nullable": False, "hint": None},
        {"name": "n", "type": "INT", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_preserved_with_alias(engine_t1):
    res = engine_t1.query("SELECT title AS name, price AS amount FROM products")
    assert res.columns == [
        {"name": "name", "type": "TEXT", "nullable": False, "hint": None},
        {"name": "amount", "type": "DECIMAL", "nullable": False, "hint": None},
    ]


@pytest.mark.django_db
def test_columns_datetime_trunc(engine_t1):
    for k in ("year", "quarter", "month", "week", "day", "hour", "minute", "second"):
        res = engine_t1.query(f"SELECT DATETRUNC('{k}', created) as col FROM orders")
        assert res.columns == [
            {
                "name": "col",
                "type": "DATETIME",
                "nullable": False,
                "hint": {"truncated": k},
            },
        ]


@pytest.mark.django_db
def test_columns_date_trunc(engine_t1):
    for k in ("year", "quarter", "month", "week"):
        res = engine_t1.query(
            f"SELECT DATETRUNC('{k}', publication_date) as col FROM products"
        )
        assert res.columns == [
            {
                "name": "col",
                "type": "DATE",
                "nullable": False,
                "hint": {"truncated": k},
            },
        ]
