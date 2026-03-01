from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryNotSupported, QueryError


@pytest.mark.django_db
def test_single_column(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM categories
        """
    )
    assert list(res) == [
        {"title": "Books"},
        {"title": "DVDs"},
    ]


@pytest.mark.django_db
def test_multiple_columns(engine_t1):
    res = engine_t1.query(
        """
        SELECT title, price
        FROM products
        """
    )
    assert list(res) == [
        {'title': 'Lord of the rings', 'price': Decimal('10.70')},
        {'title': 'SQL for Dummies', 'price': Decimal('21.40')},
        {'title': 'Lord of the rings DVD', 'price': Decimal('19.00')}
    ]


@pytest.mark.django_db
def test_column_alias(engine_t1):
    res = engine_t1.query(
        """
        SELECT title AS 'Product Name', price
        FROM products
        """
    )
    assert list(res) == [
        {'Product Name': 'Lord of the rings', 'price': Decimal('10.70')},
        {'Product Name': 'SQL for Dummies', 'price': Decimal('21.40')},
        {'Product Name': 'Lord of the rings DVD', 'price': Decimal('19.00')}
    ]


@pytest.mark.django_db
def test_auto_join_one_level(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title
        FROM products
        """
    )
    assert list(res) == [{'category.title': 'Books'}, {'category.title': 'Books'}, {'category.title': 'DVDs'}]


@pytest.mark.django_db
def test_auto_join_two_levels(engine_t1):
    res = engine_t1.query(
        """
        SELECT order.customer.name AS cust
        FROM orderpositions
        """
    )
    assert list(res) == [{'cust': 'CA'}, {'cust': 'CA'}, {'cust': 'CB'}, {'cust': 'CB'}, {'cust': None}]


@pytest.mark.django_db
def test_select_literal_expression(engine_t1):
    res = engine_t1.query(
        """
        SELECT 3 * 5
        FROM orderpositions
        """
    )
    assert list(res) == [{'3 * 5': 15}, {'3 * 5': 15}, {'3 * 5': 15}, {'3 * 5': 15}, {'3 * 5': 15}]


@pytest.mark.django_db
def test_select_literal_none(engine_t1):
    res = engine_t1.query(
        """
        SELECT NULL
        FROM orderpositions LIMIT 1
        """
    )
    assert list(res) == [{'NULL': None}]


@pytest.mark.django_db
def test_star_not_allowed(engine_t1):
    with pytest.raises(QueryNotSupported, match=r"SELECT \* is not supported"):
        list(engine_t1.query(
            """
            SELECT *
            FROM categories
            """
        ))


@pytest.mark.django_db
def test_distinct(engine_t1):
    res = engine_t1.query(
        """
        SELECT DISTINCT category.title
        FROM products
        """
    )
    assert list(res) == [{'category.title': 'Books'}, {'category.title': 'DVDs'}]


@pytest.mark.django_db
def test_case_sensitive(engine_t1):
    with pytest.raises(QueryError, match="Column 'caTegory' does not exist in table 'products'"):
        list(engine_t1.query(
            """
            SELECT DISTINCT caTegory.TiTle
            FROM products
            """
        ))


@pytest.mark.django_db
def test_invalid_table(engine_t1):
    with pytest.raises(QueryError, match="Table foo not found"):
        list(engine_t1.query(
            """
            SELECT a
            FROM foo
            """
        ))


@pytest.mark.django_db
def test_multiple_tables_not_allowed(engine_t1):
    with pytest.raises(QueryNotSupported, match="SELECT from multiple tables not supported"):
        list(engine_t1.query(
            """
            SELECT title
            FROM categories, products
            """
        ))


@pytest.mark.django_db
def test_join_not_allowed(engine_t1):
    with pytest.raises(QueryNotSupported, match="Unexpected token"):
        list(engine_t1.query(
            """
            SELECT title
            FROM products
            LEFT JOIN categories ON categories.id = products.category
            """
        ))


@pytest.mark.django_db
def test_unknown_field(engine_t1):
    with pytest.raises(QueryError, match="Column 'name' does not exist in table 'categories'"):
        list(engine_t1.query(
            """
            SELECT name
            FROM categories
            """
        ))


@pytest.mark.django_db
def test_undeclared_field(engine_t1):
    with pytest.raises(QueryError, match="Column 'name' does not exist in table 'categories'"):
        list(engine_t1.query(
            """
            SELECT name
            FROM categories
            """
        ))


@pytest.mark.django_db
def test_compound_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT title
            FROM categories
            UNION
            SELECT title
            FROM products
            """
        ))
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT title
            FROM categories
            INTERSECT
            SELECT title
            FROM products
            """
        ))
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT title
            FROM categories
            EXCEPT
            SELECT title
            FROM products
            """
        ))


@pytest.mark.django_db
def test_values_query_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Only SELECT queries are supported"):
        list(engine_t1.query(
            """
            VALUES (3);
            """
        ))


@pytest.mark.django_db
def test_table_alias_not_allowed(engine_t1):
    with pytest.raises(QueryNotSupported, match="Table alias not supported"):
        list(engine_t1.query(
            """
            SELECT title
            FROM categories as cat
            """
        ))


@pytest.mark.django_db
def test_schema_name_not_allowed(engine_t1):
    with pytest.raises(QueryNotSupported, match="Database names not supported"):
        list(engine_t1.query(
            """
            SELECT title
            FROM foo.categories
            """
        ))
