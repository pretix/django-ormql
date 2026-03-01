from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryNotSupported, QueryError


@pytest.mark.django_db
def test_update_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            UPDATE products
            SET title = "foo"
            """
        ))


@pytest.mark.django_db
def test_delete_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            DELETE products
            SET title = "foo"
            """
        ))


@pytest.mark.django_db
def test_for_update_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT id
            FROM products FOR UPDATE
            """
        ))


@pytest.mark.django_db
def test_window_declaration_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT id
            FROM products WINDOW w AS (:w
            PARTITION BY id ORDER BY id)
            """
        ))


@pytest.mark.django_db
def test_cte_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            WITH cte AS (SELECT title FROM products)
            SELECT title
            from cte
            """
        ))


@pytest.mark.django_db
def test_update_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            UPDATE products
            SET title = "foo"
            """
        ))


@pytest.mark.django_db
def test_is_distinct_from_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="IS \\(NOT\\) DISTINCT not supported"):
        list(engine_t1.query(
            """
            SELECT price
            FROM products
            WHERE tax_rate IS DISTINCT
            FROM price
            """
        ))
    with pytest.raises(QueryError, match="IS \\(NOT\\) DISTINCT not supported"):
        list(engine_t1.query(
            """
            SELECT price
            FROM products
            WHERE tax_rate IS NOT DISTINCT
            FROM price
            """
        ))


@pytest.mark.django_db
def test_where_regexp_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT price
            FROM products
            WHERE title REGEXP 'Foo'
            """
        ))


@pytest.mark.django_db
def test_where_glob_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT price
            FROM products
            WHERE title GLOB 'Foo*'
            """
        ))


@pytest.mark.django_db
def test_where_match_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT price
            FROM products
            WHERE title MATCH 'Foo'
            """
        ))


@pytest.mark.django_db
def test_where_match_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT price COLLATE "foo"
            FROM products
            """
        ))


@pytest.mark.django_db
def test_binary_operator_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Bitwise operations not supported"):
        list(engine_t1.query(
            """
            SELECT id & 1
            FROM products
            """
        ))
    with pytest.raises(QueryError, match="Bitwise operations not supported"):
        list(engine_t1.query(
            """
            SELECT id | 1
            FROM products
            """
        ))
    with pytest.raises(QueryError, match="Bitwise operations not supported"):
        list(engine_t1.query(
            """
            SELECT id << 1
            FROM products
            """
        ))
    with pytest.raises(QueryError, match="Bitwise operations not supported"):
        list(engine_t1.query(
            """
            SELECT id >> 1
            FROM products
            """
        ))
    with pytest.raises(QueryError, match="Bitwise operations not supported"):
        list(engine_t1.query(
            """
            SELECT id ^ 1
            FROM products
            """
        ))
    with pytest.raises(QueryError, match="Bitwise operations not supported"):
        list(engine_t1.query(
            """
            SELECT ~id
            FROM products
            """
        ))


@pytest.mark.django_db
def test_unsupported_function(engine_t1):
    with pytest.raises(QueryError, match="Unsupported expression: SIN\\(3\\)"):
        list(engine_t1.query(
            """
            SELECT Sin(3)
            FROM products
            """
        ))
