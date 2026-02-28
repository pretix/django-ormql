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
            SELECT id FROM products FOR UPDATE
            """
        ))


@pytest.mark.django_db
def test_window_declaration_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            SELECT id FROM products
            WINDOW w AS (:w
            PARTITION BY id ORDER BY id)
            """
        ))


@pytest.mark.django_db
def test_cte_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Invalid expression / Unexpected token"):
        list(engine_t1.query(
            """
            WITH cte AS (SELECT title FROM products)
            SELECT title from cte
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
