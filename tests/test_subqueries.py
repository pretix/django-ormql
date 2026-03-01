import datetime
from decimal import Decimal

import pytest
import pytz
from django.conf import settings
from freezegun import freeze_time

from django_ormql.exceptions import QueryNotSupported, QueryError


@pytest.mark.django_db
def test_unrelated_subquery(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT (SELECT name FROM customers LIMIT 1) AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"result": "CA"},
    ]


@pytest.mark.django_db
def test_related_subquery(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT (SELECT name FROM customers WHERE id = OUTER(order.customer)) AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"result": "CA"},
    ]


@pytest.mark.django_db
def test_nested_subquery(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT (
            SELECT (
                SELECT name FROM customers WHERE id = OUTER(customer)
            ) FROM orders WHERE id = OUTER(order)
        ) AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"result": "CA"},
    ]


@pytest.mark.django_db
def test_nested_outer_ref_in_subquery(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT (
            SELECT (
                SELECT name FROM customers WHERE id = OUTER(OUTER(order.customer))
            ) FROM orders WHERE id = OUTER(order)
        ) AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"result": "CA"},
    ]


@pytest.mark.django_db
def test_aggregate_in_subquery_with_outerref(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT title, (
            SELECT COUNT(*) FROM orderpositions WHERE product = OUTER(id)
        ) AS result
        FROM products
        """
    )
    assert list(res) == [
        {'title': 'Lord of the rings', 'result': 2},
        {'title': 'SQL for Dummies', 'result': 1},
        {'title': 'Lord of the rings DVD', 'result': 2}
    ]


@pytest.mark.django_db
def test_aggregate_in_subquery_without_outerref(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT title, (
            SELECT COUNT(*) FROM orderpositions
        ) AS result
        FROM products
        """
    )
    assert list(res) == [
        {'title': 'Lord of the rings', 'result': 5},
        {'title': 'SQL for Dummies', 'result': 5},
        {'title': 'Lord of the rings DVD', 'result': 5},
    ]


@pytest.mark.django_db
def test_compare_to_subquery(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT title, price
        FROM products
        WHERE price > (SELECT AVG(price) FROM products)
        """
    )
    assert list(res) == [
        {'title': 'SQL for Dummies', 'price': Decimal('21.40')},
        {'title': 'Lord of the rings DVD', 'price': Decimal('19.00')}
    ]


@pytest.mark.django_db
def test_exists_subquery(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT title
        FROM products
        WHERE EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid")
        """
    )
    assert list(res) == [
        {'title': 'SQL for Dummies'},
        {'title': 'Lord of the rings DVD'}
    ]
    res = engine_t1.query(
        f"""
        SELECT title
        FROM products
        WHERE NOT EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid")
        """
    )
    assert list(res) == [
        {'title': 'Lord of the rings'},
    ]


@pytest.mark.django_db
def test_in_subquery(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT title
        FROM products
        WHERE id IN (SELECT product FROM orderpositions WHERE order.status = "paid")
        """
    )
    assert list(res) == [
        {'title': 'SQL for Dummies'},
        {'title': 'Lord of the rings DVD'}
    ]
    res = engine_t1.query(
        f"""
        SELECT title
        FROM products
        WHERE id NOT IN (SELECT product FROM orderpositions WHERE order.status = "paid")
        """
    )
    assert list(res) == [
        {'title': 'Lord of the rings'},
    ]


@pytest.mark.django_db
def test_invalid_outerref(engine_t1):
    with pytest.raises(QueryError, match="Column 'foobar' does not exist in table 'orderpositions'"):
        list(engine_t1.query(
            f"""
            SELECT (SELECT name FROM customers WHERE id = OUTER(foobar)) AS result
            FROM orderpositions
            WHERE quantity = 3
            """
        ))
    with pytest.raises(QueryError, match="OUTER nested too far"):
        list(engine_t1.query(
            f"""
            SELECT (SELECT name FROM customers WHERE id = OUTER(OUTER(id))) AS result
            FROM orderpositions
            WHERE quantity = 3
            """
        ))
