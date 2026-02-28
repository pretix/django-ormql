from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryNotSupported


@pytest.mark.django_db
def test_single_column(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM categories
        ORDER BY title DESC
        """
    )
    assert list(res) == [
        {"title": "DVDs"},
        {"title": "Books"},
    ]


@pytest.mark.django_db
def test_nulls_default(engine_t1):
    res = engine_t1.query(
        """
        SELECT customer.name
        FROM orders
        ORDER BY customer.name ASC
        """
    )
    assert list(res) == [{'customer.name': None}, {'customer.name': 'CA'}, {'customer.name': 'CB'}]


@pytest.mark.django_db
def test_nulls_first(engine_t1):
    res = engine_t1.query(
        """
        SELECT customer.name
        FROM orders
        ORDER BY customer.name ASC NULLS FIRST
        """
    )
    assert list(res) == [{'customer.name': None}, {'customer.name': 'CA'}, {'customer.name': 'CB'}]


@pytest.mark.django_db
def test_nulls_last(engine_t1):
    res = engine_t1.query(
        """
        SELECT customer.name
        FROM orders
        ORDER BY customer.name ASC NULLS LAST
        """
    )
    assert list(res) == [{'customer.name': 'CA'}, {'customer.name': 'CB'}, {'customer.name': None}]


@pytest.mark.django_db
def test_joined_field(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        ORDER BY category.title DESC
        """
    )
    assert list(res) == [{'title': 'Lord of the rings DVD'}, {'title': 'Lord of the rings'},
                         {'title': 'SQL for Dummies'}]


@pytest.mark.django_db
def test_multiple_columns(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        ORDER BY category.title DESC, title DESC
        """
    )
    assert list(res) == [
        {'title': 'Lord of the rings DVD'},
        {'title': 'SQL for Dummies'},
        {'title': 'Lord of the rings'},
    ]


@pytest.mark.django_db
def test_order_by_expr(engine_t1):
    res = engine_t1.query(
        """
        SELECT quantity, single_price
        FROM orderpositions
        ORDER BY quantity * single_price DESC, single_price DESC 
        """
    )
    assert list(res) == [
        {'quantity': 3, 'single_price': Decimal('19.00')},
        {'quantity': 1, 'single_price': Decimal('21.40')},
        {'quantity': 2, 'single_price': Decimal('10.70')},
        {'quantity': 1, 'single_price': Decimal('19.00')},
        {'quantity': 1, 'single_price': Decimal('10.70')},
    ]
