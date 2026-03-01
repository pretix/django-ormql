from decimal import Decimal

import pytest
from django.conf import settings

from django_ormql.exceptions import QueryError


@pytest.mark.django_db
def test_where_eq(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate = 19
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate == 19
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_where_neq(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate != 7
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate <> 7
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_where_gt(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate > 18
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate > 19
        """
    )
    assert list(res) == []


@pytest.mark.django_db
def test_where_gte(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate >= 19
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate >= 20
        """
    )
    assert list(res) == []


@pytest.mark.django_db
def test_where_lt(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate < 8
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "SQL for Dummies"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate < 7
        """
    )
    assert list(res) == []


@pytest.mark.django_db
def test_where_lte(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate <= 7
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "SQL for Dummies"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate <= 6.5
        """
    )
    assert list(res) == []


@pytest.mark.django_db
def test_where_between(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate BETWEEN 7 AND 8
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "SQL for Dummies"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate BETWEEN 9 AND 10
        """
    )
    assert list(res) == []


@pytest.mark.django_db
def test_where_not_between(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE NOT tax_rate BETWEEN 9 AND 20
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "SQL for Dummies"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate NOT BETWEEN 9 AND 20
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "SQL for Dummies"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE NOT tax_rate BETWEEN 7 AND 20
        """
    )
    assert list(res) == []


@pytest.mark.django_db
def test_where_and(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate <= 7
          AND price < 20
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
    ]


@pytest.mark.django_db
def test_where_or(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate >= 19
           OR price < 20
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_isnull(engine_t1):
    res = engine_t1.query(
        """
        SELECT status
        FROM orders
        WHERE customer IS NULL
        """
    )
    assert list(res) == [
        {"status": "canceled"},
    ]
    res = engine_t1.query(
        """
        SELECT status
        FROM orders
        WHERE customer ISNULL
        """
    )
    assert list(res) == [
        {"status": "canceled"},
    ]


@pytest.mark.django_db
def test_isnotnull(engine_t1):
    res = engine_t1.query(
        """
        SELECT status
        FROM orders
        WHERE customer IS NOT NULL
        """
    )
    assert list(res) == [
        {"status": "paid"},
        {"status": "canceled"},
    ]
    res = engine_t1.query(
        """
        SELECT status
        FROM orders
        WHERE customer NOT NULL
        """
    )
    assert list(res) == [
        {"status": "paid"},
        {"status": "canceled"},
    ]
    res = engine_t1.query(
        """
        SELECT status
        FROM orders
        WHERE customer NOTNULL
        """
    )
    assert list(res) == [
        {"status": "paid"},
        {"status": "canceled"},
    ]


@pytest.mark.django_db
def test_where_like(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE title LIKE 'Lord%'
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE title LIKE '%DVD'
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_where_like_case_sensitive(engine_t1):
    if 'sqlite' in settings.DATABASES['default']['ENGINE']:
        pytest.skip('Not supported on SQLite')
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE title LIKE '%dvd'
        """
    )
    assert list(res) == []


@pytest.mark.django_db
def test_where_ilike(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE title ILIKE 'LORD%'
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE title ILIKE '%dvd'
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_where_in(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE tax_rate IN (19, 20)
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_where_in_complex_lhs(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE (tax_rate + price) IN (17.70, 18)
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
    ]
