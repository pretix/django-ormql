from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryError


@pytest.mark.django_db
def test_aggregate(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(id)
        FROM products
        """
    )
    assert list(res) == [
        {"COUNT(id)": 3}
    ]


@pytest.mark.django_db
def test_aggregate_star(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(*)
        FROM products
        """
    )
    assert list(res) == [
        {"COUNT(*)": 3}
    ]


@pytest.mark.django_db
def test_aggregate_multiple(engine_t1):
    res = list(engine_t1.query(
        """
        SELECT COUNT(id), SUM(price) AS sum
        FROM products
        """
    ))
    assert len(res) == 1
    assert res[0]["COUNT(id)"] == 3
    assert round(res[0]["sum"], 2) == 51.10


@pytest.mark.django_db
def test_aggregate_where(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(id)
        FROM products
        WHERE tax_rate < 10
        """
    )
    assert list(res) == [
        {"COUNT(id)": 2}
    ]


@pytest.mark.django_db
def test_aggregate_distinct(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(DISTINCT tax_rate)
        FROM products
        """
    )
    assert list(res) == [
        {"COUNT(DISTINCT tax_rate)": 2}
    ]


@pytest.mark.django_db
def test_aggregate_distinct_multiple(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(DISTINCT title, tax_rate)
        FROM products
        """
    )
    assert list(res) == [
        {"COUNT(DISTINCT title, tax_rate)": 3}
    ]


@pytest.mark.django_db
def test_aggregate_order_by_unsupported(engine_t1):
    with pytest.raises(QueryError, match="ORDER not supported in expression"):
        list(engine_t1.query(
            """
            SELECT COUNT(DISTINCT title ORDER BY tax_rate)
            FROM products
            """
        ))
