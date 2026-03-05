from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryError, QueryNotSupported


@pytest.mark.django_db
def test_aggregate(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(id)
        FROM products
        """
    )
    assert list(res) == [{"COUNT(id)": 3}]


@pytest.mark.django_db
def test_aggregate_star(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(*)
        FROM products
        """
    )
    assert list(res) == [{"COUNT(*)": 3}]


@pytest.mark.django_db
def test_aggregate_multiple(engine_t1):
    res = list(
        engine_t1.query(
            """
        SELECT COUNT(id), SUM(price) AS sum
        FROM products
        """
        )
    )
    assert len(res) == 1
    assert res[0]["COUNT(id)"] == 3
    assert res[0]["sum"] == Decimal("51.10")


@pytest.mark.django_db
def test_aggregate_where(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(id)
        FROM products
        WHERE tax_rate < 10
        """
    )
    assert list(res) == [{"COUNT(id)": 2}]


@pytest.mark.django_db
def test_aggregate_distinct(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(DISTINCT tax_rate)
        FROM products
        """
    )
    assert list(res) == [{"COUNT(DISTINCT tax_rate)": 2}]


@pytest.mark.django_db
def test_aggregate_distinct_multiple_not_supported(engine_t1):
    with pytest.raises(
        QueryNotSupported,
        match="Multiple arguments to aggregate expression not supported",
    ):
        list(
            engine_t1.query(
                """
            SELECT COUNT(DISTINCT title, tax_rate)
            FROM products
            """
            )
        )


@pytest.mark.django_db
def test_aggregate_order_by_unsupported(engine_t1):
    with pytest.raises(QueryError, match="ORDER not supported in expression"):
        list(
            engine_t1.query(
                """
            SELECT COUNT(DISTINCT title ORDER BY tax_rate)
            FROM products
            """
            )
        )


@pytest.mark.django_db
def test_aggregate_functions(engine_t1):
    res = engine_t1.query(
        """
        SELECT COUNT(*), AVG(price), MAX(price), min(price), stddev(price), VARIANCE(price), SUM(price)
        FROM products
        """
    )
    assert [{k: round(v, 2) for k, v in row.items()} for row in res] == [
        {
            "COUNT(*)": 3,
            "AVG(price)": Decimal("17.03"),
            "MAX(price)": Decimal("21.40"),
            "MIN(price)": Decimal("10.70"),
            "STDDEV(price)": Decimal("4.58"),
            "VARIANCE(price)": Decimal("21.02"),
            "SUM(price)": Decimal("51.10"),
        }
    ]


@pytest.mark.django_db
def test_aggregate_filter(engine_t1):
    res = engine_t1.query(
        """
        SELECT
            COUNT(DISTINCT id) FILTER (WHERE order.status = "paid") AS paid,
            COUNT(id) FILTER (WHERE order.status = "canceled") AS canceled,
            COUNT(id) AS all
        FROM orderpositions
        """
    )
    assert list(res) == [{"paid": 2, "canceled": 3, "all": 5}]
