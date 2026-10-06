from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryError


@pytest.mark.django_db
def test_unrelated_subquery(engine_t1):
    res = engine_t1.query(
        """
        SELECT (SELECT name FROM customers ORDER BY name LIMIT 1) AS result
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
        """
        SELECT (SELECT name FROM customers WHERE id = OUTER(order.customer)) AS result,
        TYPE_INFO(SELECT name FROM customers WHERE id = OUTER(order.customer)) AS type_info
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"result": "CA", "type_info": "TEXT"},
    ]


@pytest.mark.django_db
def test_nested_subquery(engine_t1):
    res = engine_t1.query(
        """
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
        """
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
def test_outer_ref_type_resolution(engine_t1):
    res = engine_t1.query(
        """
        SELECT (
            SELECT (
                SELECT TYPE_INFO(OUTER(OUTER(order.customer.name))) FROM customers WHERE id = OUTER(OUTER(order.customer))
            ) FROM orders WHERE id = OUTER(order)
        ) AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"result": "TEXT"},
    ]


@pytest.mark.django_db
def test_aggregate_in_subquery_with_outerref(engine_t1):
    res = engine_t1.query(
        """
        SELECT title, (
            SELECT COUNT(*) FROM orderpositions WHERE product = OUTER(id)
        ) AS result, TYPE_INFO(
            SELECT COUNT(*) FROM orderpositions WHERE product = OUTER(id)
        ) AS type_info
        FROM products
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings", "result": 2, "type_info": "INT"},
        {"title": "SQL for Dummies", "result": 1, "type_info": "INT"},
        {"title": "Lord of the rings DVD", "result": 2, "type_info": "INT"},
    ]


@pytest.mark.django_db
def test_aggregate_in_subquery_without_outerref(engine_t1):
    res = engine_t1.query(
        """
        SELECT title, (
            SELECT COUNT(*) FROM orderpositions
        ) AS result
        FROM products
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings", "result": 5},
        {"title": "SQL for Dummies", "result": 5},
        {"title": "Lord of the rings DVD", "result": 5},
    ]


@pytest.mark.django_db
def test_compare_to_subquery(engine_t1):
    res = engine_t1.query(
        """
        SELECT title, price
        FROM products
        WHERE price > (SELECT AVG(price) FROM products)
        """
    )
    assert list(res) == [
        {"title": "SQL for Dummies", "price": Decimal("21.40")},
        {"title": "Lord of the rings DVD", "price": Decimal("19.00")},
    ]


@pytest.mark.django_db
def test_exists_subquery(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid")
        """
    )
    assert list(res) == [
        {"title": "SQL for Dummies"},
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE NOT EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid")
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
    ]
    res = engine_t1.query(
        """
        SELECT
        EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid") exists,
        TYPE_INFO(EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid")) type_info
        FROM products
        LIMIT 1
        """
    )
    assert list(res) == [
        {"exists": False, "type_info": "BOOLEAN"},
    ]


@pytest.mark.django_db
def test_in_subquery(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE id IN (SELECT product FROM orderpositions WHERE order.status = "paid")
        """
    )
    assert list(res) == [
        {"title": "SQL for Dummies"},
        {"title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        WHERE id NOT IN (SELECT product FROM orderpositions WHERE order.status = "paid")
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
    ]


@pytest.mark.django_db
def test_invalid_outerref(engine_t1):
    with pytest.raises(
        QueryError, match="Column 'foobar' does not exist in table 'orderpositions'"
    ):
        list(
            engine_t1.query(
                """
            SELECT (SELECT name FROM customers WHERE id = OUTER(foobar)) AS result
            FROM orderpositions
            WHERE quantity = 3
            """
            )
        )
    with pytest.raises(QueryError, match="OUTER nested too far"):
        list(
            engine_t1.query(
                """
            SELECT (SELECT name FROM customers WHERE id = OUTER(OUTER(id))) AS result
            FROM orderpositions
            WHERE quantity = 3
            """
            )
        )
    with pytest.raises(QueryError, match="Invalid argument to OUTER"):
        list(
            engine_t1.query(
                """
            SELECT (SELECT name FROM customers WHERE id = OUTER(CASE WHEN 1 = 2 THEN 3 END)) AS result
            FROM orderpositions
            WHERE quantity = 3
            """
            )
        )


@pytest.mark.django_db
def test_union_subquery_not_allowed(engine_t1):
    with pytest.raises(QueryError, match="Only SELECT subqueries are supported"):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM products
            WHERE EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid" UNION SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "canceled")
            """
            )
        )
