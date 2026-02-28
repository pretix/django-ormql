from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryError


@pytest.mark.django_db
def test_group_by_simple(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        GROUP BY title
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
        {"title": "SQL for Dummies"},
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
@pytest.mark.xfail(reason="TODO Not probably implemented, possibly impossible in Django")
def test_group_by_foreignkey(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title
        FROM products
        GROUP BY category.title
        """
    )
    assert list(res) == [
        {"category.title": "Books"},
        {"category.title": "DVDs"},
    ]


@pytest.mark.django_db
def test_group_by_with_aggregate(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id)
        FROM products
        GROUP BY category.title
        """
    )
    assert list(res) == [
        {"category.title": "Books", "COUNT(id)": 2},
        {"category.title": "DVDs", "COUNT(id)": 1},
    ]


@pytest.mark.django_db
def test_group_by_with_aggregate_star(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(*)
        FROM products
        GROUP BY category.title
        """
    )
    assert list(res) == [
        {"category.title": "Books", "COUNT(*)": 2},
        {"category.title": "DVDs", "COUNT(*)": 1},
    ]


@pytest.mark.django_db
def test_group_by_with_aggregate_and_literal(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id), 2 as foo
        FROM products
        GROUP BY category.title
        """
    )
    assert list(res) == [
        {"category.title": "Books", "COUNT(id)": 2, "foo": 2},
        {"category.title": "DVDs", "COUNT(id)": 1, "foo": 2},
    ]


@pytest.mark.django_db
def test_group_by_aggregate_multiple_args(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id), tax_rate
        FROM products
        GROUP BY category.title, tax_rate
        """
    )
    assert list(res) == [
        {"category.title": "Books", "COUNT(id)": 2, "tax_rate": Decimal("7.00")},
        {"category.title": "DVDs", "COUNT(id)": 1, "tax_rate": Decimal("19.00")},
    ]


@pytest.mark.django_db
@pytest.mark.xfail(reason="TODO Check not yet implemented")
def test_group_by_invalid_select(engine_t1):
    with pytest.raises(QueryError):
        list(engine_t1.query(
            """
            SELECT title, count(id)
            FROM products
            GROUP BY category.title
            """
        ))


@pytest.mark.django_db
def test_group_by_with_having_aggregate_with_alias(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id) as cnt
        FROM products
        GROUP BY category.title
        HAVING cnt == 2
        """
    )
    assert list(res) == [
        {"category.title": "Books", "cnt": 2},
    ]


@pytest.mark.django_db
def test_group_by_with_having_aggregate_with_repetition(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id) as cnt
        FROM products
        GROUP BY category.title
        HAVING count(id) == 2
        """
    )
    assert list(res) == [
        {"category.title": "Books", "cnt": 2},
    ]


@pytest.mark.django_db
def test_group_by_with_having_boolean_logic(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id) as cnt
        FROM products
        GROUP BY category.title
        HAVING cnt == 2 OR cnt == 0
        """
    )
    assert list(res) == [
        {"category.title": "Books", "cnt": 2},
    ]


@pytest.mark.django_db
def test_group_by_with_order_by(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id) as cnt
        FROM products
        GROUP BY category.title
        ORDER BY cnt ASC
        """
    )
    assert list(res) == [
        {"category.title": "DVDs", "cnt": 1},
        {"category.title": "Books", "cnt": 2},
    ]


@pytest.mark.django_db
def test_group_by_with_having_and_order_by_and_limit(engine_t1):
    res = engine_t1.query(
        """
        SELECT category.title, count(id) as cnt
        FROM products
        GROUP BY category.title
        HAVING cnt > 0
        ORDER BY cnt ASC LIMIT 1
        """
    )
    assert list(res) == [
        {"category.title": "DVDs", "cnt": 1},
    ]
