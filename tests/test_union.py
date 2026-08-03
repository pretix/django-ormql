from decimal import Decimal

import pytest

from django_ormql.exceptions import QueryError


@pytest.mark.django_db
def test_union_modifiers_not_allowed(engine_t1):
    with pytest.raises(
        QueryError, match="Only SELECT and SELECT ... UNION ALL queries are supported"
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM categories
            UNION
            SELECT title
            FROM products
            """
            )
        )
    with pytest.raises(
        QueryError,
        match="ORDER, LIMIT and OFFSET modifiers are not supported on UNION queries",
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM categories
            UNION ALL
            SELECT title
            FROM products
            LIMIT 1
            """
            )
        )
    with pytest.raises(
        QueryError,
        match="ORDER, LIMIT and OFFSET modifiers are not supported on UNION queries",
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM categories
            UNION ALL
            SELECT title
            FROM products
            ORDER BY title
            """
            )
        )


@pytest.mark.django_db
def test_union(engine_t1):
    res = engine_t1.query(
        """
        SELECT 'Category' type, title
        FROM categories
        UNION ALL
        SELECT 'Product' type, title
        FROM products
        """
    )
    assert list(res) == [
        {"type": "Category", "title": "Books"},
        {"type": "Category", "title": "DVDs"},
        {"type": "Product", "title": "Lord of the rings"},
        {"type": "Product", "title": "SQL for Dummies"},
        {"type": "Product", "title": "Lord of the rings DVD"},
    ]
    res = engine_t1.query(
        """
        (SELECT 'Category' type, title
        FROM categories ORDER BY title DESC)
        UNION ALL
        (SELECT 'Product' type, title
        FROM products ORDER BY title DESC)
        """
    )
    assert list(res) == [
        {"type": "Category", "title": "DVDs"},
        {"type": "Category", "title": "Books"},
        {"type": "Product", "title": "SQL for Dummies"},
        {"type": "Product", "title": "Lord of the rings DVD"},
        {"type": "Product", "title": "Lord of the rings"},
    ]


@pytest.mark.django_db
def test_union_col_count(engine_t1):
    with pytest.raises(
        QueryError, match="All parts of UNION query must return same number of columns"
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM categories
            UNION ALL
            SELECT category.title, SUM(price)
            FROM products
            GROUP BY category.title
            """
            )
        )
    with pytest.raises(
        QueryError, match="All parts of UNION query must return same number of columns"
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM categories
            UNION ALL
            SELECT title, price
            FROM products
            """
            )
        )


@pytest.mark.django_db
def test_union_grouped(engine_t1):
    res = engine_t1.query(
        """
        SELECT 'Category' type, title, 0 sales, 0 total
        FROM categories
        UNION ALL
        SELECT 'Product' type, product.title, SUM(quantity) sales, SUM(single_price * quantity) total
        FROM orderpositions
        GROUP BY product.id
        """
    )
    assert list(res) == [
        {"type": "Category", "title": "Books", "sales": 0, "total": 0},
        {"type": "Category", "title": "DVDs", "sales": 0, "total": 0},
        {
            "type": "Product",
            "title": "Lord of the rings",
            "sales": 3,
            "total": Decimal("32.10"),
        },
        {
            "type": "Product",
            "title": "SQL for Dummies",
            "sales": 1,
            "total": Decimal("21.40"),
        },
        {
            "type": "Product",
            "title": "Lord of the rings DVD",
            "sales": 4,
            "total": Decimal("76.00"),
        },
    ]
    res = engine_t1.query(
        """
        SELECT 'Category' type, product.category.title title, SUM(quantity) sales, SUM(single_price * quantity) total
        FROM orderpositions
        GROUP BY product.category.id
        UNION ALL
        SELECT 'Product' type, product.title, SUM(quantity) sales, SUM(single_price * quantity) total
        FROM orderpositions
        GROUP BY product.id
        """
    )
    assert list(res) == [
        {"type": "Category", "title": "Books", "sales": 4, "total": Decimal("53.50")},
        {"type": "Category", "title": "DVDs", "sales": 4, "total": Decimal("76.00")},
        {
            "type": "Product",
            "title": "Lord of the rings",
            "sales": 3,
            "total": Decimal("32.10"),
        },
        {
            "type": "Product",
            "title": "SQL for Dummies",
            "sales": 1,
            "total": Decimal("21.40"),
        },
        {
            "type": "Product",
            "title": "Lord of the rings DVD",
            "sales": 4,
            "total": Decimal("76.00"),
        },
    ]
