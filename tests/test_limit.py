import pytest

from django_ormql.exceptions import QueryNotSupported


@pytest.mark.django_db
def test_limit(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products
        LIMIT 1
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings"},
    ]


@pytest.mark.django_db
def test_offset(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products OFFSET 1
        """
    )
    assert list(res) == [
        {"title": "SQL for Dummies"},
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_offset_limit(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products LIMIT 1 OFFSET 1
        """
    )
    assert list(res) == [
        {"title": "SQL for Dummies"},
    ]


@pytest.mark.django_db
def test_offset_limit_weird_form(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        FROM products LIMIT 2, 1
        """
    )
    assert list(res) == [
        {"title": "Lord of the rings DVD"},
    ]


@pytest.mark.django_db
def test_limit_expr_not_allowed(engine_t1):
    with pytest.raises(
        QueryNotSupported, match="LIMIT may only contain literal numbers"
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM products LIMIT 4 + 5
            """
            )
        )
    with pytest.raises(
        QueryNotSupported, match="OFFSET may only contain literal numbers"
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM products LIMIT 4 OFFSET 3 + 5
            """
            )
        )
    with pytest.raises(
        QueryNotSupported, match="OFFSET may only contain literal numbers"
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM products OFFSET 4 + 5
            """
            )
        )
    with pytest.raises(
        QueryNotSupported, match="LIMIT may only contain literal numbers"
    ):
        list(
            engine_t1.query(
                """
            SELECT title
            FROM products LIMIT 4 + 5 OFFSET 1
            """
            )
        )
