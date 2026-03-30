import pytest

from django_ormql.exceptions import QueryError


def raises_query_error(engine_t1, error_match, query):
    with pytest.raises(QueryError, match=error_match) as exc_info:
        list(engine_t1.query(query))
    assert "\033" not in str(exc_info.value)


@pytest.mark.django_db
def test_update_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            UPDATE products
            SET title = "foo"
        """,
    )


@pytest.mark.django_db
def test_delete_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            DELETE products
            SET title = "foo"
        """,
    )


@pytest.mark.django_db
def test_for_update_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            SELECT id
            FROM products FOR UPDATE
        """,
    )


@pytest.mark.django_db
def test_window_declaration_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            SELECT id
            FROM products WINDOW w AS (:w
            PARTITION BY id ORDER BY id)
        """,
    )


@pytest.mark.django_db
def test_cte_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            WITH cte AS (SELECT title FROM products)
            SELECT title
            from cte
        """,
    )


@pytest.mark.django_db
def test_is_distinct_from_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "IS \\(NOT\\) DISTINCT not supported",
        """
            SELECT price
            FROM products
            WHERE tax_rate IS DISTINCT
            FROM price
        """,
    )
    raises_query_error(
        engine_t1,
        "IS \\(NOT\\) DISTINCT not supported",
        """
            SELECT price
            FROM products
            WHERE tax_rate IS NOT DISTINCT
            FROM price
        """,
    )


@pytest.mark.django_db
def test_where_regexp_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            SELECT price
            FROM products
            WHERE title REGEXP 'Foo'
        """,
    )


@pytest.mark.django_db
def test_where_glob_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            SELECT price
            FROM products
            WHERE title GLOB 'Foo*'
        """,
    )


@pytest.mark.django_db
def test_where_match_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            SELECT price
            FROM products
            WHERE title MATCH 'Foo'
        """,
    )


@pytest.mark.django_db
def test_collate_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            SELECT price COLLATE "foo"
            FROM products
        """,
    )


@pytest.mark.django_db
def test_binary_operator_not_allowed(engine_t1):
    raises_query_error(
        engine_t1,
        "Bitwise operations not supported",
        """
            SELECT id & 1
            FROM products
        """,
    )
    raises_query_error(
        engine_t1,
        "Bitwise operations not supported",
        """
            SELECT id | 1
            FROM products
        """,
    )
    raises_query_error(
        engine_t1,
        "Bitwise operations not supported",
        """
            SELECT id << 1
            FROM products
        """,
    )
    raises_query_error(
        engine_t1,
        "Bitwise operations not supported",
        """
            SELECT id >> 1
            FROM products
        """,
    )
    raises_query_error(
        engine_t1,
        "Bitwise operations not supported",
        """
            SELECT id ^ 1
            FROM products
        """,
    )
    raises_query_error(
        engine_t1,
        "Bitwise operations not supported",
        """
            SELECT ~id
            FROM products
        """,
    )


@pytest.mark.django_db
def test_unsupported_function(engine_t1):
    raises_query_error(
        engine_t1,
        "Unsupported expression: SIN\\(3\\)",
        """
            SELECT Sin(3)
            FROM products
        """,
    )


@pytest.mark.django_db
def test_unsupported_any_all(engine_t1):
    raises_query_error(
        engine_t1,
        "Unsupported expression: ANY",
        """
            SELECT id
            FROM products
            WHERE id = ANY(SELECT product FROM orderpositions)
        """,
    )
    raises_query_error(
        engine_t1,
        "Unsupported expression: ALL",
        """
            SELECT id
            FROM products
            WHERE id >= ALL(SELECT product FROM orderpositions)
        """,
    )
    raises_query_error(
        engine_t1,
        "Unsupported expression: SOME",
        """
            SELECT id
            FROM products
            WHERE id = SOME(SELECT product FROM orderpositions)
        """,
    )


@pytest.mark.django_db
def test_select_from_subquery(engine_t1):
    raises_query_error(
        engine_t1,
        "Unsupported FROM statement",
        """
            SELECT id
            FROM (SELECT id, title FROM products)
        """,
    )


@pytest.mark.django_db
def test_aggregate_window(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
            SELECT COUNT(*) FILTER(WHERE status = "paid") OVER (foo)
            FROM orderpositions
        """,
    )


@pytest.mark.django_db
def test_hex_number(engine_t1):
    raises_query_error(
        engine_t1,
        "Invalid expression / Unexpected token",
        """
                SELECT 0x100 AS result
                FROM orderpositions
            """,
    )


@pytest.mark.django_db
def test_other_parameters(engine_t1):
    raises_query_error(
        engine_t1,
        "Unsupported expression: @a",
        """
                SELECT 1
                FROM orderpositions
                where id = @a
            """,
    )
    raises_query_error(
        engine_t1,
        "Placeholder must be named",
        """
                SELECT 1
                FROM orderpositions
                where id = ?
            """,
    )
    raises_query_error(
        engine_t1,
        "Column '\\$a' does not exist in table",
        """
                SELECT 1
                FROM orderpositions
                where id = $a
            """,
    )
