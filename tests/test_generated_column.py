import pytest


@pytest.mark.django_db
def test_select_same_table(engine_t1):
    res = engine_t1.query(
        """
        SELECT status, validity, email
        FROM orders
        """
    )
    assert list(res) == [
        {"status": "paid", "validity": "valid", "email": "ca1@example.com"},
        {"status": "canceled", "validity": "invalid", "email": "cb2@example.com"},
        {"status": "canceled", "validity": "invalid", "email": None},
    ]


@pytest.mark.django_db
def test_select_joined_table(engine_t1):
    res = engine_t1.query(
        """
        SELECT order.status, order.validity, order.email
        FROM orderpositions
        """
    )
    assert list(res) == [
        {
            "order.status": "paid",
            "order.validity": "valid",
            "order.email": "ca1@example.com",
        },
        {
            "order.status": "paid",
            "order.validity": "valid",
            "order.email": "ca1@example.com",
        },
        {
            "order.status": "canceled",
            "order.validity": "invalid",
            "order.email": "cb2@example.com",
        },
        {
            "order.status": "canceled",
            "order.validity": "invalid",
            "order.email": "cb2@example.com",
        },
        {"order.status": "canceled", "order.validity": "invalid", "order.email": None},
    ]


@pytest.mark.django_db
def test_select_function_col(engine_t1):
    res = engine_t1.query(
        """
        SELECT order.email_upper
        FROM orderpositions LIMIT 1
        """
    )
    assert list(res) == [
        {"order.email_upper": "CA1@EXAMPLE.COM"},
    ]


@pytest.mark.django_db
def test_select_static_col(engine_t1):
    res = engine_t1.query(
        """
        SELECT order.static_value
        FROM orderpositions LIMIT 1
        """
    )
    assert list(res) == [
        {"order.static_value": 2},
    ]


@pytest.mark.django_db
def test_subquery_col(engine_t1):
    res = engine_t1.query(
        """
        SELECT email, position_count
        FROM customers
        """
    )
    assert list(res) == [
        {"email": "ca1@example.com", "position_count": 2},
        {"email": "cb2@example.com", "position_count": 2},
    ]


@pytest.mark.django_db
def test_subquery_related_col(engine_t1):
    res = engine_t1.query(
        """
        SELECT order.email as email, order.customer.position_count as position_count
        FROM orderpositions
        """
    )
    assert list(res) == [
        {"email": "ca1@example.com", "position_count": 2},
        {"email": "ca1@example.com", "position_count": 2},
        {"email": "cb2@example.com", "position_count": 2},
        {"email": "cb2@example.com", "position_count": 2},
        {"email": None, "position_count": None},
    ]
