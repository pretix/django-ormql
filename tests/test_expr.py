import datetime
from decimal import Decimal

import pytest
import pytz
from django.conf import settings
from freezegun import freeze_time

from django_ormql.exceptions import QueryNotSupported, QueryError


@pytest.mark.django_db
@pytest.mark.parametrize("expr,result", [
    # Binary operators
    ("single_price + 10", Decimal("29.00")),
    ("single_price + 10.5", Decimal("29.5")),
    ("single_price + quantity", Decimal("22.0")),
    ("single_price - 10", Decimal("9.00")),
    ("single_price - 10.5", Decimal("8.5")),
    ("single_price - quantity", Decimal("16.0")),
    ("single_price * 10", Decimal("190.00")),
    ("single_price * 10.5", Decimal("199.5")),
    ("single_price * quantity", Decimal("57.0")),
    ("single_price / 10", Decimal("1.90")),
    ("single_price / 10.5", Decimal("1.80952380952381")),
    ("single_price / quantity", Decimal("6.33333333333333")),
    ("single_price % 3", Decimal("1.00")),
    ("single_price % 3.0", Decimal("1.00")),
    ("single_price % quantity", Decimal("1.00")),
    # Boolean operators
    ("single_price > 12", True),
    ("single_price < 12", False),
    ("(single_price > 12) OR (single_price < 3)", True),
    ("(single_price > 12) AND (single_price < 3)", False),
    ("single_price IN (19, 20)", True),
    ("single_price BETWEEN 19 AND 20", True),
    ("TRUE AND FALSE", False),
    # Unary operators
    ("+single_price", Decimal("19")),
    ("-single_price", Decimal("-19")),
    # Combinations
    ("-(single_price + 1)", Decimal("-20")),
    ("(single_price - 1) * 10", Decimal("180.00")),
])
def test_simple_math(engine_t1, expr, result):
    res = engine_t1.query(
        f"""
        SELECT single_price, quantity, {expr} AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"single_price": Decimal("19.00"), "quantity": 3, "result": result},
    ]


@pytest.mark.django_db
@pytest.mark.parametrize("expr,result", [
    ("CAST(single_price AS TEXT)", "19"),
    ("single_price::TEXT", "19"),
    ("CAST(single_price AS INT)", 19),
    ("single_price::INT", 19),
    ("CAST(single_price AS BIGINT)", 19),
    ("single_price::BIGINT", 19),
    ("CAST(single_price AS DECIMAL)", Decimal("19.00")),
    ("single_price::DECIMAL", Decimal("19.00")),
    ("CAST(single_price AS FLOAT)", 19.00),
    ("single_price::FLOAT", 19.00),
    ("CAST(single_price AS DOUBLE)", 19.00),
    ("single_price::DOUBLE", 19.00),
    ("CAST(single_price AS BOOL)", True),
    ("single_price::BOOL", True),
    ("CAST(single_price AS BOOLEAN)", True),
    ("single_price::BOOLEAN", True),
])
def test_cast(engine_t1, expr, result):
    if isinstance(result, bool) and 'sqlite' in settings.DATABASES['default']['ENGINE']:
        pytest.skip('Not supported on SQLite')

    res = engine_t1.query(
        f"""
        SELECT single_price, quantity, {expr} AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"single_price": Decimal("19.00"), "quantity": 3, "result": result},
    ]


@pytest.mark.django_db
def test_case_when_else(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT CASE
            WHEN order.status == "canceled"
            THEN single_price * quantity
            ELSE 0
        END AS revenue
        FROM orderpositions
        """
    )
    assert list(res) == [
        {'revenue': Decimal('0')},
        {'revenue': Decimal('0')},
        {'revenue': Decimal('21.40')},
        {'revenue': Decimal('19')},
        {'revenue': Decimal('10.70')}
    ]


@pytest.mark.django_db
def test_case_base_when_else(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT CASE order.status
            WHEN "canceled"
            THEN single_price * quantity
            ELSE 0
        END AS revenue
        FROM orderpositions
        """
    )
    assert list(res) == [
        {'revenue': Decimal('0')},
        {'revenue': Decimal('0')},
        {'revenue': Decimal('21.40')},
        {'revenue': Decimal('19')},
        {'revenue': Decimal('10.70')}
    ]


@pytest.mark.django_db
def test_case_base_when_no_else(engine_t1):
    res = engine_t1.query(
        f"""
        SELECT CASE order.status
            WHEN "canceled"
            THEN single_price * quantity
        END AS revenue
        FROM orderpositions
        """
    )
    assert list(res) == [
        {'revenue': None},
        {'revenue': None},
        {'revenue': Decimal('21.40')},
        {'revenue': Decimal('19')},
        {'revenue': Decimal('10.70')}
    ]


@pytest.mark.django_db
@pytest.mark.parametrize("expr,result", [
    # Number functions
    ("GREATEST(4, 3, 5)", 5),
    ("GREATEST(single_price, tax_rate)", 19),
    ("LEAST(4, 3, 5)", 3),
    ("LEAST(single_price, tax_rate)", 19),
    ("ABS(single_price)", Decimal("19.00")),
    ("ABS(-3)", 3),
    ("ABS(-3.2)", Decimal("3.2")),
    ("CEIL(single_price / 2)", 10),
    ("FLOOR(single_price / 2)", 9),
    ("ROUND(single_price / 2)", Decimal("10.00")),
    ("ROUND(single_price / 2, 2)", Decimal("9.50")),
    ("MOD(single_price, 3)", 1),

    # Date functions
    ("EXTRACT('year' FROM order.created)", 2024),
    ("EXTRACT(YEAR FROM order.created)", 2024),
    ("EXTRACT('iso_year' FROM order.created)", 2024),
    ("EXTRACT(ISO_YEAR FROM order.created)", 2024),
    ("EXTRACT('quarter' FROM order.created)", 4),
    ("EXTRACT(QUARTER FROM order.created)", 4),
    ("EXTRACT('month' FROM order.created)", 12),
    ("EXTRACT(MONTH FROM order.created)", 12),
    ("EXTRACT('day' FROM order.created)", 14),
    ("EXTRACT(DAY FROM order.created)", 14),
    ("EXTRACT('week' FROM order.created)", 50),
    ("EXTRACT(WEEK FROM order.created)", 50),
    ("EXTRACT('week_day' FROM order.created)", 7),
    ("EXTRACT(WEEK_DAY FROM order.created)", 7),
    ("EXTRACT('iso_week_day' FROM order.created)", 6),
    ("EXTRACT(ISO_WEEK_DAY FROM order.created)", 6),
    ("EXTRACT('hour' FROM order.created)", 11),
    ("EXTRACT(HOUR FROM order.created)", 11),
    ("EXTRACT('minute' FROM order.created)", 13),
    ("EXTRACT(MINUTE FROM order.created)", 13),
    ("EXTRACT('second' FROM order.created)", 14),
    ("EXTRACT(SECOND FROM order.created)", 14),
    # TODO: test timezone support
    ("DATETRUNC('year', order.created)", datetime.datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)),
    ("DATETRUNC('quarter', order.created)", datetime.datetime(2024, 10, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)),
    ("DATETRUNC('month', order.created)", datetime.datetime(2024, 12, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)),
    ("DATETRUNC('day', order.created)", datetime.datetime(2024, 12, 14, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)),
    ("DATETRUNC('week', order.created)", datetime.datetime(2024, 12, 9, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)),
    ("DATETRUNC('hour', order.created)", datetime.datetime(2024, 12, 14, 11, 0, 0, 0, tzinfo=datetime.timezone.utc)),
    ("DATETRUNC('minute', order.created)", datetime.datetime(2024, 12, 14, 11, 13, 0, 0, tzinfo=datetime.timezone.utc)),
    ("DATETRUNC('second', order.created)",
     datetime.datetime(2024, 12, 14, 11, 13, 14, 0, tzinfo=datetime.timezone.utc)),
    # TODO: test timezone support

    # String functions
    ("CONCAT(product.title, ' for ', order.customer.name)", "Lord of the rings DVD for CA"),
    ("product.title || ' for ' || order.customer.name", "Lord of the rings DVD for CA"),
    ("LEFT(product.title, 3)", "Lor"),
    ("RIGHT(product.title, 3)", "DVD"),
    ("LENGTH(product.title)", 21),
    ("LOWER(product.title)", "lord of the rings dvd"),
    ("upper(product.title)", "LORD OF THE RINGS DVD"),
    ("LPAD(UPPER(product.title), 23)", "  LORD OF THE RINGS DVD"),
    ("LPAD(UPPER(product.title), 23, '_')", "__LORD OF THE RINGS DVD"),
    ("RPAD(UPPER(product.title), 23)", "LORD OF THE RINGS DVD  "),
    ("RPAD(UPPER(product.title), 23, '_')", "LORD OF THE RINGS DVD__"),
    ("REPLACE(product.title, ' ')", "LordoftheringsDVD"),
    ("REPLACE(product.title, ' ', '_')", "Lord_of_the_rings_DVD"),
    ("INSTR(product.title, 'of')", 6),
    ("SUBSTRING(product.title, 6)", "of the rings DVD"),
    ("SUBSTRING(product.title, INSTR(product.title, 'DVD'))", "DVD"),
    ("SUBSTRING(product.title, 6, 2)", "of"),
])
def test_functions(engine_t1, expr, result):
    res = engine_t1.query(
        f"""
        SELECT single_price, quantity, {expr} AS result
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {"single_price": Decimal("19.00"), "quantity": 3, "result": result},
    ]


@pytest.mark.django_db
@pytest.mark.parametrize("expr", [
    # Number functions
    "GREATEST(4)",
    "LEAST(4)",
    "ABS(single_price, 2)",
    "CEIL(single_price / 2, 3)",
    "FLOOR(single_price / 2, 4)",
    "ROUND(single_price / 2, 4, 5)",
    # "MOD(single_price, 3, 4)", parser ignores this for some reason
    "EXTRACT('year' FROM order.created, 12)",
    "EXTRACT('year', order.created, 12)",
    "EXTRACT('year')",
    "DATETRUNC('year', order.created, 4)",
    "DATETRUNC('year')",
    "LEFT(product.title)",
    "RIGHT(product.title)",
    "LENGTH(product.title, 3)",
    "LOWER(product.title, 'foo')",
    "UPPER(product.title, 'foo')",
    "LPAD(product.title)",
    "RPAD(product.title)",
    "REPLACE(product.title)",
    "INSTR(product.title)",
    "INSTR(product.title, 'of', 4)",
    "SUBSTRING(product.title)",
])
def test_functions_wrong_arity(engine_t1, expr):
    with pytest.raises((QueryError, ValueError)):
        list(engine_t1.query(
            f"""
            SELECT single_price, quantity, {expr} AS result
            FROM orderpositions
            WHERE quantity = 3
            """
        ))
