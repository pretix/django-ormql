import datetime
import re
import zoneinfo
from decimal import Decimal

import pytest
from django.conf import settings
from django.utils.timezone import now

from django_ormql.exceptions import QueryError, QueryNotSupported

tz_ny = zoneinfo.ZoneInfo("America/New_York")


@pytest.mark.django_db
@pytest.mark.parametrize(
    "expr,result,type_guess",
    [
        # String quotes
        ('"foo"', "foo", "TEXT"),
        ("'foo'", "foo", "TEXT"),
        # Number formats
        ("10e2", Decimal(1000), "DECIMAL"),
        # Binary operators
        # Decimals being reported as floats is currently expected, we haven't implemented the logic since we have no need
        ("10.5::float + 10::int", Decimal("20.5"), "FLOAT"),
        ("single_price::decimal + 10.5::float + 10::int", Decimal("39.5"), "FLOAT"),
        ("single_price + 10", Decimal("29.00"), "FLOAT"),
        ("single_price + 10.5", Decimal("29.5"), "FLOAT"),
        ("single_price + quantity", Decimal("22.0"), "FLOAT"),
        ("single_price - 10", Decimal("9.00"), "FLOAT"),
        ("single_price - 10.5", Decimal("8.5"), "FLOAT"),
        ("single_price - quantity", Decimal("16.0"), "FLOAT"),
        ("single_price * 10", Decimal("190.00"), "FLOAT"),
        ("single_price * 10.5", Decimal("199.5"), "FLOAT"),
        ("single_price * quantity", Decimal("57.0"), "FLOAT"),
        ("single_price / 10", Decimal("1.90"), "FLOAT"),
        ("ROUND(single_price / 10.5, 2)", Decimal("1.81"), "FLOAT"),
        ("ROUND(single_price / quantity, 2)", Decimal("6.33"), "FLOAT"),
        ("single_price % 3", Decimal("1.00"), "FLOAT"),
        ("single_price % 3.0", Decimal("1.00"), "FLOAT"),
        ("single_price % quantity", Decimal("1.00"), "FLOAT"),
        # Boolean operators
        ("single_price > 12", True, "BOOLEAN"),
        ("single_price < 12", False, "BOOLEAN"),
        ("(single_price > 12) OR (single_price < 3)", True, "BOOLEAN"),
        ("(single_price > 12) AND (single_price < 3)", False, "BOOLEAN"),
        ("single_price IN (19, 20)", True, "BOOLEAN"),
        ("single_price BETWEEN 19 AND 20", True, "BOOLEAN"),
        ("TRUE AND FALSE", False, "BOOLEAN"),
        # Unary operators
        ("+single_price", Decimal(19), "DECIMAL"),
        ("-single_price", Decimal(-19), "DECIMAL"),
        # Combinations
        ("-(single_price + 1)", Decimal(-20), "FLOAT"),
        ("(single_price - 1) * 10", Decimal("180.00"), "FLOAT"),
    ],
)
def test_simple_math(engine_t1, expr, result, type_guess):
    res = engine_t1.query(
        f"""
        SELECT single_price, quantity, {expr} AS result, TYPE_INFO({expr}) AS type_guess
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    assert list(res) == [
        {
            "single_price": Decimal("19.00"),
            "quantity": 3,
            "result": result,
            "type_guess": type_guess,
        },
    ]


@pytest.mark.django_db
@pytest.mark.parametrize(
    "expr,result,type_guess",
    [
        ("CAST(single_price AS TEXT)", re.compile(r"19(\.00)?"), "TEXT"),
        ("single_price::TEXT", re.compile(r"19(\.00)?"), "TEXT"),
        ("CAST(single_price AS INT)", 19, "INT"),
        ("single_price::INT", 19, "INT"),
        ("CAST(single_price AS BIGINT)", 19, "INT"),
        ("single_price::BIGINT", 19, "INT"),
        ("CAST(single_price AS DECIMAL)", Decimal("19.00"), "DECIMAL"),
        ("single_price::DECIMAL", Decimal("19.00"), "DECIMAL"),
        ("CAST(single_price AS FLOAT)", 19.00, "FLOAT"),
        ("single_price::FLOAT", 19.00, "FLOAT"),
        ("CAST(single_price AS DOUBLE)", 19.00, "FLOAT"),
        ("single_price::DOUBLE", 19.00, "FLOAT"),
        ("CAST(single_price > 0 AS BOOL)", True, "BOOLEAN"),
        ("(single_price > 0)::BOOL", True, "BOOLEAN"),
        ("CAST(single_price > 0 AS BOOLEAN)", True, "BOOLEAN"),
        ("(single_price > 0)::BOOLEAN", True, "BOOLEAN"),
        (
            "CAST(order.created AS DATETIME)",
            datetime.datetime(2024, 12, 14, 2, 13, 14, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "order.created::DATETIME",
            datetime.datetime(2024, 12, 14, 2, 13, 14, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        ("CAST(order.created AS DATE)", datetime.date(2024, 12, 14), "DATE"),
        ("order.created::DATE", datetime.date(2024, 12, 14), "DATE"),
        ("CAST(order.created AS TIME)", datetime.time(2, 13, 14), "TIME"),
        ("order.created::TIME", datetime.time(2, 13, 14), "TIME"),
    ],
)
def test_cast(engine_t1, expr, result, type_guess):
    if isinstance(result, bool) and "sqlite" in settings.DATABASES["default"]["ENGINE"]:
        pytest.skip("Not supported on SQLite")

    res = engine_t1.query(
        f"""
        SELECT single_price, quantity, {expr} AS result, TYPE_INFO({expr}) AS type_guess
        FROM orderpositions
        WHERE quantity = 3
        """
    )
    if isinstance(result, re.Pattern):
        assert result.match(list(res)[0]["result"])
    else:
        assert list(res) == [
            {
                "single_price": Decimal("19.00"),
                "quantity": 3,
                "result": result,
                "type_guess": type_guess,
            },
        ]


@pytest.mark.django_db
def test_cast_fail(engine_t1):
    with pytest.raises(QueryError):
        list(
            engine_t1.query(
                """
            SELECT EXTRACT("day", "123"::INT) FROM orderpositions
            """
            )
        )


@pytest.mark.django_db
def test_case_when_else(engine_t1):
    res = engine_t1.query(
        """
        SELECT CASE
            WHEN order.status == "canceled"
            THEN single_price * quantity
            ELSE 0
        END AS revenue
        FROM orderpositions
        """
    )
    assert list(res) == [
        {"revenue": Decimal(0)},
        {"revenue": Decimal(0)},
        {"revenue": Decimal("21.40")},
        {"revenue": Decimal(19)},
        {"revenue": Decimal("10.70")},
    ]


@pytest.mark.django_db
def test_case_base_when_else(engine_t1):
    res = engine_t1.query(
        """
        SELECT CASE order.status
            WHEN "canceled"
            THEN single_price * quantity
            ELSE 0
        END AS revenue
        FROM orderpositions
        """
    )
    assert list(res) == [
        {"revenue": Decimal(0)},
        {"revenue": Decimal(0)},
        {"revenue": Decimal("21.40")},
        {"revenue": Decimal(19)},
        {"revenue": Decimal("10.70")},
    ]


@pytest.mark.django_db
def test_case_base_when_no_else(engine_t1):
    res = engine_t1.query(
        """
        SELECT CASE order.status
                   WHEN "canceled"
                       THEN single_price * quantity
                   END AS revenue
        FROM orderpositions
        """
    )
    assert list(res) == [
        {"revenue": None},
        {"revenue": None},
        {"revenue": Decimal("21.40")},
        {"revenue": Decimal(19)},
        {"revenue": Decimal("10.70")},
    ]


@pytest.mark.django_db
def test_case_type_guess(engine_t1):
    res = engine_t1.query(
        """
        SELECT TYPE_INFO(CASE order.status
            WHEN "canceled"
            THEN single_price * quantity
        END) AS type_info
        FROM orderpositions
        """
    )
    assert list(res)[0] == {"type_info": "FLOAT"}


@pytest.mark.django_db
@pytest.mark.parametrize(
    "expr,result,type_guess",
    [
        # Number functions
        ("GREATEST(4, 3, 5)", 5, "FLOAT"),
        ("GREATEST(4, 3.4, 5)", 5, "FLOAT"),
        ("GREATEST(single_price, tax_rate)", 19, "FLOAT"),
        ("LEAST(4, 3, 5)", 3, "FLOAT"),
        ("LEAST(4.5, 3, 5)", 3, "FLOAT"),
        ("LEAST(single_price, tax_rate)", 19, "FLOAT"),
        ("ABS(single_price)", Decimal("19.00"), "FLOAT"),
        ("ABS(-3)", 3, "FLOAT"),
        ("ABS(-3.2)", Decimal("3.2"), "FLOAT"),
        ("CEIL(single_price / 2)", 10, "FLOAT"),
        ("FLOOR(single_price / 2)", 9, "FLOAT"),
        ("ROUND(single_price / 2)", Decimal("10.00"), "FLOAT"),
        ("ROUND(single_price / 2, 2)", Decimal("9.50"), "FLOAT"),
        ("MOD(single_price, 3)", 1, "FLOAT"),
        # Datetime functions
        ("EXTRACT('year' FROM order.created)", 2024, "INT"),
        ("EXTRACT(YEAR FROM order.created)", 2024, "INT"),
        ("EXTRACT('iso_year' FROM order.created)", 2024, "INT"),
        ("EXTRACT(ISO_YEAR FROM order.created)", 2024, "INT"),
        ("EXTRACT('quarter' FROM order.created)", 4, "INT"),
        ("EXTRACT(QUARTER FROM order.created)", 4, "INT"),
        ("EXTRACT('month' FROM order.created)", 12, "INT"),
        ("EXTRACT(MONTH FROM order.created)", 12, "INT"),
        ("EXTRACT('day' FROM order.created)", 14, "INT"),
        ("EXTRACT(DAY FROM order.created)", 14, "INT"),
        ("EXTRACT('week' FROM order.created)", 50, "INT"),
        ("EXTRACT(WEEK FROM order.created)", 50, "INT"),
        ("EXTRACT('week_day' FROM order.created)", 7, "INT"),
        ("EXTRACT(WEEK_DAY FROM order.created)", 7, "INT"),
        ("EXTRACT('iso_week_day' FROM order.created)", 6, "INT"),
        ("EXTRACT(ISO_WEEK_DAY FROM order.created)", 6, "INT"),
        ("EXTRACT('hour' FROM order.created)", 2, "INT"),
        ("EXTRACT(HOUR FROM order.created)", 2, "INT"),
        ("EXTRACT('minute' FROM order.created)", 13, "INT"),
        ("EXTRACT(MINUTE FROM order.created)", 13, "INT"),
        ("EXTRACT('second' FROM order.created)", 14, "INT"),
        ("EXTRACT(SECOND FROM order.created)", 14, "INT"),
        (
            "DATETRUNC('year', order.created)",
            datetime.datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "DATETRUNC('quarter', order.created)",
            datetime.datetime(2024, 10, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "DATETRUNC('month', order.created)",
            datetime.datetime(2024, 12, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "DATETRUNC('day', order.created)",
            datetime.datetime(2024, 12, 14, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "DATETRUNC('week', order.created)",
            datetime.datetime(2024, 12, 9, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "DATETRUNC('hour', order.created)",
            datetime.datetime(2024, 12, 14, 2, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "DATETRUNC('minute', order.created)",
            datetime.datetime(2024, 12, 14, 2, 13, 0, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        (
            "DATETRUNC('second', order.created)",
            datetime.datetime(2024, 12, 14, 2, 13, 14, 0, tzinfo=datetime.timezone.utc),
            "DATETIME",
        ),
        # Date functions
        ("EXTRACT('year' FROM product.publication_date)", 2026, "INT"),
        ("EXTRACT(YEAR FROM product.publication_date)", 2026, "INT"),
        ("EXTRACT('iso_year' FROM product.publication_date)", 2026, "INT"),
        ("EXTRACT(ISO_YEAR FROM product.publication_date)", 2026, "INT"),
        ("EXTRACT('quarter' FROM product.publication_date)", 1, "INT"),
        ("EXTRACT(QUARTER FROM product.publication_date)", 1, "INT"),
        ("EXTRACT('month' FROM product.publication_date)", 2, "INT"),
        ("EXTRACT(MONTH FROM product.publication_date)", 2, "INT"),
        ("EXTRACT('day' FROM product.publication_date)", 20, "INT"),
        ("EXTRACT(DAY FROM product.publication_date)", 20, "INT"),
        ("EXTRACT('week' FROM product.publication_date)", 8, "INT"),
        ("EXTRACT(WEEK FROM product.publication_date)", 8, "INT"),
        ("EXTRACT('week_day' FROM product.publication_date)", 6, "INT"),
        ("EXTRACT(WEEK_DAY FROM product.publication_date)", 6, "INT"),
        ("EXTRACT('iso_week_day' FROM product.publication_date)", 5, "INT"),
        ("EXTRACT(ISO_WEEK_DAY FROM product.publication_date)", 5, "INT"),
        (
            "DATETRUNC('year', product.publication_date)",
            datetime.date(2026, 1, 1),
            "DATE",
        ),
        (
            "DATETRUNC('quarter', product.publication_date)",
            datetime.date(2026, 1, 1),
            "DATE",
        ),
        (
            "DATETRUNC('month', product.publication_date)",
            datetime.date(2026, 2, 1),
            "DATE",
        ),
        (
            "DATETRUNC('day', product.publication_date)",
            datetime.date(2026, 2, 20),
            "DATE",
        ),
        # Time functions
        ("EXTRACT('hour' FROM product.category.closing_hour)", 22, "INT"),
        ("EXTRACT(HOUR FROM product.category.closing_hour)", 22, "INT"),
        ("EXTRACT('minute' FROM product.category.closing_hour)", 30, "INT"),
        ("EXTRACT(MINUTE FROM product.category.closing_hour)", 30, "INT"),
        ("EXTRACT('second' FROM product.category.closing_hour)", 0, "INT"),
        ("EXTRACT(SECOND FROM product.category.closing_hour)", 0, "INT"),
        (
            "DATETRUNC('hour', product.category.closing_hour)",
            datetime.time(22, 0),
            "TIME",
        ),
        (
            "DATETRUNC('minute', product.category.closing_hour)",
            datetime.time(22, 30, 0),
            "TIME",
        ),
        (
            "DATETRUNC('second', product.category.closing_hour)",
            datetime.time(22, 30, 0),
            "TIME",
        ),
        # String functions
        (
            "CONCAT(product.title, ' for ', order.customer.name)",
            "Lord of the rings DVD for CA",
            "TEXT",
        ),
        (
            "CONCAT(order.status, ' / ', order.comment)",  # combine CharField and TextField
            "paid / No comment",
            "TEXT",
        ),
        (
            "product.title || ' for ' || order.customer.name",
            "Lord of the rings DVD for CA",
            "TEXT",
        ),
        ("LEFT(product.title, 3)", "Lor", "TEXT"),
        ("RIGHT(product.title, 3)", "DVD", "TEXT"),
        ("LENGTH(product.title)", 21, "INT"),
        ("LOWER(product.title)", "lord of the rings dvd", "TEXT"),
        ("upper(product.title)", "LORD OF THE RINGS DVD", "TEXT"),
        ("LPAD(UPPER(product.title), 23)", "  LORD OF THE RINGS DVD", "TEXT"),
        ("LPAD(UPPER(product.title), 23, '_')", "__LORD OF THE RINGS DVD", "TEXT"),
        ("RPAD(UPPER(product.title), 23)", "LORD OF THE RINGS DVD  ", "TEXT"),
        ("RPAD(UPPER(product.title), 23, '_')", "LORD OF THE RINGS DVD__", "TEXT"),
        ("REPLACE(product.title, ' ')", "LordoftheringsDVD", "TEXT"),
        ("REPLACE(product.title, ' ', '_')", "Lord_of_the_rings_DVD", "TEXT"),
        ("INSTR(product.title, 'of')", 6, "INT"),
        ("SUBSTRING(product.title, 6)", "of the rings DVD", "TEXT"),
        ("SUBSTRING(product.title, INSTR(product.title, 'DVD'))", "DVD", "TEXT"),
        ("SUBSTRING(product.title, 6, 2)", "of", "TEXT"),
    ],
)
def test_functions(engine_t1, expr, result, type_guess):
    res = list(
        engine_t1.query(
            f"""
        SELECT single_price, quantity, {expr} AS result, TYPE_INFO({expr}) AS type_guess
        FROM orderpositions
        WHERE quantity = 3
        """
        )
    )
    assert res == [
        {
            "single_price": Decimal("19.00"),
            "quantity": 3,
            "result": result,
            "type_guess": type_guess,
        },
    ]
    if type_guess == "DATE":
        # date and datetime are quite compatible, let's be sure about what we get
        assert type(res[0]["result"]) == datetime.date


@pytest.mark.django_db
@pytest.mark.parametrize(
    "expr",
    [
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
        "EXTRACT('invalid', order.created)",
        "DATETRUNC('year', order.created, 4)",
        "DATETRUNC('year')",
        "DATETRUNC('invalid', order.created)",
        "DATETRUNC(CASE WHEN 1 = 2 THEN 'year' ELSE 'month' END, order.created)",
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
        "INSTR(product.title, 'of', 4, 5)",
        "SUBSTRING(product.title)",
        "SUBSTRING(product.title, 2, 3, 4, 5)",
    ],
)
def test_functions_wrong_arity_or_input(engine_t1, expr):
    with pytest.raises(QueryError):
        list(
            engine_t1.query(
                f"""
            SELECT single_price, quantity, {expr} AS result
            FROM orderpositions
            WHERE quantity = 3
            """
            )
        )


@pytest.mark.django_db
def test_current_date(engine_t1):
    res = list(
        engine_t1.query(
            """
        SELECT CURRENT_DATE AS d, TYPE_INFO(CURRENT_DATE) AS type_guess
        FROM orderpositions
        WHERE quantity = 3
        """
        )
    )
    assert type(res[0]["d"]) is datetime.date
    assert res[0]["type_guess"] == "DATE"
    assert abs(now().date() - res[0]["d"]) < datetime.timedelta(days=1)


@pytest.mark.django_db
def test_current_datetime(engine_t1):
    res = list(
        engine_t1.query(
            """
        SELECT CURRENT_TIMESTAMP AS d, TYPE_INFO(CURRENT_TIMESTAMP) AS type_guess
        FROM orderpositions
        WHERE quantity = 3
        """
        )
    )
    assert type(res[0]["d"]) is datetime.datetime
    assert res[0]["type_guess"] == "DATETIME"
    assert abs(now() - res[0]["d"]) < datetime.timedelta(minutes=1)


@pytest.mark.django_db
def test_current_time(engine_t1):
    res = list(
        engine_t1.query(
            """
        SELECT CURRENT_TIME AS d, TYPE_INFO(CURRENT_TIME) AS type_guess
        FROM orderpositions
        WHERE quantity = 3
        """
        )
    )
    assert type(res[0]["d"]) is datetime.time
    assert res[0]["type_guess"] == "TIME"
    assert now().astimezone(datetime.timezone.utc).time().minute == res[0]["d"].minute


@pytest.mark.django_db
@pytest.mark.parametrize(
    "expr,result",
    [
        ("EXTRACT('day' FROM order.created)", 13),
        ("EXTRACT('hour' FROM order.created)", 21),
        ("EXTRACT('minute' FROM order.created)", 13),
        (
            "DATETRUNC('day', order.created)",
            datetime.datetime(2024, 12, 13, 0, 0, 0, 0, tzinfo=tz_ny),
        ),
        (
            "DATETRUNC('hour', order.created)",
            datetime.datetime(2024, 12, 13, 21, 0, 0, 0, tzinfo=tz_ny),
        ),
        (
            "DATETRUNC('minute', order.created)",
            datetime.datetime(2024, 12, 13, 21, 13, 0, 0, tzinfo=tz_ny),
        ),
    ],
)
def test_functions_with_timezone(engine_t1, expr, result):
    res = engine_t1.query(
        f"""
        SELECT single_price, quantity, {expr} AS result
        FROM orderpositions
        WHERE quantity = 3
        """,
        timezone=tz_ny,
    )
    assert list(res) == [
        {"single_price": Decimal("19.00"), "quantity": 3, "result": result},
    ]


@pytest.mark.django_db
@pytest.mark.parametrize(
    "expr,result",
    [
        (
            "EXTRACT('minute' FROM product.publication_date)",
            "Unsupported extract value",
        ),
        ("EXTRACT('hour' FROM product.publication_date)", "Unsupported extract value"),
        (
            "EXTRACT('second' FROM product.publication_date)",
            "Unsupported extract value",
        ),
        ("DATETRUNC('hour', product.publication_date)", "Unsupported truncation type"),
        (
            "DATETRUNC('minute', product.publication_date)",
            "Unsupported truncation type",
        ),
        (
            "DATETRUNC('second', product.publication_date)",
            "Unsupported truncation type",
        ),
    ],
)
def test_date_too_much_granularity(engine_t1, expr, result):
    with pytest.raises(QueryNotSupported) as e:
        engine_t1.query(
            f"""
            SELECT {expr} AS result
            FROM orderpositions
            WHERE quantity = 3
            """,
            timezone=tz_ny,
        )
        assert result in str(e)
