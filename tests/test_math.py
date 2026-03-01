from decimal import Decimal

import pytest


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
