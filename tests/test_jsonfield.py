import pytest


@pytest.mark.django_db
def test_select_json(engine_t1):
    res = engine_t1.query(
        """
        SELECT address
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [
        {
            "address": {
                "city": {
                    "name": "Heidelberg",
                    "state": {"code": "BW", "country": {"code": "DE"}},
                }
            }
        }
    ]


@pytest.mark.django_db
def test_select_json_key(engine_t1):
    res = engine_t1.query(
        """
        SELECT address->city->state AS state
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"state": {"code": "BW", "country": {"code": "DE"}}}]
    res = engine_t1.query(
        """
        SELECT address->city->state->code AS state
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"state": "BW"}]
