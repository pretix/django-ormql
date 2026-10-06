import pytest
from django.conf import settings


@pytest.mark.django_db
def test_select_json(engine_t1):
    res = engine_t1.query(
        """
        SELECT address, TYPE_INFO(address) as type_info
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [
        {
            "address": {
                "business": True,
                "quality": 23,
                "city": {
                    "name": "Heidelberg",
                    "state": {"code": "BW", "country": {"code": "DE"}},
                },
            },
            "type_info": "JSONB",
        }
    ]


@pytest.mark.django_db
def test_select_json_key(engine_t1):
    res = engine_t1.query(
        """
        SELECT address->city->state AS state, TYPE_INFO(address->city->state) AS type_info
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [
        {"state": {"code": "BW", "country": {"code": "DE"}}, "type_info": "JSONB"}
    ]

    res = engine_t1.query(
        """
        SELECT address->city->>state AS state
        FROM customers
        WHERE name = "CA"
        """
    )
    assert (
        list(res)[0]["state"].replace(" ", "")
        == '{"code":"BW","country":{"code":"DE"}}'
    )

    res = engine_t1.query(
        """
        SELECT address->city->state->>code AS state,
            LOWER(address->city->state->>code) AS lower_code,
            TYPE_INFO(address->city->state->>code) AS type_info
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"state": "BW", "lower_code": "bw", "type_info": "TEXT"}]


@pytest.mark.django_db
def test_select_json_key_scalar_to_string(engine_t1):
    if "sqlite" in settings.DATABASES["default"]["ENGINE"]:
        pytest.skip("Not supported on SQLite")
    res = engine_t1.query(
        """
        SELECT address->>business AS business
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"business": "true"}]

    res = engine_t1.query(
        """
        SELECT address->>quality AS quality
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"quality": "23"}]


@pytest.mark.django_db
def test_select_json_key_strongly_typed(engine_t1):
    if "sqlite" in settings.DATABASES["default"]["ENGINE"]:
        pytest.skip("Not supported on SQLite")
    res = engine_t1.query(
        """
        SELECT address->business AS business
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"business": True}]

    res = engine_t1.query(
        """
        SELECT address->quality AS quality
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"quality": 23}]


@pytest.mark.django_db
def test_select_json_key_string(engine_t1):
    res = engine_t1.query(
        """
        SELECT address->"city"->"state" AS state
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"state": {"code": "BW", "country": {"code": "DE"}}}]

    res = engine_t1.query(
        """
        SELECT address->"city.state"->"code" AS state
        FROM customers
        WHERE name = "CA"
        """
    )
    assert list(res) == [{"state": "BW"}]


@pytest.mark.django_db
def test_select_json_key_in_where(engine_t1):
    res = engine_t1.query(
        """
        SELECT address->city->state AS state
        FROM customers
        WHERE address->city->>name = "Heidelberg"
        """
    )
    assert list(res) == [{"state": {"code": "BW", "country": {"code": "DE"}}}]

    res = engine_t1.query(
        """
        SELECT address->city->state->>code AS state
        FROM customers
        WHERE address->quality::int = 23
        """
    )
    assert list(res) == [{"state": "BW"}]

    res = engine_t1.query(
        """
        SELECT address->city->state->>code AS state
        FROM customers
        WHERE address->>business = 'true'
        """
    )
    assert list(res) == [{"state": "BW"}]


@pytest.mark.django_db
def test_select_json_key_in_where_strongly_typed(engine_t1):
    if "sqlite" in settings.DATABASES["default"]["ENGINE"]:
        pytest.skip("Not supported on SQLite")
    res = engine_t1.query(
        """
        SELECT address->city->state AS state
        FROM customers
        WHERE address->city->name = '"Heidelberg"'
        """
    )
    assert list(res) == [{"state": {"code": "BW", "country": {"code": "DE"}}}]

    res = engine_t1.query(
        """
        SELECT address->city->state->>code AS state
        FROM customers
        WHERE address->>quality = '23'
        """
    )
    assert list(res) == [{"state": "BW"}]

    res = engine_t1.query(
        """
        SELECT address->city->state->>code AS state
        FROM customers
        WHERE address->business::bool = true
        """
    )
    assert list(res) == [{"state": "BW"}]
