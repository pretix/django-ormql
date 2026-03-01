import datetime
import zoneinfo
from decimal import Decimal

import pytest
from django.conf import settings
from django.utils.timezone import now

from django_ormql.exceptions import QueryError

tz_ny = zoneinfo.ZoneInfo("America/New_York")


@pytest.mark.django_db
def test_unknown_placeholder(engine_t1):
    with pytest.raises(QueryError, match="Placeholder 'var' not filled"):
        res = list(engine_t1.query(
            f"""
            SELECT COUNT(*) as c
            FROM orderpositions
            WHERE quantity = :var
            """
        ))
        assert res == [{"c": 1}]


@pytest.mark.django_db
def test_placeholder(engine_t1):
    res = list(engine_t1.query(
        f"""
        SELECT COUNT(*) as c
        FROM orderpositions
        WHERE quantity = :var
        """,
        placeholders={"var": 3},
    ))
    assert res == [{"c": 1}]
