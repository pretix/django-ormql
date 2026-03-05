import pytest


@pytest.mark.django_db
def test_line_comment(engine_t1):
    res = engine_t1.query(
        """
        SELECT title
        -- Just a comment
        FROM categories
        """
    )
    assert list(res) == [
        {"title": "Books"},
        {"title": "DVDs"},
    ]


@pytest.mark.django_db
def test_inline_comment(engine_t1):
    res = engine_t1.query(
        """
        SELECT /* foo */ title
        FROM categories
        """
    )
    assert list(res) == [
        {"title": "Books"},
        {"title": "DVDs"},
    ]
