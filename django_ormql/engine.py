import datetime

from .query import Query


class QueryEngine:
    def __init__(self):
        self.tables = {}

    def register_table(self, table):
        self.tables[table.Meta.name] = table

    def query(
        self,
        query,
        placeholders=None,
        timezone=datetime.timezone.utc,
        default_limit=None,
    ):
        return Query(
            query,
            self.tables,
            placeholders,
            timezone,
            default_limit,
        ).evaluate()
