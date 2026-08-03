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
        dry_run=False,
    ):
        query = Query(
            query,
            self.tables,
            placeholders,
            timezone,
            default_limit,
        )
        if dry_run:
            return query.parse()
        else:
            return query.evaluate()
