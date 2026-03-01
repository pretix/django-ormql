from .query import Query


class QueryEngine:
    def __init__(self):
        self.tables = {}

    def register_table(self, table):
        self.tables[table.Meta.name] = table

    def query(self, query):
        return Query(query, self.tables).evaluate()