from .query import sql_to_queryset

class QueryEngine:
    def __init__(self):
        self.tables = {}

    def register_table(self, table):
        self.tables[table.Meta.name] = table

    def query(self, query):
        return sql_to_queryset(query, self.tables)