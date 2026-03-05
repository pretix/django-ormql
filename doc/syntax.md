# ORMQL query syntax

## Differences to SQL

- The only query type supported are `SELECT` queries.
- Only one table can be contained in the `FROM` part of the query.
- Foreign key traversal is possible through an object-oriented-style syntax, e.g. `SELECT customer.name FROM orders` will automatically create a join from the orders table to the customers table and resolve the name column.
- Subqueries support a special `OUTER(id)` syntax to refer to a column of the outer query, similar do Django's [OuterRef](https://docs.djangoproject.com/en/6.0/ref/models/expressions/#referencing-columns-from-the-outer-queryset) function.
- Only named parameters (`:name`) are supported.
- The following features are not supported:
  - Manual joins
  - Table aliases
  - `SELECT *`
  - `WITH` / common table expressions
  - Window functions
  - Aggregate functions with multiple arguments or `ORDER BY` clauses
  - Bitwise operators
  - `IS DISTINCT`
  - `LIMIT` and `OFFSET` with complex expressions
  - `UNION`, `INTERSECT`, `EXCEPT`

This list of differences is not complete.

## SELECT syntax

```
select-query: SELECT [DISTINCT]
              result-column [, ...]
              FROM table
              [WHERE expr]
              [GROUP BY expr [, ...]]
              [HAVING expr]
              [ORDER BY ordering-term [, ...]]
              [LIMIT numeric-literal] [OFFSET numeric-literal]

result-column: expr [AS column-alias]

ordering-term: expr [ASC | DESC] [NULLS FIRST | NULLS LAST]

literal-value: numeric-literal | string-literal | NULL | TRUE | FALSE |
               CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP

expr: literal-value |
      column-ref |
      unary-operator expr |
      expr binary-operator expr |
      function-name ( * | [DISTINCT] expr [, ...] ) [FILTER (WHERE expr)] |
      CAST ( expr AS type-name ) | expr::type-name |
      expr [NOT] LIKE expr |
      expr ISNULL | expr NOTNULL | expr NOT NULL |
      expr [NOT] BETWEEN [expr] AND [expr] |
      expr [NOT] IN ( expr [, ...] | select-query ) |
      [NOT] EXISTS ( select-query ) |
      CASE [expr] WHEN expr THEN expr [WHEN ...] [ELSE expr] END |
      :placeholder-name

column-ref: [column-ref.]column-name | | OUTER(column-ref)

unary-operator: + | - | NOT

binary-operator: || | -> |
                 + | - | * | / | % | 
                 <= | >= | < | > |
                 = | == | <> | != | IS | IS NOT |
                 IN | LIKE | ILIKE |
                 AND | NOR

type-name: BOOL[EAN] | [BIG]INT | DECIMAL | FLOAT | DOUBLE | JSONB | TEXT | TIME | DATE | DATETIME
```

## Syntax notes

- String literals can be quited with `"` or `'`.
- Column or table names can be escaped with <code>`</code>.
- Column and table names are case-sensitive, syntax keywords are not.
- Comments are supported with `--` at the start of the line or `/* comment */` syntax.
- The result of comparisons between different types or with NULL depends on the underlying database.
- Casting behaviour depends on the underlying database.
- Math operations involving more than one of the numeric type are always casted to either decimal or float, i.e. `10 / 3` will not return `3`, but always `3.3333...`.
- `||` is a concatenation operator and `->` a JSON traversal operator.

## Supported functions

### Aggregate functions

- `AVG(* | expr)`
- `COUNT(* | expr)`
- `MAX(* | expr)`
- `MIN(* | expr)`
- `STDDEV(* | expr)`
- `VARIANCE(* | expr)`
- `SUM(* | expr)`

### Numeric functions

- `GREATEST(expr, expr)`
- `LEAST(expr, expr)`
- `ABS(expr)`
- `CEIL(expr[, scale])`
- `FLOOR(expr[, scale])`
- `ROUND(expr[, scale])`
- `MOD(expr, expr)`

### String functions

- `CONCAT(expr, expr)`
- `LEFT(expr, length)`
- `RIGHT(expr, length)`
- `LENGTH(expr)`
- `LOWER(expr)`
- `UPPER(expr)`
- `LPAD(expr, length[, pad_value])`
- `RPAD(expr, length[, pad_value])`
- `INSTR(haystack, needle)`
- `SUBSTRING(expr, start[, length])`

### Date/time functions

- `EXTRACT("component" FROM column)` with components `year`, `iso_year`, `quarter`, `month`, `day`, `week`, `week_day`, `iso_week_day`, `hour`, `minute`, `second`
- `DATETRUNC("component", column)` with components `year`, `quarter`, `month`, `day`, `week`, `hour`, `minute`, `second`

### Generic functions

- `COALESCE(expr, expr)`

## Examples

### Automatic join usage

```
SELECT id, order.id, order.created, order.customer.email
FROM orderpositions
WHERE order.created > "2026-03-01"
```

### Subqueries

```
SELECT title
FROM products
WHERE EXISTS(SELECT 1 FROM orderpositions WHERE product = OUTER(id) AND order.status = "paid")
```

```
SELECT title
FROM products
WHERE id IN (SELECT product FROM orderpositions WHERE order.status = "paid")
```

```
SELECT id, (SELECT COUNT(*) FROM orders WHERE customer = OUTER(id)) AS order_cnt
FROM customers
```

### Aggregation

```
SELECT
    product,
    COUNT(DISTINCT id) FILTER (WHERE order.status = "paid") AS paid,
    COUNT(id) FILTER (WHERE order.status = "canceled") AS canceled,
    COUNT(id) AS all
FROM orderpositions
GROUP BY product
ORDER BY paid DESC
```

### JSON traversal

```
SELECT address->city->state AS state
FROM customers
WHERE name = "CA"
```