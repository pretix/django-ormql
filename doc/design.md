# Design

## Goals

For [pretix](https://github.com/pretix/pretix), we were looking for a way to solve two problems:

- We needed a language to describe customer-specific reports and exports in a way that allows them to be defined and edited by untrusted users at runtime. The complexity of the desired reports exceeds simple  column selection and filtering.
- Customers who use our platform as an API backend with a custom frontend should be able to request aggregate data, such as "how much money did we make last year" without having to download all raw data.

We looked a lot for prior art, however there is not much to look at.
Almost all platforms that go beyond the most simple reporting options offer some kind of SQL access.
This makes sense, to a degree, since SQL is the most widely known, powerful language to describe almost any kind of queries.

## Why not raw SQL?

Giving customers raw SQL access is problematic to us for two reasons:

### Security

We host the data of thousands of customers in the same PostgreSQL database.
Every customer, however, is only allowed to view their own data.
It's probably *possible* to somehow solve this with PostgreSQL's [row-level security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html) feature, but that would make handling the data more complex across our entire application.
Even then, we'd still need to be *really* sure no-one is able to sneak in SQL that causes unexpected effects or access.

### API compatibility

When we allow our customers to create reports based on SQL queries, the SQL layer becomes a public API.
For a public API, customers expect API compatibility.
This would basically mean freezing large parts of the data model of our application.
We, however, would like to retain the option to change parts of our data model without impacting customers.
You could probably solve that with database-level views, if you really wanted to.
However, to go one step further, we would also like to retain the option to change our database engine.
That we are using PostgreSQL and how our tables look like should be an implementation detail our customers do not need to care about.

## So what then?

We invented a DSL inspired by SQL, parse it using the customizable pure-Python [sqlglot](https://sqlglot.com/sqlglot.html) parse into an [abstract syntax tree](https://en.wikipedia.org/wiki/Abstract_syntax_tree) (AST).
Based on the AST, we rebuild a Django [QuerySet](https://docs.djangoproject.com/en/6.0/ref/models/querysets/) based on queries that are already restricted to the data the customer can access.
To translate between the AST and the Django models, we use a concept similar to [DRF serializers](https://www.django-rest-framework.org/api-guide/serializers/).
This in-between concept allows us to translate between the "API data model" and the "internal data model" in some ways to retain backwards compatibility when we make changes to the internal data model.

### Why is the DSL not SQL?

Even though Django's capabilities are quite strong these days, it's not possible to express every SQL query as a Django QuerySet.
As a prominent example, [common table expressions](https://www.postgresql.org/docs/current/queries-with.html) are not natively supported by Django, but there are many more subtle examples, like a QuerySet not being able to usefully query from multiple tables in one query or use joins not intended to be used by Django.

This actually plays in our hands by limiting the way in which one could trick around our data access restrictions.
And since we're not able to provide "full SQL" support anyways, we can just stop trying, give the language a different name, and simplify a few things for customers that make the language more ergonomic to use, such as outomated joins for foreign keys.