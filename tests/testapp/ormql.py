from django.db.models import Case, When, Value, F
from django.db.models.functions import Upper

from django_ormql.engine import QueryEngine
from django_ormql.tables import ModelTable
from django_ormql.columns import ForeignKeyColumn, GeneratedColumn, ModelColumn
from .models import Category, Tag, Product, Customer, Order, OrderPosition


class CategoryTable(ModelTable):
    class Meta:
        name = "categories"
        model = Category
        columns = [
            "id",
            "title",
        ]


class TagTable(ModelTable):
    class Meta:
        name = "tags"
        model = Tag
        columns = [
            "id",
            "tag",
        ]


class ProductTable(ModelTable):
    category = ForeignKeyColumn(CategoryTable)
    tag = ForeignKeyColumn(TagTable)

    class Meta:
        name = "products"
        model = Product
        columns = [
            "id",
            "category",
            "tag",
            "title",
            "price",
            "tax_rate",
            "publication_date",
        ]


class CustomerTable(ModelTable):
    enabled = ModelColumn(source="active")

    class Meta:
        name = "customers"
        model = Customer
        columns = [
            "id",
            "name",
            "email",
            "enabled",
            "address",
        ]


class OrderTable(ModelTable):
    customer = ForeignKeyColumn(CustomerTable)
    validity = GeneratedColumn(
        Case(
            When(status__in=("new", "paid"), then=Value("valid")),
            default=Value("invalid")
        )
    )
    email = GeneratedColumn(
        F("customer__email")
    )
    email_upper = GeneratedColumn(
        Upper(F("customer__email"))
    )
    static_value = GeneratedColumn(Value(2))

    class Meta:
        name = "orders"
        model = Order
        columns = [
            "id",
            "customer",
            "created",
            "status",
            "validity",
            "email",
            "email_upper",
            "static_value",
        ]


class OrderPositionTable(ModelTable):
    order = ForeignKeyColumn(OrderTable)
    product = ForeignKeyColumn(ProductTable)

    class Meta:
        name = "orderpositions"
        model = OrderPosition
        columns = [
            "id",
            "order",
            "product",
            "quantity",
            "single_price",
            "tax_rate",
        ]


def engine_for_tenant(tenant):
    engine = QueryEngine()
    engine.register_table(
        CategoryTable(
            base_qs=Category.objects.filter(tenant=tenant)
        )
    )
    engine.register_table(
        TagTable(
            base_qs=Tag.objects.filter(tenant=tenant)
        )
    )
    engine.register_table(
        ProductTable(
            base_qs=Product.objects.filter(tenant=tenant)
        )
    )
    engine.register_table(
        CustomerTable(
            base_qs=Customer.objects.filter(tenant=tenant)
        )
    )
    engine.register_table(
        OrderTable(
            base_qs=Order.objects.filter(tenant=tenant)
        )
    )
    engine.register_table(
        OrderPositionTable(
            base_qs=OrderPosition.objects.filter(order__tenant=tenant)
        )
    )
    return engine