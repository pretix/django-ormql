from django_ormql.engine import QueryEngine
from django_ormql.tables import ModelTable
from django_ormql.columns import ForeignKeyColumn
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
    class Meta:
        name = "customers"
        model = Customer
        columns = [
            "id",
            "name",
            "email",
            "active",
            "address",
        ]


class OrderTable(ModelTable):
    customer = ForeignKeyColumn(CustomerTable)

    class Meta:
        name = "orders"
        model = Order
        columns = [
            "id",
            "customer",
            "created",
            "status",
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