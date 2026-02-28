import datetime
import hashlib
from decimal import Decimal

import factory
from django.utils.crypto import get_random_string
from factory.django import DjangoModelFactory

from .models import Tenant, Category, Tag, Product, Customer, Order, OrderPosition


class TenantFactory(DjangoModelFactory):
    domain = factory.Sequence(lambda n: "tenant%d.example.com" % n)

    class Meta:
        model = Tenant


class CategoryFactory(DjangoModelFactory):
    title = factory.Sequence(lambda n: "Category %d" % n)

    class Meta:
        model = Category


class TagFactory(DjangoModelFactory):
    tag = factory.Sequence(lambda n: "Category %d" % n)

    class Meta:
        model = Tag


class ProductFactory(DjangoModelFactory):
    title = factory.Sequence(lambda n: "Product %d" % n)
    price = Decimal("119.00")
    tax_rate = Decimal("19.00")
    publication_date = datetime.date(2026, 2, 20)
    category = factory.SubFactory(CategoryFactory)

    @factory.post_generation
    def tags(self, create, extracted, **kwargs):
        if not create or not extracted:
            return
        self.tags.add(*extracted)

    class Meta:
        model = Product


class CustomerFactory(DjangoModelFactory):
    name = factory.Sequence(lambda n: "Customer %d" % n)
    email = factory.Sequence(lambda n: "customer%d@example.com" % n)
    active = True
    password_hash = factory.LazyFunction(lambda: hashlib.sha256(get_random_string(16).encode()).hexdigest())

    class Meta:
        model = Customer


class OrderFactory(DjangoModelFactory):
    customer = factory.SubFactory(CustomerFactory)

    class Meta:
        model = Order


class OrderPositionFactory(DjangoModelFactory):
    quantity = 1
    single_price = Decimal("119.00")
    tax_rate = Decimal("19.00")

    class Meta:
        model = OrderPosition
