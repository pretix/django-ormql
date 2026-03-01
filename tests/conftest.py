import os
from decimal import Decimal

import pytest
from freezegun import freeze_time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")

import django

django.setup()

from .testapp.factories import TenantFactory, CategoryFactory, TagFactory, ProductFactory, CustomerFactory, \
    OrderFactory, OrderPositionFactory
from .testapp.models import Order
from .testapp.ormql import engine_for_tenant


@pytest.fixture
@freeze_time("2024-12-14 03:13:14+01:00")
def dataset1():
    # Tenants
    t1 = TenantFactory.create()
    t2 = TenantFactory.create()

    # Categories
    t1_cat_books = CategoryFactory.create(tenant=t1, title="Books")
    t1_cat_dvds = CategoryFactory.create(tenant=t1, title="DVDs")
    t2_cat_shirts = CategoryFactory.create(tenant=t2, title="T-Shirts")

    # Tags
    t1_tag_fiction = TagFactory.create(tenant=t1, tag="Fiction")
    t1_tag_nonfiction = TagFactory.create(tenant=t1, tag="Nonfiction")
    t1_tag_paperback = TagFactory.create(tenant=t1, tag="Paperback")
    t2_tag_red = TagFactory.create(tenant=t2, tag="red")
    t2_tag_blue = TagFactory.create(tenant=t2, tag="blue")

    # Products
    t1_prod_lotr_book = ProductFactory.create(
        tenant=t1,
        title="Lord of the rings",
        category=t1_cat_books,
        tags=[t1_tag_fiction, t1_tag_paperback],
        price=Decimal("10.70"),
        tax_rate=Decimal("7.00"),
    )
    t1_prod_sql_book = ProductFactory.create(
        tenant=t1,
        title="SQL for Dummies",
        category=t1_cat_books,
        tags=[t1_tag_nonfiction],
        price=Decimal("21.40"),
        tax_rate=Decimal("7.00"),
    )
    t1_prod_lotr_movie = ProductFactory.create(
        tenant=t1,
        title="Lord of the rings DVD",
        category=t1_cat_dvds,
        tags=[t1_tag_fiction],
        price=Decimal("19.00"),
    )
    t2_prod_red_shirt = ProductFactory.create(
        tenant=t2,
        title="Red shirt",
        category=t2_cat_shirts,
        tags=[t2_tag_red],
        price=Decimal("19.00"),
    )
    t2_prod_blue_shirt = ProductFactory.create(
        tenant=t2,
        title="Blue shirt",
        category=t2_cat_shirts,
        tags=[t2_tag_blue],
        price=Decimal("19.00"),
    )

    # Customers
    t1_c_active = CustomerFactory.create(tenant=t1, name="CA")
    t1_c_inactive = CustomerFactory.create(tenant=t1, active=False, name="CB")
    t2_c = CustomerFactory.create(tenant=t2, name="CC")

    # Orders
    t1_o1 = OrderFactory.create(tenant=t1, customer=t1_c_active, status=Order.OrderStatus.PAID)
    OrderPositionFactory.create(
        order=t1_o1,
        quantity=1,
        product=t1_prod_sql_book,
        single_price=t1_prod_sql_book.price,
        tax_rate=t1_prod_sql_book.tax_rate,
    )
    OrderPositionFactory.create(
        order=t1_o1,
        quantity=3,
        product=t1_prod_lotr_movie,
        single_price=t1_prod_lotr_movie.price,
        tax_rate=t1_prod_lotr_movie.tax_rate,
    )
    t1_o2 = OrderFactory.create(tenant=t1, customer=t1_c_inactive, status=Order.OrderStatus.CANCELED)
    OrderPositionFactory.create(
        order=t1_o2,
        quantity=2,
        product=t1_prod_lotr_book,
        single_price=t1_prod_lotr_book.price,
        tax_rate=t1_prod_lotr_book.tax_rate,
    )
    OrderPositionFactory.create(
        order=t1_o2,
        quantity=1,
        product=t1_prod_lotr_movie,
        single_price=t1_prod_lotr_movie.price,
        tax_rate=t1_prod_lotr_movie.tax_rate,
    )
    t1_o3 = OrderFactory.create(tenant=t1, customer=None, status=Order.OrderStatus.CANCELED)
    OrderPositionFactory.create(
        order=t1_o3,
        quantity=1,
        product=t1_prod_lotr_book,
        single_price=t1_prod_lotr_book.price,
        tax_rate=t1_prod_lotr_book.tax_rate,
    )
    t2_o1 = OrderFactory.create(tenant=t2, customer=t2_c, status=Order.OrderStatus.PAID)
    OrderPositionFactory.create(
        order=t2_o1,
        quantity=1,
        product=t2_prod_blue_shirt,
        single_price=t2_prod_blue_shirt.price,
        tax_rate=t2_prod_blue_shirt.tax_rate,
    )
    return t1, t2


@pytest.fixture
def engine_t1(dataset1):
    t1, t2 = dataset1
    return engine_for_tenant(t1)
