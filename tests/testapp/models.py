import uuid

from django.db import models


class Tenant(models.Model):
    domain = models.CharField(max_length=250)


class Category(models.Model):
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)
    title = models.CharField(max_length=250)


class Tag(models.Model):
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)
    tag = models.CharField(max_length=250)


class Product(models.Model):
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    tags = models.ManyToManyField(Tag)
    title = models.CharField(max_length=250)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    tax_rate = models.DecimalField(max_digits=10, decimal_places=2)
    publication_date = models.DateField()


class Customer(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)
    name = models.CharField(max_length=250)
    email = models.EmailField()
    active = models.BooleanField(default=False)
    password_hash = models.CharField(max_length=250)
    address = models.JSONField(default=dict)


class Order(models.Model):
    class OrderStatus(models.TextChoices):
        NEW = "new"
        PAID = "paid"
        CANCELED = "canceled"

    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, null=True)
    created = models.DateTimeField(auto_now_add=True)
    status = models.CharField(
        choices=OrderStatus.choices, max_length=250, default=OrderStatus.NEW
    )


class OrderPosition(models.Model):
    order = models.ForeignKey(Order, on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    quantity = models.IntegerField()
    single_price = models.DecimalField(max_digits=10, decimal_places=2)
    tax_rate = models.DecimalField(max_digits=10, decimal_places=2)
