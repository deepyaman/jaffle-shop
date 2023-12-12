from __future__ import annotations

from typing import TYPE_CHECKING

from ibis import _

if TYPE_CHECKING:
    import ibis.expr.types as ir


def process_customers(
    customers: ir.Table, orders: ir.Table, payments: ir.Table
) -> ir.Table:
    customer_orders = orders.group_by("customer_id").aggregate(
        first_order=orders.order_date.min(),
        most_recent_order=orders.order_date.max(),
        number_of_orders=orders.order_id.count(),
    )

    customer_payments = (
        payments.left_join(orders, "order_id")
        .group_by(orders.customer_id)
        .aggregate(total_amount=_.amount.sum())
    )

    final = (
        customers.left_join(customer_orders, "customer_id")
        .drop("customer_id_right")
        .left_join(customer_payments, "customer_id")[
            "customer_id",
            "first_name",
            "last_name",
            "first_order",
            "most_recent_order",
            "number_of_orders",
            _.total_amount.name("customer_lifetime_value"),
        ]
    )
    return final
