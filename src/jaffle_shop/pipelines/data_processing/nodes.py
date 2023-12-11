from __future__ import annotations

from typing import TYPE_CHECKING

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
        .aggregate(total_amount=payments.amount.sum())
    )

    final = (
        customers.left_join(customer_orders, "customer_id")
        .drop("customer_id_right")
        .left_join(customer_payments, "customer_id")[
            customers.customer_id,
            customers.first_name,
            customers.last_name,
            customer_orders.first_order,
            customer_orders.most_recent_order,
            customer_orders.number_of_orders,
            customer_payments.total_amount.name("customer_lifetime_value"),
        ]
    )
    return final
