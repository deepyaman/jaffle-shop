from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ibis.expr.types as ir


def rename_customers(source: ir.Table) -> ir.Table:
    renamed = source[
        source.id.name("customer_id"),
        "first_name",
        "last_name",
    ]
    return renamed


def rename_orders(source: ir.Table) -> ir.Table:
    renamed = source[
        source.id.name("order_id"),
        source.user_id.name("customer_id"),
        "order_date",
        "status",
    ]
    return renamed


def rename_payments(source: ir.Table) -> ir.Table:
    # `amount` is currently stored in cents, so we convert it to dollars
    amount_in_dollars = (source.amount / 100).name("amount")

    renamed = source[
        source.id.name("payment_id"),
        "order_id",
        "payment_method",
        amount_in_dollars,
    ]
    return renamed
