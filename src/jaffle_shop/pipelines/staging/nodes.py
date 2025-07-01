import ibis


def rename_customers(source: ibis.Table) -> ibis.Table:
    renamed = source[
        source.id.name("customer_id"),
        "first_name",
        "last_name",
    ]
    return renamed


def rename_orders(source: ibis.Table) -> ibis.Table:
    renamed = source[
        source.id.name("order_id"),
        source.user_id.name("customer_id"),
        "order_date",
        "status",
    ]
    return renamed


def rename_payments(source: ibis.Table) -> ibis.Table:
    # `amount` is currently stored in cents, so we convert it to dollars
    amount_in_dollars = (source.amount / 100).name("amount")

    renamed = source[
        source.id.name("payment_id"),
        "order_id",
        "payment_method",
        amount_in_dollars,
    ]
    return renamed
