from kedro.pipeline import Pipeline, node, pipeline

from .nodes import rename_customers, rename_orders, rename_payments


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=rename_customers,
                inputs="raw_customers",
                outputs="stg_customers",
            ),
            node(
                func=rename_orders,
                inputs="raw_orders",
                outputs="stg_orders",
            ),
            node(
                func=rename_payments,
                inputs="raw_payments",
                outputs="stg_payments",
            ),
        ]
    )
