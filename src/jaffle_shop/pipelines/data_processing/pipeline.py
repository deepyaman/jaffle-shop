from kedro.pipeline import Pipeline, node, pipeline

from .nodes import process_customers, process_orders


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=process_customers,
                inputs=["stg_customers", "stg_orders", "stg_payments"],
                outputs="customers",
            ),
            node(
                func=process_orders,
                inputs=["stg_orders", "stg_payments", "params:payment_methods"],
                outputs="orders",
            ),
        ]
    )
