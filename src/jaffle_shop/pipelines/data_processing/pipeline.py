from kedro.pipeline import Pipeline, node, pipeline

from .nodes import process_customers


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=process_customers,
                inputs=["stg_customers", "stg_orders", "stg_payments"],
                outputs="customers",
            )
        ]
    )
