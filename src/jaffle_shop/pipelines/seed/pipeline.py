from kedro.pipeline import Pipeline, node, pipeline

from .nodes import identity


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=identity,
                inputs="seed_customers",
                outputs="raw_customers",
            ),
            node(
                func=identity,
                inputs="seed_orders",
                outputs="raw_orders",
            ),
            node(
                func=identity,
                inputs="seed_payments",
                outputs="raw_payments",
            ),
        ]
    )
