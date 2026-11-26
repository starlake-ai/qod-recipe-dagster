"""Five Dagster assets in one DuckDB file: three raw tables loaded from the
CSVs in data/, and the two models built from them.

    dagster asset materialize --select '*' -f pipeline/definitions.py
    dagster dev -f pipeline/definitions.py      # to see the asset graph
"""

from pathlib import Path

from dagster import Definitions, asset
from dagster_duckdb import DuckDBResource

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def _load(duckdb: DuckDBResource, table: str, csv_options: str = "") -> None:
    path = (DATA / f"{table}.csv").as_posix()
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv('{path}'{csv_options})")


@asset
def customers(duckdb: DuckDBResource) -> None:
    _load(duckdb, "customers")


@asset
def orders(duckdb: DuckDBResource) -> None:
    _load(duckdb, "orders")


@asset
def order_items(duckdb: DuckDBResource) -> None:
    _load(duckdb, "order_items", ", types = {'unit_price': 'DECIMAL(10,2)'}")


ORDER_TOTALS = """
    SELECT o.order_id, o.customer_id, o.order_date,
           CAST(sum(i.quantity * i.unit_price) AS DECIMAL(12,2)) AS order_total
    FROM orders o JOIN order_items i USING (order_id)
    GROUP BY ALL
"""


@asset(deps=[customers, orders, order_items])
def customers_enriched(duckdb: DuckDBResource) -> None:
    """One row per customer, customers without orders included with 0."""
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE OR REPLACE TABLE customers_enriched AS
            SELECT c.customer_id, c.name, c.country,
                   count(t.order_id) AS order_count,
                   CAST(coalesce(sum(t.order_total), 0) AS DECIMAL(12,2)) AS lifetime_value
            FROM customers c LEFT JOIN ({ORDER_TOTALS}) t USING (customer_id)
            GROUP BY ALL
        """)


@asset(deps=[orders, order_items])
def daily_revenue(duckdb: DuckDBResource) -> None:
    """Revenue per day."""
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE OR REPLACE TABLE daily_revenue AS
            SELECT order_date, CAST(sum(order_total) AS DECIMAL(12,2)) AS revenue
            FROM ({ORDER_TOTALS})
            GROUP BY ALL
        """)


defs = Definitions(
    assets=[customers, orders, order_items, customers_enriched, daily_revenue],
    resources={"duckdb": DuckDBResource(database=(ROOT / "out" / "warehouse.duckdb").as_posix())},
)
