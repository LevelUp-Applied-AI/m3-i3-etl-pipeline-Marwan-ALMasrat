"""ETL Pipeline — Amman Digital Market Customer Analytics"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    tables = ["customers", "products", "orders", "order_items"]
    data = {}
    for table in tables:
        data[table] = pd.read_sql(f"SELECT * FROM {table}", engine)
        print(f"  Extracted {table}: {len(data[table])} rows")
    return data


def transform(data_dict):
    customers   = data_dict["customers"]
    products    = data_dict["products"]
    orders      = data_dict["orders"]
    order_items = data_dict["order_items"]

    df = order_items.merge(products[["product_id", "unit_price", "category"]], on="product_id")
    df = df[df["quantity"] <= 100]
    df["line_total"] = df["quantity"] * df["unit_price"]
    df = df.merge(orders[["order_id", "customer_id", "status"]], on="order_id")
    df = df[df["status"] != "cancelled"]

    cat_revenue = df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_cat = (
        cat_revenue.sort_values("line_total", ascending=False)
        .groupby("customer_id").first().reset_index()
        [["customer_id", "category"]].rename(columns={"category": "top_category"})
    )

    summary = (
        df.groupby("customer_id")
        .agg(total_orders=("order_id", "nunique"), total_revenue=("line_total", "sum"))
        .reset_index()
    )
    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]
    summary = summary.merge(
        customers[["customer_id", "customer_name", "city"]],
        on="customer_id"
    )
    summary = summary.merge(top_cat, on="customer_id")

    return summary[["customer_id", "customer_name", "city", "total_orders",
                     "total_revenue", "avg_order_value", "top_category"]]


def validate(df):
    checks = {
        "no_null_customer_id":    df["customer_id"].notna().all(),
        "no_null_customer_name":  df["customer_name"].notna().all(),
        "total_revenue_positive": (df["total_revenue"] > 0).all(),
        "no_duplicate_customers": df["customer_id"].nunique() == len(df),
        "total_orders_positive":  (df["total_orders"] > 0).all(),
    }
    for check, passed in checks.items():
        print(f"  [{'PASS' if passed else 'FAIL'}] {check}")
    if not all(checks.values()):
        raise ValueError("Data quality check failed!")
    return checks


def load(df, engine, csv_path):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)
    print(f"  Loaded {len(df)} rows to DB and {csv_path}")


def main():

    DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5433/amman_market")
    engine = create_engine(DB_URL)

    print("=== EXTRACT ===")
    data = extract(engine)

    print("\n=== TRANSFORM ===")
    df = transform(data)
    print(f"  Result: {len(df)} customer rows")

    print("\n=== VALIDATE ===")
    validate(df)

    print("\n=== LOAD ===")
    load(df, engine, "output/customer_analytics.csv")

    print("\nDone!")


if __name__ == "__main__":
    main()