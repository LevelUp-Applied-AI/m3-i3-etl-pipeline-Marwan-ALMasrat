import pandas as pd
import pytest
from etl_pipeline import transform, validate


def make_base_data(orders_df, items_df):
    customers = pd.DataFrame({
        "customer_id": [1, 2],
        "customer_name": ["Alice", "Bob"],
        "city": ["Amman", "Irbid"]
    })
    products = pd.DataFrame({
        "product_id": [10],
        "unit_price": [50.0],
        "category": ["Electronics"]
    })
    return {"customers": customers, "products": products,
            "orders": orders_df, "order_items": items_df}


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    orders = pd.DataFrame({
        "order_id": [1, 2], "customer_id": [1, 1],
        "status": ["completed", "cancelled"]
    })
    items = pd.DataFrame({
        "item_id": [1, 2], "order_id": [1, 2],
        "product_id": [10, 10], "quantity": [2, 3]
    })
    result = transform(make_base_data(orders, items))
    assert result.iloc[0]["total_orders"] == 1


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    orders = pd.DataFrame({
        "order_id": [1, 2], "customer_id": [1, 1],
        "status": ["completed", "completed"]
    })
    items = pd.DataFrame({
        "item_id": [1, 2], "order_id": [1, 2],
        "product_id": [10, 10], "quantity": [2, 150]
    })
    result = transform(make_base_data(orders, items))
    assert result.iloc[0]["total_orders"] == 1


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    bad_df = pd.DataFrame({
        "customer_id": [None, 2],
        "customer_name": ["Alice", "Bob"],
        "city": ["Amman", "Irbid"],
        "total_orders": [3, 2],
        "total_revenue": [500.0, 300.0],
        "avg_order_value": [166.7, 150.0],
        "top_category": ["Electronics", "Clothing"]
    })
    with pytest.raises(ValueError):
        validate(bad_df)