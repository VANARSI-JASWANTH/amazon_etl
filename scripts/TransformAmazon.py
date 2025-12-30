import pandas as pd
import numpy as np

def transform_amazon(df: pd.DataFrame):
    """
    Applies all cleaning operations on the Amazon DataFrame.
    Handles mixed types, missing values, and data quality issues specific to Amazon dataset.
    """

    print(f"Starting transformation on {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")

    # ========================================
    # T0008: Build reusable cleaning utilities
    # ========================================
    # Convert empty/whitespace to NaN
    df = df.replace(r'^\s*$', np.nan, regex=True)
    print(f"After empty string conversion: {len(df)} rows")

    # Trim spaces in all string columns
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
    print(f"String columns trimmed")

    # Handle name column (fillna + trim)
    df["CustomerName"] = df["CustomerName"].fillna("Unknown")
    df["CustomerName"] = df["CustomerName"].str.strip()
    print(f"Names cleaned: {df['CustomerName'].isna().sum()} missing")

    # ========================================
    # T0009: Handle incorrect data types
    # ========================================
    # Age: Remove quoted strings like "26", convert to numeric
    df["Age"] = df["Age"].astype(str).str.replace('"', '', regex=False)
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    print(f"Age column converted to numeric")

    # Phone: Standardize format (keep only 10-digit numbers)
    def standardize_phone(phone_val):
        if pd.isna(phone_val) or phone_val == "":
            return np.nan
        phone_str = str(phone_val)
        digits_only = ''.join(filter(str.isdigit, phone_str))
        return digits_only if len(digits_only) == 10 else np.nan
    
    df["Phone"] = df["Phone"].apply(standardize_phone)
    print(f"Phone column standardized. Missing: {df['Phone'].isna().sum()}")

    # ========================================
    # T0010: Duplicate data detection & removal
    # ========================================
    df = df.drop_duplicates(subset="OrderID", keep="first")
    print(f"After duplicate removal: {len(df)} rows")

    # Email validation (before dropping missing values)
    valid_email_mask = (
        (df["Email"].isna()) | 
        (df["Email"].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', regex=True, na=False))
    )
    invalid_emails = df[~valid_email_mask]["Email"].unique()
    print(f"Invalid emails found: {len(invalid_emails)} - {list(invalid_emails)[:5]}")
    df = df[valid_email_mask]
    print(f"After email validation: {len(df)} rows")

    # ========================================
    # T0011: Missing data handling strategies
    # ========================================
    # Fill missing age with mean
    age_mean = df["Age"].mean()
    df["Age"] = df["Age"].fillna(age_mean).astype(int)
    print(f"Age filled with mean: {age_mean:.1f}, Missing after: {df['Age'].isna().sum()}")

    # Drop rows with missing critical values
    critical_cols = ["OrderID", "CustomerID", "OrderDate", "TotalAmount", "Email", "Phone", "Age"]
    df = df.dropna(subset=critical_cols)
    print(f"After dropping rows with missing critical values: {len(df)} rows")

    # ========================================
    # T0016: Date/time transformations
    # ========================================
    # Parse OrderDate to datetime
    df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")
    df = df.dropna(subset=["OrderDate"])
    df = df.sort_values(by="OrderDate", ascending=False)
    print(f"OrderDate parsed and sorted: {len(df)} rows")

    # Extract date features (year, month, day_of_week)
    df["order_year"] = df["OrderDate"].dt.year
    df["order_month"] = df["OrderDate"].dt.month
    df["order_day_of_week"] = df["OrderDate"].dt.day_name()
    print(f"Date features engineered (year, month, day_of_week)")

    # ========================================
    # T0015: Feature engineering logic
    # ========================================
    # Order amount categories
    df["order_amount_category"] = pd.cut(
        df["TotalAmount"],
        bins=[0, 500, 1000, 1500, 2000, np.inf],
        labels=["Small", "Medium", "Large", "Very Large", "Premium"]
    )
    print(f"Order amount categories created")

    # Customer age groups
    df["age_group"] = pd.cut(
        df["Age"],
        bins=[0, 25, 35, 45, 55, 100],
        labels=["18-25", "26-35", "36-45", "46-55", "55+"]
    )
    print(f"Age groups created")

    # ========================================
    # T0014: Normalization & scaling
    # ========================================
    # Min-Max normalization for TotalAmount
    min_amount = df["TotalAmount"].min()
    max_amount = df["TotalAmount"].max()
    df["total_amount_normalized"] = (df["TotalAmount"] - min_amount) / (max_amount - min_amount)
    print(f"Total amount normalized (min: {min_amount:.2f}, max: {max_amount:.2f})")

    # ========================================
    # T0013: Aggregations (groupBy, sum, min, max)
    # ========================================
    # Global order statistics
    total_orders = len(df)
    max_quantity = df["Quantity"].max()
    min_quantity = df["Quantity"].min()
    total_quantity = df["Quantity"].sum()
    
    orders_summary_df = pd.DataFrame([{
        "total_orders": total_orders,
        "max_quantity": max_quantity,
        "min_quantity": min_quantity,
        "total_quantity": total_quantity
    }])
    
    orders_summary_df.to_csv("data/processed/orders_summary.csv", index=False)
    orders_summary_df.to_excel("data/processed/orders_summary.xlsx", index=False)
    print(f"Order stats: total={total_orders}, max_qty={max_quantity}, min_qty={min_quantity}, total_qty={total_quantity}")
    
    # State-wise aggregations (groupBy)
    state_summary_df = (
        df.groupby("State").agg(
            total_orders=("OrderID", "count"),
            max_quantity=("Quantity", "max"),
            min_quantity=("Quantity", "min"),
            total_quantity=("Quantity", "sum")
        ).reset_index()
    )
    
    state_summary_df.to_csv("data/processed/orders_summary.csv", mode="a", index=False)
    
    with pd.ExcelWriter("data/processed/orders_summary.xlsx", engine="openpyxl") as writer:
        orders_summary_df.to_excel(writer, sheet_name="global_summary", index=False)
        state_summary_df.to_excel(writer, sheet_name="state_summary", index=False)
    
    print(f"✓ Orders summary saved: orders_summary.csv & orders_summary.xlsx")

    # ========================================
    # T0016: Date/time transformations (final)
    # ========================================
    # Convert datetime to string format for CSV output
    df["OrderDate"] = df["OrderDate"].dt.strftime('%Y-%m-%d')

    print(f"\nTransformation complete!")
    print(f"Final shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    return df
