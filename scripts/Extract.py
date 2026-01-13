import pandas as pd

# T0007: Implement demo script to read/write CSVs
def extract(input_path: str):
    """
    Extracts data from CSV or Excel and returns a DataFrame.
    Also converts Excel → CSV automatically.
    """

    # If Excel → convert to CSV first
    if input_path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(input_path)
        df.to_csv("customer.csv", index=False)
        print("Excel converted to customer.csv")
        input_path = "customer.csv"
    else:
        print("Reading CSV directly...")

    # Read the final CSV
    df = pd.read_csv(input_path)

    # ---------------- PIPELINE EXECUTION SUMMARY ----------------
    try:
        dataset_name = input_path.split("/")[-1]
        row_count = len(df)
        print(
            f"[PIPELINE SUMMARY] EXTRACT | "
            f"Dataset: {dataset_name} | Rows extracted: {row_count}"
        )
    except Exception as e:
        print(f"[PIPELINE SUMMARY] EXTRACT | Failed to capture summary: {e}")
    # ------------------------------------------------------------

    return df

extract
