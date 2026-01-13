import os
import json
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# TODO: FUTURE ENHANCEMENT - Option 2 (Smart Incremental Load)
# Implement date-based incremental loading with metadata tracking:
# 1. Create etl_metadata table to store last_load_date per table
# 2. Query metadata before load to get last successful load timestamp
# 3. Filter DataFrame based on last_order_date > last_load_date
# 4. Update metadata table after successful load
# 5. Handle first run (no metadata) as full load
# Benefits: Only processes new/updated records, more efficient
# Estimated: ~30-35 lines across Extract/Transform/Load files


# ========================================
# T0022: Reject logging helpers
# ========================================
def _create_reject_table(engine):
    """Create rejected_records table if missing (T0022)."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS rejected_records (
                    id SERIAL PRIMARY KEY,
                    rejected_at TIMESTAMP DEFAULT NOW(),
                    error_message TEXT,
                    row_data JSONB
                )
            """))
            conn.commit()
    except Exception as e:
        print(f"[LOAD] Could not create rejected_records table: {e}")


def _insert_reject_record(engine, row_data: dict, error_msg: str):
    """Insert a rejected row with error context (T0022)."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO rejected_records (error_message, row_data)
                VALUES (:error_msg, :row_data)
            """), {"error_msg": error_msg, "row_data": json.dumps(row_data)})
            conn.commit()
    except Exception as e:
        print(f"[LOAD] Could not insert reject record: {e}")


def _upsert_records(engine, df: pd.DataFrame, table_name: str, key_col: str, chunk_size: int):
    """T0021: Upsert logic with constraint handling (T0020 inside)."""
    rejected_rows = []
    try:
        with engine.connect() as conn:
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]
                try:
                    chunk.to_sql(
                        table_name,
                        conn,
                        if_exists="append",
                        index=False,
                        method="multi"
                    )
                except IntegrityError as e:
                    print(f"[LOAD] Constraint violation in chunk {i//chunk_size}: {e}")
                    for idx, row in chunk.iterrows():
                        try:
                            pd.DataFrame([row]).to_sql(
                                table_name,
                                conn,
                                if_exists="append",
                                index=False
                            )
                        except IntegrityError as row_err:
                            rejected_rows.append((row.to_dict(), str(row_err)))
                            _insert_reject_record(engine, row.to_dict(), str(row_err))
                except Exception as e:
                    print(f"[LOAD] Error in chunk {i//chunk_size}: {e}")
            conn.commit()
    except Exception as e:
        print(f"[LOAD] Upsert failed: {e}")
    
    return rejected_rows


# ========================================
# T0007 & T0018-T0022: Load entrypoint
# ========================================
def load(
    df: pd.DataFrame,
    csv_path: str = "cleaned_data.csv",
    xlsx_path: str = "cleaned_data.xlsx",
    load_type: str = "full",
    bulk_chunk_size: int = 1000,
    upsert_key: str = None,
    reject_csv_path: str = "data/processed/rejected_records.csv"
):
    """
    Save cleaned data to CSV, Excel, and PostgreSQL table.
    """

    # >>> PIPELINE SUMMARY (ADDED)
    total_rows_to_load = len(df)
    load_start_time = datetime.now()
    # <<< PIPELINE SUMMARY (ADDED)

    # >>> REJECT RECOVERY (ADDED)
    rejected_rows_bulk = []
    # <<< REJECT RECOVERY (ADDED)

    # ======== T0007: Local file outputs ========
    try:
        df.to_csv(csv_path, index=False)
        print(f"[LOAD] ✓ Saved CSV: {csv_path}")
    except PermissionError as e:
        print(f"[LOAD] ✗ Permission denied for CSV: {csv_path}")
        print(f"[LOAD]   Error: {e}")
    except Exception as e:
        print(f"[LOAD] ✗ Failed to save CSV: {e}")

    try:
        output_dir = os.path.dirname(xlsx_path)
        if output_dir and not os.access(output_dir, os.W_OK):
            print(f"[LOAD] ⚠ Directory not writable: {output_dir}")
            print(f"[LOAD]   Skipping Excel file creation")
        else:
            df.to_excel(xlsx_path, index=False)
            print(f"[LOAD] ✓ Saved Excel: {xlsx_path}")
    except PermissionError as e:
        print(f"[LOAD] ⚠ Permission denied for Excel: {xlsx_path}")
        print(f"[LOAD]   Continuing without Excel file (CSV saved successfully)")
    except ImportError as e:
        print(f"[LOAD] ⚠ Excel writer not available: {e}")
        print(f"[LOAD]   Skipping Excel file (openpyxl may not be installed)")
    except Exception as e:
        print(f"[LOAD] ⚠ Failed to save Excel (non-critical): {e}")
        print(f"[LOAD]   Continuing with CSV only")

    # ======== T0018-T0022: Database load pipeline ========
    try:
        engine = create_engine(
            "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        )

        _create_reject_table(engine)

        if load_type == "incremental":
            if_exists_mode = "append"
            print(f"[LOAD] T0019: Incremental load: Appending {len(df)} rows")
        else:
            if_exists_mode = "replace"
            print(f"[LOAD] T0019: Full load: Replacing table with {len(df)} rows")

        if upsert_key:
            print(f"[LOAD] T0021: Upsert logic enabled (key: {upsert_key})")
            rejected = _upsert_records(engine, df, "customers_cleaned", upsert_key, bulk_chunk_size)
            if rejected:
                reject_df = pd.DataFrame([r[0] for r in rejected])
                reject_df.to_csv(reject_csv_path, index=False)
                print(f"[LOAD] T0022: {len(rejected)} rejected records saved to {reject_csv_path}")
        else:
            try:
                df.to_sql(
                    "customers_cleaned",
                    engine,
                    if_exists=if_exists_mode,
                    index=False,
                    chunksize=bulk_chunk_size
                )
                print(f"[LOAD] ✓ T0018: Bulk load written to PostgreSQL (chunk_size: {bulk_chunk_size})")
            except IntegrityError as e:
                rejected_rows_bulk = df.to_dict(orient="records")
                print(f"[LOAD] Bulk load failed, rows captured for rejection: {e}")

        print(f"[LOAD] ✓ Written to PostgreSQL table: customers_cleaned (mode: {load_type})")

        # >>> PIPELINE SUMMARY (ADDED)
        load_end_time = datetime.now()
        print(
            f"[PIPELINE SUMMARY] LOAD | "
            f"Table: customers_cleaned | "
            f"Rows loaded: {total_rows_to_load} | "
            f"Mode: {load_type} | "
            f"Duration: {(load_end_time - load_start_time).seconds}s | "
            f"Status: SUCCESS"
        )
        # <<< PIPELINE SUMMARY (ADDED)

    except ImportError as e:
        print(f"[LOAD] ⚠ PostgreSQL driver not available: {e}")
        print(f"[LOAD]   Database load skipped (psycopg2 may not be installed)")
    except Exception as e:
        print(f"[LOAD] ⚠ Database load failed (non-critical): {e}")
        print(f"[LOAD]   File outputs may still be available")

    # >>> REJECT RECOVERY (ADDED)
    if rejected_rows_bulk:
        pd.DataFrame(rejected_rows_bulk).to_csv(reject_csv_path, index=False)
        print(f"[LOAD] ✓ Rejected records saved to CSV: {reject_csv_path}")
    # <<< REJECT RECOVERY (ADDED)
