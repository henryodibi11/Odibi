from typing import List, Optional

from pydantic import BaseModel, Field


class SCD2Params(BaseModel):
    """
    Parameters for SCD Type 2 (Slowly Changing Dimensions) transformer.

    ### 🕰️ The "Time Machine" Pattern

    **Business Problem:**
    "I need to know what the customer's address was *last month*, not just where they live now."

    **The Solution:**
    SCD Type 2 tracks the full history of changes. Each record has an "effective window" (start/end dates) and a flag indicating if it is the current version.

    **Recipe:**
    ```yaml
    transformer: "scd2"
    params:
      # The "Time Machine" Configuration
      keys: ["customer_id"]            # How we identify the entity
      track_cols: ["address", "tier"]  # What changes we care about
      effective_time_col: "txn_date"   # When the change actually happened
      end_time_col: "valid_to"         # (Optional) Name of closing timestamp
      current_flag_col: "is_active"    # (Optional) Name of current flag
    ```

    **How it works:**
    1. **Match**: Finds existing records using `keys`.
    2. **Compare**: Checks `track_cols` to see if data changed.
    3. **Close**: If changed, updates the old record's `end_time_col` to the new `effective_time_col`.
    4. **Insert**: Adds a new record with `effective_time_col` as start and open-ended end date.

    ### 🔎 Visual Example

    **Input (Source Update):**
    *Customer 101 moved to NY on Feb 1st.*

    | customer_id | address | tier | txn_date   |
    |-------------|---------|------|------------|
    | 101         | NY      | Gold | 2024-02-01 |

    **Target Table (Before):**
    *Customer 101 lived in CA since Jan 1st.*

    | customer_id | address | tier | txn_date   | valid_to | is_active |
    |-------------|---------|------|------------|----------|-----------|
    | 101         | CA      | Gold | 2024-01-01 | NULL     | true      |

    **Target Table (After SCD2):**
    *Old record CLOSED (valid_to set). New record OPEN (is_active=true).*

    | customer_id | address | tier | txn_date   | valid_to   | is_active |
    |-------------|---------|------|------------|------------|-----------|
    | 101         | CA      | Gold | 2024-01-01 | 2024-02-01 | false     |
    | 101         | NY      | Gold | 2024-02-01 | NULL       | true      |

    **Matching YAML Configuration:**
    ```yaml
    transformer: "scd2"
    params:
      keys: ["customer_id"]
      track_cols: ["address", "tier"]
      effective_time_col: "txn_date"
      end_time_col: "valid_to"
      current_flag_col: "is_active"
    ```

    **⚡ Automation Note:**
    You do **NOT** need to calculate `valid_to` or `is_active` in your input.
    The transformer automatically manages these columns based on changes in your `track_cols`.
    """

    keys: List[str] = Field(description="Natural keys to identify unique entities")
    track_cols: List[str] = Field(description="Columns to monitor for changes")
    effective_time_col: Optional[str] = Field(
        default=None,
        description="Source column indicating when the change occurred. If omitted, defaults to Current Timestamp (Processing Time).",
    )
    end_time_col: str = Field(default="end_date", description="Name of the end timestamp column")
    current_flag_col: str = Field(
        default="is_current", description="Name of the current record flag column"
    )
    delete_col: Optional[str] = Field(
        default=None, description="Column indicating soft deletion (boolean)"
    )
