import os
import pandas as pd

PROCESSED_DATA_DIR = "data/processed"
FEATURES_DATA_DIR = "data/features"
os.makedirs(FEATURES_DATA_DIR, exist_ok=True)

def build_master_table():
    # Read cleaned files
    activity = pd.read_parquet(os.path.join(PROCESSED_DATA_DIR, "dailyactivity_merged.parquet"))
    sleep = pd.read_parquet(os.path.join(PROCESSED_DATA_DIR, "sleepday_merged.parquet"))

    # Rename for consistency
    activity = activity.rename(columns={"id": "user_id", "activitydate": "date"})
    sleep = sleep.rename(columns={"id": "user_id", "sleepday": "date"})

    # Ensure datetime64
    activity["date"] = pd.to_datetime(activity["date"], errors="coerce", infer_datetime_format=True)
    sleep["date"] = pd.to_datetime(sleep["date"], errors="coerce", infer_datetime_format=True)

    # Aggregate numeric columns only
    numeric_cols = sleep.select_dtypes(include="number").columns.tolist()
    sleep_agg = sleep.groupby(["user_id", "date"], as_index=False)[numeric_cols].sum()

    # Merge
    master_df = activity.merge(sleep_agg, on=["user_id", "date"], how="left")

    # Convert to date if you want
    master_df["date"] = master_df["date"].dt.date

    output_path = os.path.join(FEATURES_DATA_DIR, "master_user_day.parquet")
    master_df.to_parquet(output_path, index=False)
    print("Master user-day table created successfully")

if __name__ == "__main__":
    build_master_table()
