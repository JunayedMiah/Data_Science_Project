import pandas as pd
import os

RAW_PATH = "data/raw/fitbit"
PROCESSED_PATH = "data/processed"

os.makedirs(PROCESSED_PATH, exist_ok=True)

def standardize_datetime_columns(df):
    for col in df.columns:
        col_lower = col.lower()
        
        if "date" in col_lower or "time" in col_lower:
            if pd.api.types.is_numeric_dtype(df[col]):
                max_val = df[col].dropna().max()
                if max_val > 1e11:
                    df[col] = pd.to_datetime(df[col], unit="ms" , errors="coerce")
                else:
                    df[col] = pd.to_datetime(df[col], unit="s", errors="coerce")
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format = True)
    return df

def clean_dataframe(df):
    """ 
    Apply generic cleaning rules that work for all fitbit CSV files
    """
    df = df.copy()
    df = df.drop_duplicates()
    
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    
    df = standardize_datetime_columns(df)
    
    return df

def clean_all_csv_files():
    """ 
    Clean all csv files in the raw Fitbit folder and save as Parquet
    """
    
    for file_name in os.listdir(RAW_PATH):
        if not file_name.endswith(".csv"):
            continue
        
        raw_file_path = os.path.join(RAW_PATH, file_name)
        print(f"Cleaning {file_name}.....")
        
        df = pd.read_csv(raw_file_path)
        df_cleaned = clean_dataframe(df)
        
        parquet_name = file_name.replace(".csv", ".parquet")
        processed_file_path = os.path.join(PROCESSED_PATH, parquet_name)
        
        df_cleaned.to_parquet(processed_file_path, index=False)
        
        print(f"saved cleaned file: {parquet_name}")
        
if __name__ == "__main__":
    clean_all_csv_files()