import pandas as pd

EXPECTED_FILES = {
    "dailyActivity_merged.csv":[
        "Id", "ActivityDate", "TotalSteps", "Calories"
    ],
    "sleepDay_merged.csv":[
        "Id", "SleepDay", "TotalMinutesAsleep"
    ]
}

RAW_PATH = "data/raw/fitbit"

def validate_files():
    for file, columns in EXPECTED_FILES.items():
        df = pd.read_csv(f"{RAW_PATH}/{file}")
        missing = set(columns) - set(df.columns)
        
        if missing:
            raise ValueError(f"{file} missing columns: {missing}")
        
        else:
            print(f"{file} validated")

if __name__ == "__main__":
    validate_files()