import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_parquet("data/features/master_user_day.parquet")
print(df.head())
print(df.info())
print(df.describe())

# checking missing values
missing = df.isnull().sum()
missing_percent = (missing/len(df)) * 100
print(pd.concat([missing, missing_percent], axis=1, keys=["missing_count", "missing_percent"]))

# distribution of key numeric columns

numeric_cols = ["totalsteps", "totaldistance", "veryactiveminutes", "fairlyactiveminutes", "lightlyactiveminutes", "sedentaryminutes", "calories", "totalminutesasleep"]

"""
df[numeric_cols].hist(histtype = 'bar', align = 'mid', bottom = 100,  bins=30, layout= (4,3), figsize=(15,10))
plt.show()
"""
# check correlation
"""
plt.figure(figsize=(12,8))
sns.heatmap(df[numeric_cols].corr(), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap")
plt.show()

"""

# daily patterns (time series)

df['date'] = pd.to_datetime(df['date'])
daily_steps = df.groupby('date')['totalsteps'].mean()