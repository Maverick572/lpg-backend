import sqlite3
import pandas as pd

############################
# config
############################

DB_FILE = "lpg_simulation.db"

############################
# connect
############################

conn = sqlite3.connect(DB_FILE)

############################
# load table
############################

df = pd.read_sql(

    "SELECT * FROM district_demand",
    conn

)

conn.close()

############################
# basic preview
############################

print("\n================ SAMPLE ROWS ================\n")

print(df.head(10))

############################
# daily national demand
############################

daily_total = (

    df
    .groupby("day")["demand"]
    .sum()
    .reset_index()

)

print("\n================ DAILY TOTAL DEMAND ================\n")

print(daily_total.head(15))

print("\nAverage daily national demand:")

print(int(daily_total["demand"].mean()))

############################
# top districts
############################

district_avg = (

    df
    .groupby(["state","district"])["demand"]
    .mean()
    .reset_index()

)

district_avg = district_avg.sort_values(

    "demand",
    ascending=False

)

print("\n================ TOP 10 DISTRICTS ================\n")

print(district_avg.head(10))

############################
# variability check
############################

district_std = (

    df
    .groupby("district")["demand"]
    .std()
    .reset_index()

)

print("\n================ DEMAND VARIABILITY ================\n")

print(district_std.head(10))

############################
# national statistics
############################

print("\n================ NATIONAL STATS ================\n")

print("Min daily demand:",
      int(daily_total["demand"].min()))

print("Max daily demand:",
      int(daily_total["demand"].max()))

print("Mean daily demand:",
      int(daily_total["demand"].mean()))

print("Std deviation:",
      int(daily_total["demand"].std()))

############################
# example district query
############################

example = df[df["district"] == df.iloc[0]["district"]]

print("\n================ EXAMPLE DISTRICT TREND ================\n")

print(example.head(15))

print("\nDone")