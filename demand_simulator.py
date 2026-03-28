import pandas as pd
import numpy as np
import sqlite3

############################################
# CONFIG
############################################

INPUT_FILE = "district_daily_demand_baseline.csv"
DB_FILE = "lpg_simulation.db"

SIM_DAYS = 60
RANDOM_SEED = 42

############################################
# BEHAVIOUR PARAMETERS
############################################

MONTH_FACTOR = {
    1: 1.05,
    2: 1.03,
    3: 1.00,
    4: 0.98,
    5: 0.97,
    6: 0.96,
    7: 0.98,
    8: 1.00,
    9: 1.02,
    10: 1.04,
    11: 1.06,
    12: 1.08
}

WEEKDAY_FACTOR = {
    0: 1.05,
    1: 1.02,
    2: 1.00,
    3: 1.00,
    4: 1.01,
    5: 0.97,
    6: 0.95
}

############################################
# LOAD BASELINE DATA
############################################

df = pd.read_csv(INPUT_FILE)

df.columns = df.columns.str.strip().str.lower()

required_cols = ["state", "district", "daily_cylinders"]

for col in required_cols:
    if col not in df.columns:
        raise Exception(f"Missing column: {col}")

############################################
# CLEAN DATA (prevents NaN overflow)
############################################

df["daily_cylinders"] = pd.to_numeric(
    df["daily_cylinders"],
    errors="coerce"
)

df = df.dropna(subset=["daily_cylinders"])

df = df[df["daily_cylinders"] > 0]

df.reset_index(drop=True, inplace=True)

############################################
# SANITY CHECK
############################################

national_daily = df["daily_cylinders"].sum()

max_district = df["daily_cylinders"].max()

print("\n--- BASELINE CHECK ---")

print("national daily demand:", int(national_daily))

print("max district demand:", int(max_district))

if national_daily < 3_000_000 or national_daily > 8_000_000:
    raise Exception(
        "Demand scale unrealistic — check consumption conversion"
    )

############################################
# RANDOM GENERATOR
############################################

rng = np.random.default_rng(RANDOM_SEED)

############################################
# SIMULATION
############################################

rows = []

for day in range(SIM_DAYS):

    month = (day // 30) + 1

    weekday = day % 7

    seasonal_multiplier = MONTH_FACTOR.get(month, 1.0)

    weekday_multiplier = WEEKDAY_FACTOR.get(weekday, 1.0)

    adjusted_mean = (
        df["daily_cylinders"]
        * seasonal_multiplier
        * weekday_multiplier
    )

    ########################################
    # VARIABILITY MODEL
    ########################################

    std_dev = np.sqrt(adjusted_mean) * 1.4

    simulated_values = rng.normal(
        loc=adjusted_mean,
        scale=std_dev
    )

    ########################################
    # FIX NaN / inf BEFORE casting
    ########################################

    simulated_values = np.nan_to_num(
        simulated_values,
        nan=0,
        posinf=0,
        neginf=0
    )

    simulated_values = np.clip(
        simulated_values,
        a_min=0,
        a_max=None
    )

    simulated_values = simulated_values.round().astype(int)

    ########################################
    # BUILD DAY DATAFRAME
    ########################################

    day_df = pd.DataFrame({

        "day": day + 1,
        "state": df["state"],
        "district": df["district"],
        "demand": simulated_values,
        "baseline": df["daily_cylinders"],
        "season_factor": seasonal_multiplier,
        "weekday_factor": weekday_multiplier

    })

    rows.append(day_df)

############################################
# COMBINE ALL DAYS
############################################

sim_df = pd.concat(rows, ignore_index=True)

############################################
# SAVE TO SQLITE
############################################

conn = sqlite3.connect(DB_FILE)

sim_df.to_sql(
    "district_demand",
    conn,
    if_exists="replace",
    index=False
)

conn.execute("""
CREATE INDEX IF NOT EXISTS idx_district_day
ON district_demand(district, day)
""")

conn.commit()
conn.close()

############################################
# SUMMARY
############################################

print("\nSimulation complete\n")

print("Top districts:\n")

print(
    df.sort_values(
        "daily_cylinders",
        ascending=False
    ).head(10)[["state", "district", "daily_cylinders"]]
)

print("\nExpected national daily demand:\n")

print(int(national_daily))