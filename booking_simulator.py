import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client

############################################
# CONFIG
############################################

INPUT_FILE = "district_daily_demand_baseline.csv"

SUPABASE_URL = "https://igcmuolrpapjrbucezko.supabase.co"
SUPABASE_KEY = "sb_publishable_3CgfrcTkroF9vITNQCbL9g_EPO0nXdb"

SIM_DAYS       = 60
SIM_SCALE      = 0.02       # 2% of real population
SIM_START_DATE = datetime(2026, 1, 1)
RANDOM_SEED    = 42

############################################
# LPG BEHAVIOUR
############################################

REFILL_MEAN              = 40
DOUBLE_CYLINDER_RATE     = 0.35
HOARDING_RATE            = 0.03
MAX_CONNECTIONS_PER_HOUSE = 4

UPLOAD_BATCH = 2000

############################################

rng      = np.random.default_rng(RANDOM_SEED)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

############################################
# HELPER: make dataframe JSON safe
############################################

def make_json_safe(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
        elif df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: x.isoformat()
                if isinstance(x, (datetime, pd.Timestamp))
                else x
            )
        elif "int"   in str(df[col].dtype): df[col] = df[col].astype(int)
        elif "float" in str(df[col].dtype): df[col] = df[col].astype(float)
        elif "bool"  in str(df[col].dtype): df[col] = df[col].astype(bool)
    return df

############################################
# LOAD DEMAND CSV
############################################

df = pd.read_csv(INPUT_FILE)
df.columns = df.columns.str.strip().str.lower()

required_cols = ["state", "district", "daily_cylinders"]
for col in required_cols:
    if col not in df.columns:
        raise Exception(f"Missing column: {col}")

df["daily_cylinders"] = pd.to_numeric(df["daily_cylinders"], errors="coerce")
df = df.dropna(subset=["daily_cylinders"])
df = df[df["daily_cylinders"] > 0]

############################################
# ADDRESS DATA
############################################

society_prefix = ["Shiv","Sai","Ganesh","Om","Krishna",
                  "Lakshmi","Green","Sun","Silver","Royal","Galaxy","Shanti"]
society_suffix = ["Residency","Heights","Enclave","CHS","Apartments","Towers"]
streets        = ["MG Road","Temple Road","Station Road","Link Road"]
localities     = ["Central","West","East","Phase 1","Phase 2","Sector 4","Sector 9"]
landmarks      = ["near temple","near school","near metro","near hospital","near market"]

############################################
# GENERATE HOUSEHOLDS
############################################

print("creating households...")

consumer_rows        = []
households_by_district = {}
consumer_counter     = 0

for _, row in df.iterrows():
    district     = row["district"]
    state        = row["state"]
    daily_demand = int(row["daily_cylinders"])

    households_needed = int(daily_demand * SIM_DAYS / REFILL_MEAN * SIM_SCALE)
    households_by_district[district] = []

    for _ in range(households_needed):
        society  = rng.choice(society_prefix) + " " + rng.choice(society_suffix)
        building = f"Tower {rng.choice(list('ABCDE'))}"
        house_no = str(rng.integers(1, 1500))
        locality = rng.choice(localities)
        street   = rng.choice(streets)
        landmark = rng.choice(landmarks)
        pincode  = int(rng.integers(400000, 799999))
        lat      = float(18 + rng.random() * 10)
        lon      = float(72 + rng.random() * 10)

        connections = 1
        if rng.random() < DOUBLE_CYLINDER_RATE:
            connections = 2
        if rng.random() < HOARDING_RATE:
            connections = int(rng.integers(2, MAX_CONNECTIONS_PER_HOUSE))

        for _ in range(connections):
            consumer_counter += 1
            cid = f"C-{consumer_counter}"

            consumer_rows.append({
                "consumer_id":           cid,
                "connection_type":       "domestic",
                "house_number":          house_no,
                "building_name":         building,
                "society_name":          society,
                "street":                street,
                "locality":              locality,
                "landmark":              landmark,
                "pincode":               pincode,
                "district":              district,
                "state":                 state,
                "latitude":              lat,
                "longitude":             lon,
                "connection_start_date": "2022-01-01",
                "is_active":             True,
            })

            households_by_district[district].append(cid)

############################################
# UPLOAD CONSUMERS
############################################

consumer_df = make_json_safe(pd.DataFrame(consumer_rows))
print(f"uploading {len(consumer_df)} consumers...")

for i in range(0, len(consumer_df), UPLOAD_BATCH):
    supabase.table("consumer").insert(
        consumer_df.iloc[i:i+UPLOAD_BATCH].to_dict("records")
    ).execute()
    print(f"  uploaded {min(i+UPLOAD_BATCH, len(consumer_df))}")

############################################
# GENERATE BOOKINGS
############################################

print("generating bookings...")

booking_rows    = []
booking_counter = 0

for day in range(SIM_DAYS):
    date = SIM_START_DATE + timedelta(days=day)

    for _, row in df.iterrows():
        district  = row["district"]
        state     = row["state"]
        consumers = households_by_district[district]

        if not consumers:
            continue

        prob_booking_today = 1 / REFILL_MEAN

        for cid in consumers:
            if rng.random() < prob_booking_today:
                booking_counter += 1
                hour = int(rng.choice([8, 9, 10, 18, 19, 20, 21]))

                booking_rows.append({
                    "booking_id":          f"BKG-{booking_counter}",
                    "timestamp":           (date + timedelta(hours=hour))
                                           .strftime("%Y-%m-%dT%H:%M:%S"),
                    "consumer_id":         cid,
                    "distributor_id":      f"DIST-{district[:3]}",
                    "district":            district,
                    "state":               state,
                    "cylinders_requested": 1,
                    "booking_channel":     str(rng.choice(["app","ivr","whatsapp"])),
                    "payment_mode":        str(rng.choice(["UPI","cash"])),
                    "booking_status":      "confirmed",
                })

############################################
# UPLOAD BOOKINGS
############################################

booking_df = make_json_safe(pd.DataFrame(booking_rows))
print(f"uploading {len(booking_df)} bookings...")

for i in range(0, len(booking_df), UPLOAD_BATCH):
    supabase.table("booking_log").insert(
        booking_df.iloc[i:i+UPLOAD_BATCH].to_dict("records")
    ).execute()
    print(f"  uploaded {min(i+UPLOAD_BATCH, len(booking_df))}")

############################################

print("\nComplete.")
print(f"consumers: {len(consumer_df)}")
print(f"bookings:  {len(booking_df)}")