import pandas as pd
from supabase import create_client

############################################
# CONFIG
############################################

SUPABASE_URL = "https://igcmuolrpapjrbucezko.supabase.co"
SUPABASE_KEY = "sb_publishable_3CgfrcTkroF9vITNQCbL9g_EPO0nXdb"

TABLE_BOOKINGS = "booking_log"
TABLE_DEMAND   = "district_demand"
TABLE_CONSUMER = "consumer"

BATCH = 3000

############################################

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

############################################
# HELPER: paginate any table fully
############################################

def fetch_all(table, columns, order_col=None):
    """
    Offset pagination. Supabase caps .range() at 1000 rows per call
    regardless of the limit you pass, so we step in chunks of 1000.
    """
    rows  = []
    limit = 1000          # Supabase hard cap per request
    offset = 0
    query_cols = columns
    if order_col and order_col not in columns:
        query_cols = columns + f",{order_col}"
    while True:
        q = supabase.table(table).select(query_cols).range(offset, offset + limit - 1)
        if order_col:
            q = q.order(order_col)
        data = q.execute().data
        if not data:
            break
        rows.extend(data)
        print(f"  {table}: loaded {len(rows)}")
        if len(data) < limit:
            break
        offset += limit
    return rows

############################################
# LOAD BOOKINGS
############################################

print("loading booking logs...")
booking_rows = fetch_all(
    TABLE_BOOKINGS,
    "timestamp,consumer_id,district,cylinders_requested",
    order_col="timestamp"
)
booking_df = pd.DataFrame(booking_rows)

############################################
# LOAD CONSUMERS (state mapping)
############################################

print("loading consumers...")
consumer_rows = fetch_all(TABLE_CONSUMER, "consumer_id,state", order_col="consumer_id")
consumer_df   = pd.DataFrame(consumer_rows)

consumer_state_map = dict(
    zip(consumer_df.consumer_id, consumer_df.state)
)

############################################
# PREPARE DATE COLUMN
############################################

booking_df["timestamp"] = pd.to_datetime(
    booking_df["timestamp"], format="ISO8601"
)
booking_df["day"]   = booking_df["timestamp"].dt.date
booking_df["state"] = booking_df["consumer_id"].map(consumer_state_map)

# drop rows where state could not be mapped
booking_df = booking_df.dropna(subset=["state"])

############################################
# AGGREGATE DEMAND  (one row per day/state/district)
############################################

print("aggregating demand...")

demand_df = (
    booking_df
    .groupby(["day", "state", "district"])["cylinders_requested"]
    .sum()
    .reset_index()
    .rename(columns={"cylinders_requested": "demand"})
)

demand_df["baseline"]      = demand_df["demand"]
demand_df["season_factor"] = 1.0
demand_df["day"]           = demand_df["day"].astype(str)

print(f"  {len(demand_df)} district-day rows to write")

############################################
# CLEAR OLD DATA  (safe truncate pattern)
############################################

print("clearing old demand table...")

# Delete all rows using the season_factor column which always exists.
# gte("season_factor", 0) matches every row (factors are always >= 0).
supabase.table(TABLE_DEMAND).delete().gte("season_factor", 0).execute()
print("  cleared")

############################################
# UPLOAD NEW DEMAND
############################################

print("uploading demand...")

records = demand_df.to_dict("records")

for i in range(0, len(records), BATCH):
    supabase.table(TABLE_DEMAND).insert(records[i:i+BATCH]).execute()
    print(f"  uploaded {min(i+BATCH, len(records))} / {len(records)}")

############################################

print("\nDone. district_demand updated from booking logs.")
print(f"Total rows written: {len(demand_df)}")