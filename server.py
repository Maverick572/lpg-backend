"""
LPG Supply Chain — Unified API Server
======================================
Combines:
  • Live booking simulator  (background task)
  • Demand aggregation loop (background task)
  • REST API endpoints      (for frontend)

Endpoints
---------
GET /                          → server status
GET /api/demand                → district-wise demand (latest or ?date=YYYY-MM-DD)
GET /api/supply-chain          → full optimizer output (nodes + flows + summary)
GET /api/supply-chain/flows    → flows array only (lighter payload for arrow rendering)
GET /api/supply-chain/nodes    → nodes dict only  (lighter payload for marker rendering)

Run with:
    uvicorn server:app --reload --port 8000

Static files required in same directory:
    lpg_bottling_plants_india.json
    lpg_refineries_india.json
    lpg_fractionators_india.json
    gadm41_IND_2.json
"""

import asyncio
import json
import math
import heapq
import uuid
import os
from datetime import datetime, timedelta, date
from functools import lru_cache
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client

##############################################################
# CONFIG
##############################################################

import os
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# Simulator
BOOKINGS_PER_CYCLE    = 200
BOOKING_INTERVAL_SEC  = 10
AGG_INTERVAL_SEC      = 60
RANDOM_SEED           = 42

# Supply estimates
BOTTLING_PLANT_DAILY_CAPACITY = 3000      # cyl/day per plant
REFINERY_CYL_PER_MMTPA_DAY   = 8100      # cyl/day per MMTPA capacity
FRACTIONATOR_DAILY_CAPACITY   = 1500      # cyl/day per fractionator

# Routing
MAX_SUPPLY_DISTANCE_KM         = 800
MAX_DISTRICT_TRANSFER_DISTANCE = 400
TOP_K_SOURCES_PER_DISTRICT     = 5

# Static files
BOTTLING_PLANTS_FILE = "lpg_bottling_plants_india.json"
REFINERIES_FILE      = "lpg_refineries_india.json"
FRACTIONATORS_FILE   = "lpg_fractionators_india.json"
GEOJSON_FILE         = "gadm41_IND_2.json"

# Optimizer cache TTL (seconds) — re-run optimizer at most once per minute
OPTIMIZER_CACHE_TTL  = 60

##############################################################
# INIT
##############################################################

rng      = np.random.default_rng(RANDOM_SEED)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
app      = FastAPI(title="LPG Supply Chain API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # tighten this in production
    allow_methods=["*"],
    allow_headers=["*"],
)

##############################################################
# ── STATIC DATA (loaded once at startup) ──────────────────
##############################################################

_static: dict = {}          # populated in startup
_optimizer_cache: dict = {} # {date_str: {result, computed_at}}

def load_static_files():
    with open(BOTTLING_PLANTS_FILE)  as f: bp   = json.load(f)
    with open(REFINERIES_FILE)       as f: ref  = json.load(f)
    with open(FRACTIONATORS_FILE)    as f: frac = json.load(f)
    with open(GEOJSON_FILE)          as f: geo  = json.load(f)
    return bp, ref, frac, geo

##############################################################
# ── HELPERS ───────────────────────────────────────────────
##############################################################

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_centroid(geometry):
    coords = []
    def collect(obj):
        if isinstance(obj, list):
            if len(obj) >= 2 and isinstance(obj[0], (int, float)):
                coords.append((obj[1], obj[0]))
            else:
                for item in obj:
                    collect(item)
    collect(geometry["coordinates"])
    if not coords:
        return None, None
    return (sum(c[0] for c in coords) / len(coords),
            sum(c[1] for c in coords) / len(coords))


def normalise(s: str) -> str:
    return s.lower().replace(" ", "").replace("-", "").replace(".", "").replace("_", "")


def fetch_all_supabase(table: str, columns: str, order_col: str = None) -> list:
    """Paginate Supabase respecting the 1000-row hard cap."""
    rows   = []
    limit  = 1000
    offset = 0
    while True:
        q = supabase.table(table).select(columns).range(offset, offset + limit - 1)
        if order_col:
            q = q.order(order_col)
        data = q.execute().data
        if not data:
            break
        rows.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return rows

##############################################################
# ── DISTRICT GEO INDEX ────────────────────────────────────
##############################################################

def build_district_index(geo) -> dict:
    index = {}
    for feat in geo["features"]:
        p = feat["properties"]
        lat, lon = get_centroid(feat["geometry"])
        if lat is None:
            continue
        key = normalise(p["NAME_2"])
        index[key] = {
            "lat":   lat,
            "lon":   lon,
            "name":  p["NAME_2"],
            "state": p["NAME_1"],
            "gid":   p["GID_2"],
        }
    return index

##############################################################
# ── OPTIMIZER CORE ────────────────────────────────────────
##############################################################

def build_nodes(bp_data, ref_data, frac_data, demand_rows, district_index) -> dict:
    nodes = {}

    for bp in bp_data["bottling_plants"]:
        nid = f"bp_{bp['id']}"
        nodes[nid] = {
            "id": nid, "type": "bottling_plant",
            "name": bp["name"], "lat": bp["latitude"], "lon": bp["longitude"],
            "state": bp["state"], "district": None,
            "supply": float(BOTTLING_PLANT_DAILY_CAPACITY), "demand": 0.0,
            "net": float(BOTTLING_PLANT_DAILY_CAPACITY),
        }

    for ref in ref_data["refineries"]:
        nid    = f"ref_{ref['id']}"
        supply = (ref.get("capacity_mmtpa") or 5.0) * REFINERY_CYL_PER_MMTPA_DAY
        nodes[nid] = {
            "id": nid, "type": "refinery",
            "name": ref["name"], "lat": ref["latitude"], "lon": ref["longitude"],
            "state": ref["state"], "district": None,
            "supply": round(supply, 1), "demand": 0.0, "net": round(supply, 1),
        }

    for frac in frac_data["fractionators"]:
        nid = f"frac_{frac['id']}"
        nodes[nid] = {
            "id": nid, "type": "fractionator",
            "name": frac["name"], "lat": frac["latitude"], "lon": frac["longitude"],
            "state": frac["state"], "district": None,
            "supply": float(FRACTIONATOR_DAILY_CAPACITY), "demand": 0.0,
            "net": float(FRACTIONATOR_DAILY_CAPACITY),
        }

    unmatched = 0
    for row in demand_rows:
        dname  = row["district"]
        dstate = row["state"]
        demand = float(row["demand"])
        key    = normalise(dname)

        geo_entry = district_index.get(key)
        if geo_entry is None:
            short      = key[:6]
            candidates = [k for k in district_index if k.startswith(short)]
            geo_entry  = district_index[candidates[0]] if candidates else None
        if geo_entry is None:
            unmatched += 1
            continue

        nid = f"district_{key}"
        if nid in nodes:
            nodes[nid]["demand"] += demand
            nodes[nid]["net"]     = nodes[nid]["supply"] - nodes[nid]["demand"]
        else:
            nodes[nid] = {
                "id": nid, "type": "district",
                "name": dname, "lat": geo_entry["lat"], "lon": geo_entry["lon"],
                "state": dstate, "district": dname,
                "supply": 0.0, "demand": demand, "net": -demand,
            }

    if unmatched:
        print(f"[optimizer] {unmatched} districts could not be geo-located")
    return nodes


def run_optimizer(nodes: dict):
    supply_nodes   = {nid: n for nid, n in nodes.items()
                      if n["type"] in ("bottling_plant", "refinery", "fractionator")}
    district_nodes = {nid: n for nid, n in nodes.items()
                      if n["type"] == "district"}

    supply_remaining = {nid: n["supply"] for nid, n in supply_nodes.items()}
    demand_remaining = {nid: n["demand"] for nid, n in district_nodes.items()}
    flows = []

    # Phase 1 — sources → districts
    for did, dn in district_nodes.items():
        if demand_remaining[did] <= 0:
            continue
        heap = []
        for sid, sn in supply_nodes.items():
            dist = haversine(dn["lat"], dn["lon"], sn["lat"], sn["lon"])
            if dist <= MAX_SUPPLY_DISTANCE_KM:
                heapq.heappush(heap, (dist, sid))
        drawn = 0
        while heap and demand_remaining[did] > 0:
            dist_km, sid = heapq.heappop(heap)
            if supply_remaining[sid] <= 0:
                continue
            vol = min(demand_remaining[did], supply_remaining[sid])
            if vol <= 0:
                continue
            flows.append({
                "from_id":    sid,
                "to_id":      did,
                "from_type":  nodes[sid]["type"],
                "to_type":    "district",
                "from_lat":   nodes[sid]["lat"],
                "from_lon":   nodes[sid]["lon"],
                "to_lat":     dn["lat"],
                "to_lon":     dn["lon"],
                "from_name":  nodes[sid]["name"],
                "to_name":    dn["name"],
                "volume":     round(vol, 1),
                "distance_km": round(dist_km, 1),
            })
            supply_remaining[sid] -= vol
            demand_remaining[did] -= vol
            drawn += 1
            if drawn >= TOP_K_SOURCES_PER_DISTRICT:
                break

    # Phase 2 — hub districts → deficit districts
    hub_districts = {}
    for did, dn in district_nodes.items():
        for sid, sn in supply_nodes.items():
            d = haversine(dn["lat"], dn["lon"], sn["lat"], sn["lon"])
            if d < 50 and supply_remaining[sid] > 0:
                hub_districts[did] = hub_districts.get(did, 0) + supply_remaining[sid]

    deficit_list = sorted(
        [(demand_remaining[did], did) for did in district_nodes if demand_remaining[did] > 0],
        reverse=True
    )
    for _, did in deficit_list:
        dn = district_nodes[did]
        if demand_remaining[did] <= 0:
            continue
        heap = []
        for hid, avail in hub_districts.items():
            if hid == did or avail <= 0:
                continue
            hn   = district_nodes[hid]
            dist = haversine(dn["lat"], dn["lon"], hn["lat"], hn["lon"])
            if dist <= MAX_DISTRICT_TRANSFER_DISTANCE:
                heapq.heappush(heap, (dist, hid))
        while heap and demand_remaining[did] > 0:
            dist_km, hid = heapq.heappop(heap)
            avail = hub_districts[hid]
            if avail <= 0:
                continue
            vol = min(demand_remaining[did], avail)
            hn  = district_nodes[hid]
            flows.append({
                "from_id":    hid,
                "to_id":      did,
                "from_type":  "district",
                "to_type":    "district",
                "from_lat":   hn["lat"],
                "from_lon":   hn["lon"],
                "to_lat":     dn["lat"],
                "to_lon":     dn["lon"],
                "from_name":  hn["name"],
                "to_name":    dn["name"],
                "volume":     round(vol, 1),
                "distance_km": round(dist_km, 1),
            })
            hub_districts[hid]    -= vol
            demand_remaining[did] -= vol

    return flows, demand_remaining


def run_full_optimizer(target_date: str = None) -> dict:
    """Run optimizer end-to-end and return the full output dict."""
    if target_date is None:
        target_date = date.today().isoformat()

    # Check cache
    cached = _optimizer_cache.get(target_date)
    if cached:
        age = (datetime.utcnow() - cached["computed_at"]).total_seconds()
        if age < OPTIMIZER_CACHE_TTL:
            return cached["result"]

    print(f"[optimizer] running for {target_date}...")

    demand_rows = fetch_all_supabase(
        "district_demand", "day,state,district,demand", order_col="day"
    )
    # filter to target date; fall back to latest available
    day_rows = [r for r in demand_rows if r.get("day") == target_date]
    if not day_rows and demand_rows:
        latest   = max(r["day"] for r in demand_rows)
        day_rows = [r for r in demand_rows if r["day"] == latest]
        target_date = latest

    bp_data   = _static["bp"]
    ref_data  = _static["ref"]
    frac_data = _static["frac"]
    dist_idx  = _static["district_index"]

    nodes                    = build_nodes(bp_data, ref_data, frac_data, day_rows, dist_idx)
    flows, demand_remaining  = run_optimizer(nodes)

    total_demand = sum(n["demand"] for n in nodes.values() if n["type"] == "district")
    total_supply = sum(n["supply"] for n in nodes.values() if n["type"] != "district")
    total_routed = sum(f["volume"] for f in flows)
    unmet        = sum(v for v in demand_remaining.values() if v > 0)

    result = {
        "run_at":  datetime.utcnow().isoformat() + "Z",
        "date":    target_date,
        "nodes":   nodes,
        "flows":   flows,
        "summary": {
            "total_demand":           round(total_demand, 1),
            "total_supply":           round(total_supply, 1),
            "total_routed":           round(total_routed, 1),
            "unmet_demand":           round(unmet, 1),
            "districts_with_surplus": sum(1 for v in demand_remaining.values() if v <= 0),
            "districts_with_deficit": sum(1 for v in demand_remaining.values() if v >  0),
            "total_arcs":             len(flows),
        },
    }

    _optimizer_cache[target_date] = {
        "result":      result,
        "computed_at": datetime.utcnow(),
    }
    print(f"[optimizer] done — {len(flows)} arcs, unmet={round(unmet,1)}")
    return result

##############################################################
# ── SIMULATOR TASKS ───────────────────────────────────────
##############################################################

_sim: dict = {}   # populated at startup with consumer data

async def generate_bookings():
    now  = datetime.utcnow()
    rows = []
    districts = _sim["districts"]
    dgroups   = _sim["district_groups"]
    for _ in range(BOOKINGS_PER_CYCLE):
        d   = rng.choice(districts)
        row = dgroups[d].sample(1).iloc[0]
        rows.append({
            "booking_id":          "SIM-" + uuid.uuid4().hex,
            "timestamp":           now.isoformat(),
            "consumer_id":         row.consumer_id,
            "district":            row.district,
            "state":               row.state,
            "distributor_id":      f"DIST-{row.district[:3]}",
            "cylinders_requested": 1,
            "booking_channel":     str(rng.choice(["app", "ivr", "whatsapp"])),
            "payment_mode":        str(rng.choice(["UPI", "cash"])),
            "booking_status":      "confirmed",
        })
    try:
        supabase.table("booking_log").insert(rows).execute()
        print(f"[sim] inserted {len(rows)} bookings")
    except Exception as e:
        print(f"[sim] booking error: {e}")


async def update_demand_table():
    since = (datetime.utcnow() - timedelta(hours=1)).isoformat()
    try:
        data = (
            supabase.table("booking_log")
            .select("consumer_id,district,state")
            .gte("timestamp", since)
            .execute()
            .data
        )
        if not data:
            return

        df  = pd.DataFrame(data).dropna(subset=["state", "district"])
        agg = df.groupby(["state", "district"]).size().reset_index(name="demand")

        today = datetime.utcnow().date().isoformat()
        upload_rows = [
            {
                "day": today, "state": r.state, "district": r.district,
                "demand": int(r.demand), "baseline": int(r.demand), "season_factor": 1.0,
            }
            for _, r in agg.iterrows()
        ]
        supabase.table("district_demand").upsert(
            upload_rows, on_conflict="day,state,district"
        ).execute()
        print(f"[sim] demand table updated — {len(upload_rows)} districts")

        # Invalidate optimizer cache for today
        _optimizer_cache.pop(today, None)

    except Exception as e:
        print(f"[sim] aggregation error: {e}")


async def booking_loop():
    while True:
        await generate_bookings()
        await asyncio.sleep(BOOKING_INTERVAL_SEC)


async def demand_loop():
    while True:
        await update_demand_table()
        await asyncio.sleep(AGG_INTERVAL_SEC)

##############################################################
# ── STARTUP ───────────────────────────────────────────────
##############################################################

@app.on_event("startup")
async def startup():
    print("[startup] loading static files...")
    bp, ref, frac, geo     = load_static_files()
    _static["bp"]          = bp
    _static["ref"]         = ref
    _static["frac"]        = frac
    _static["geo"]         = geo
    _static["district_index"] = build_district_index(geo)
    print(f"[startup] {len(_static['district_index'])} districts indexed")

    print("[startup] loading consumers...")
    consumers   = fetch_all_supabase("consumer", "consumer_id,district,state", "consumer_id")
    consumer_df = pd.DataFrame(consumers)
    if len(consumer_df) == 0:
        print("[startup] WARNING: consumer table empty — simulator disabled")
    else:
        _sim["district_groups"] = {
            d: g.reset_index(drop=True)
            for d, g in consumer_df.groupby("district")
        }
        _sim["districts"] = list(_sim["district_groups"].keys())
        print(f"[startup] {len(_sim['districts'])} districts loaded for simulator")
        asyncio.create_task(booking_loop())
        asyncio.create_task(demand_loop())


##############################################################
# ── API ROUTES ────────────────────────────────────────────
##############################################################

@app.get("/")
def status():
    return {
        "status":    "running",
        "utc_time":  datetime.utcnow().isoformat() + "Z",
        "simulator": {
            "bookings_per_min":         int(BOOKINGS_PER_CYCLE * 60 / BOOKING_INTERVAL_SEC),
            "aggregation_interval_sec": AGG_INTERVAL_SEC,
            "districts_loaded":         len(_sim.get("districts", [])),
        },
    }


# ── /api/demand ──────────────────────────────────────────
@app.get("/api/demand")
def get_demand(date_filter: Optional[str] = Query(None, alias="date")):
    """
    Returns district-wise demand enriched with lat/lon for map rendering.

    Query params:
      ?date=YYYY-MM-DD   (optional; defaults to latest available day)

    Response:
    {
      "date": "2026-03-28",
      "districts": [
        {
          "district":   "Thane",
          "state":      "Maharashtra",
          "demand":     508,
          "lat":        19.21,
          "lon":        73.09,
          "fill_color": "#e03131"   // red→green scale based on demand
        }, ...
      ]
    }
    """
    rows = fetch_all_supabase("district_demand", "day,state,district,demand", "day")
    if not rows:
        raise HTTPException(status_code=404, detail="No demand data found")

    if date_filter:
        day_rows = [r for r in rows if r["day"] == date_filter]
        if not day_rows:
            raise HTTPException(status_code=404, detail=f"No data for date {date_filter}")
    else:
        latest   = max(r["day"] for r in rows)
        day_rows = [r for r in rows if r["day"] == latest]

    dist_idx = _static.get("district_index", {})
    max_demand = max((float(r["demand"]) for r in day_rows), default=1)

    result = []
    for r in day_rows:
        key       = normalise(r["district"])
        geo_entry = dist_idx.get(key)
        if geo_entry is None:
            short      = key[:6]
            candidates = [k for k in dist_idx if k.startswith(short)]
            geo_entry  = dist_idx[candidates[0]] if candidates else None

        demand = float(r["demand"])
        # Simple red (high) → green (low) colour scale
        ratio = min(demand / max_demand, 1.0)
        red   = int(224 * ratio)
        green = int(166 + (1 - ratio) * 89)
        color = f"#{red:02x}{green:02x}31"

        result.append({
            "district":   r["district"],
            "state":      r["state"],
            "demand":     demand,
            "lat":        geo_entry["lat"] if geo_entry else None,
            "lon":        geo_entry["lon"] if geo_entry else None,
            "fill_color": color,
        })

    return {
        "date":      day_rows[0]["day"],
        "districts": sorted(result, key=lambda x: -x["demand"]),
    }


# ── /api/supply-chain ────────────────────────────────────
@app.get("/api/supply-chain")
def get_supply_chain(date_filter: Optional[str] = Query(None, alias="date")):
    """
    Full optimizer output: nodes + flows + summary.
    Heavy — prefer /flows or /nodes for partial fetches.
    """
    try:
        return run_full_optimizer(date_filter)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply-chain/flows")
def get_flows(date_filter: Optional[str] = Query(None, alias="date")):
    """
    Flows array only — coordinates + volumes for arrow rendering.

    Each item:
    {
      "from_id":     "bp_12",
      "to_id":       "district_thane",
      "from_type":   "bottling_plant",
      "to_type":     "district",
      "from_name":   "Thane LPG Bottling Plant",
      "to_name":     "Thane",
      "from_lat":    19.2,
      "from_lon":    73.1,
      "to_lat":      19.21,
      "to_lon":      73.09,
      "volume":      842.5,
      "distance_km": 37.4
    }
    """
    try:
        result = run_full_optimizer(date_filter)
        return {
            "date":    result["date"],
            "run_at":  result["run_at"],
            "summary": result["summary"],
            "flows":   result["flows"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/supply-chain/nodes")
def get_nodes(date_filter: Optional[str] = Query(None, alias="date")):
    """
    Nodes dict only — for marker/layer rendering.
    """
    try:
        result = run_full_optimizer(date_filter)
        return {
            "date":  result["date"],
            "nodes": result["nodes"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))