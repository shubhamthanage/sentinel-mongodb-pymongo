"""Step 2: load SENTINEL data into two collections.

customers    -> one document per customer, devices EMBEDDED as an array
transactions -> one document per transaction, customer_id REFERENCED,
                merchant + device details copied in (snapshot at transaction time)

Source: the instructor's supabase_db_kit (the same tables `python ingest.py all` loads
into Supabase). Tables used: transactions, labels, customers, merchants, devices.
"""
import glob
import json
import math
import os
import sys

import pandas as pd

from config import CUSTOMERS, DATA_DIR, TARGET_TXNS, TRANSACTIONS, get_db


def read_table(name: str) -> pd.DataFrame:
    """Read every csv.gz shard of one table (all splits)."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, name, "*", "*.csv.gz")))
    if not files:
        sys.exit(f"No files found for table '{name}' under DATA_DIR={DATA_DIR!r}.\n"
                 "Unzip supabase_db_kit and point DATA_DIR at its data/tables folder.")
    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)


def clean(v):
    """Turn numpy / pandas values into plain Python values that BSON understands."""
    if v is None or v is pd.NaT:
        return None
    if hasattr(v, "item"):                       # numpy scalar -> python scalar
        v = v.item()
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def parse_place(v):
    """merchant_location is a JSON string like {"city":"Jaipur","lat":26.9,"lon":75.8}."""
    try:
        return json.loads(v) if isinstance(v, str) else None
    except ValueError:
        return None


def load_frame() -> pd.DataFrame:
    tx = read_table("transactions")
    labels = read_table("labels")[["transaction_id", "fraud_label"]]
    df = tx.merge(labels, on="transaction_id", how="left", validate="one_to_one")
    if df["fraud_label"].isna().any():
        sys.exit("Some transactions have no label - check the labels table.")
    df["transaction_time"] = pd.to_datetime(df["transaction_time"], utc=True)
    return df


def sample_by_customer(df: pd.DataFrame, target: int) -> pd.DataFrame:
    """Pick whole customers (all their transactions) until we have >= target rows.
    Sampling customers, not random rows, keeps each customer's history together,
    which the 'last 24 hours' pipeline needs."""
    if target <= 0:
        return df
    counts = df.groupby("customer_id").size().sample(frac=1.0, random_state=42)
    chosen = counts[counts.cumsum().shift(fill_value=0) < target].index
    return df[df["customer_id"].isin(chosen)]


def build_documents(df, customers, merchants, devices):
    merch = {m["merchant_id"]: m for m in merchants.to_dict("records")}
    kind = dict(zip(devices["device_id"], devices["device_kind"]))
    cust = {c["customer_id"]: c for c in customers.to_dict("records")}

    txn_docs, cust_devices = [], {}
    for r in df.to_dict("records"):
        r = {k: clean(v) for k, v in r.items()}
        cid, did, t = r["customer_id"], r["device_id"], r["transaction_time"]
        m = merch[r["merchant_id"]]

        txn_docs.append({
            "transaction_id": r["transaction_id"],
            "customer_id": cid,                                    # reference
            "amount": r["amount_paise"] / 100,                     # rupees
            "amount_paise": r["amount_paise"],
            "transaction_time": t.to_pydatetime(),
            "payment_channel": r["payment_channel"],
            "merchant": {                                          # embedded snapshot
                "merchant_id": r["merchant_id"],
                "category": r["merchant_category"],
                "location": parse_place(m["merchant_location"]),
            },
            "device": {"device_id": did, "kind": kind.get(did)},   # embedded snapshot
            "fraud_label": int(r["fraud_label"]),
            "split": r["split"],
        })

        if did is not None:
            d = cust_devices.setdefault(cid, {}).setdefault(did, {
                "device_id": did, "kind": kind.get(did),
                "first_seen": t, "last_seen": t, "txn_count": 0})
            d["first_seen"], d["last_seen"] = min(d["first_seen"], t), max(d["last_seen"], t)
            d["txn_count"] += 1

    cust_docs = []
    for cid in df["customer_id"].unique():
        c = cust[cid]
        cust_docs.append({
            "customer_id": cid,
            "age": clean(c["customer_age"]),
            "home_city": clean(c["home_city"]),
            "region": clean(c["region"]),
            "created_at": pd.Timestamp(c["created_at"]).to_pydatetime(),
            "devices": [
                {**d, "first_seen": d["first_seen"].to_pydatetime(),
                 "last_seen": d["last_seen"].to_pydatetime()}
                for d in cust_devices.get(cid, {}).values()],      # embedded array
        })
    return cust_docs, txn_docs


def main():
    df = sample_by_customer(load_frame(), TARGET_TXNS)
    cust_docs, txn_docs = build_documents(
        df, read_table("customers"), read_table("merchants"), read_table("devices"))
    print(f"Prepared {len(cust_docs)} customers and {len(txn_docs)} transactions")

    db = get_db()
    db[CUSTOMERS].drop()                       # re-runnable: start clean every time
    db[TRANSACTIONS].drop()
    db[CUSTOMERS].create_index("customer_id", unique=True)

    for name, docs in ((CUSTOMERS, cust_docs), (TRANSACTIONS, txn_docs)):
        for i in range(0, len(docs), 5000):    # batches keep each request small
            db[name].insert_many(docs[i:i + 5000], ordered=False)
        print(f"{name}: {db[name].count_documents({})} documents")

    frauds = db[TRANSACTIONS].count_documents({"fraud_label": 1})
    print(f"fraud transactions: {frauds}")
    print("sample customer:", db[CUSTOMERS].find_one({}, {"_id": 0}))
    print("sample transaction:", db[TRANSACTIONS].find_one({}, {"_id": 0}))


if __name__ == "__main__":
    main()
