"""Step 3: the five basic operations - insert_one, insert_many, find, update_many, delete_one."""
from datetime import datetime, timezone

from config import CUSTOMERS, TRANSACTIONS, get_db

db = get_db()
customers, txns = db[CUSTOMERS], db[TRANSACTIONS]

# clean up demo documents from a previous run so this script can be re-run
customers.delete_many({"demo": True})
txns.delete_many({"demo": True})

# ---- 1. insert_one: a brand new customer -----------------------------------
res = customers.insert_one({
    "customer_id": "DEMO-CUST-1", "age": 29, "location": "Pune", "account_age": 12,
    "devices": [{"device_id": "DEMO-DEV-1", "txn_count": 0}], "demo": True,
})
print("insert_one ->", res.inserted_id)

# ---- 2. insert_many: three transactions for that customer ------------------
now = datetime.now(timezone.utc)
res = txns.insert_many([
    {"transaction_id": f"DEMO-TXN-{i}", "customer_id": "DEMO-CUST-1",
     "amount": amt, "amount_paise": amt * 100, "transaction_time": now,
     "merchant": {"category": cat, "location": "Pune"},
     "device": {"device_id": "DEMO-DEV-1"}, "fraud_label": 0, "demo": True}
    for i, (amt, cat) in enumerate([(250, "grocery"), (1200, "electronics"), (99, "food")], 1)
])
print("insert_many ->", len(res.inserted_ids), "documents")

# ---- 3. find with a filter AND a projection --------------------------------
# filter: fraud transactions above 10,000 rupees. projection: only 4 fields, no _id
print("\nfind (fraud, amount > 10000), projection only 4 fields:")
for doc in txns.find({"fraud_label": 1, "amount": {"$gt": 10000}},
                     {"_id": 0, "transaction_id": 1, "amount": 1, "merchant.category": 1}
                     ).sort("amount", -1).limit(5):
    print("  ", doc)

# ---- 4. update_many: flag every fraud transaction for review ---------------
res = txns.update_many({"fraud_label": 1}, {"$set": {"review_status": "flagged"}})
print(f"\nupdate_many -> matched {res.matched_count}, modified {res.modified_count}")

# ---- 5. delete_one: remove one demo transaction ----------------------------
res = txns.delete_one({"transaction_id": "DEMO-TXN-3"})
print("delete_one  -> deleted", res.deleted_count)

# remove the rest of the demo data so the real data stays clean
txns.delete_many({"demo": True})
customers.delete_many({"demo": True})
print("demo documents cleaned up")
