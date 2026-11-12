"""Step 5: two aggregation pipelines."""
from config import TRANSACTIONS, get_db

txns = get_db()[TRANSACTIONS]

# ---- Pipeline 1: fraud rate per merchant_category ---------------------------
# fraud_label is 0/1, so $avg of it is the fraud rate directly
p1 = [
    {"$group": {"_id": "$merchant.category",
                "transactions": {"$sum": 1},
                "frauds": {"$sum": "$fraud_label"},
                "fraud_rate": {"$avg": "$fraud_label"}}},
    {"$addFields": {"fraud_rate_pct": {"$round": [{"$multiply": ["$fraud_rate", 100]}, 3]}}},
    {"$project": {"_id": 0, "merchant_category": "$_id", "transactions": 1,
                  "frauds": 1, "fraud_rate_pct": 1}},
    {"$sort": {"fraud_rate_pct": -1, "transactions": -1}},
]
print("Pipeline 1 - fraud rate per merchant_category")
print(f"{'category':<20}{'txns':>8}{'frauds':>8}{'fraud %':>10}")
for r in txns.aggregate(p1):
    print(f"{str(r['merchant_category']):<20}{r['transactions']:>8}{r['frauds']:>8}{r['fraud_rate_pct']:>10}")

# ---- Pipeline 2: top 10 customers by amount in their LAST 24 HOURS of activity
# "last 24 hours" is measured per customer, back from THEIR most recent transaction.
p2 = [
    # 1. attach each customer's latest transaction time to every one of their rows
    {"$setWindowFields": {"partitionBy": "$customer_id",
                          "output": {"last_txn_time": {"$max": "$transaction_time"}}}},
    # 2. keep only rows within 24 hours before that latest time
    {"$match": {"$expr": {"$gte": ["$transaction_time",
                                   {"$dateSubtract": {"startDate": "$last_txn_time",
                                                      "unit": "hour", "amount": 24}}]}}},
    # 3. add up the amount per customer
    {"$group": {"_id": "$customer_id", "total_amount": {"$sum": "$amount"},
                "txn_count": {"$sum": 1}, "window_ends": {"$first": "$last_txn_time"}}},
    {"$sort": {"total_amount": -1}},
    {"$limit": 10},
    {"$project": {"_id": 0, "customer_id": "$_id", "total_amount": {"$round": ["$total_amount", 2]},
                  "txn_count": 1, "window_ends": 1}},
]
print("\nPipeline 2 - top 10 customers by amount in their last 24 hours of activity")
print(f"{'customer_id':<16}{'total_amount':>16}{'txns':>6}  window ends (UTC)")
for r in txns.aggregate(p2):
    print(f"{str(r['customer_id']):<16}{r['total_amount']:>16,.2f}{r['txn_count']:>6}  {r['window_ends']:%Y-%m-%d %H:%M}")
