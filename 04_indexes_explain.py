"""Step 4: indexes on customer_id and transaction_time, with explain() before and after."""
from config import TRANSACTIONS, get_db

db = get_db()
txns = db[TRANSACTIONS]


def stages(plan):
    """Walk the winning plan and collect stage names (handles MongoDB 7 nesting)."""
    out = []
    if isinstance(plan, dict):
        if "stage" in plan:
            out.append(plan["stage"])
        for key in ("queryPlan", "inputStage", "inputStages"):
            child = plan.get(key)
            if isinstance(child, list):
                for c in child:
                    out += stages(c)
            elif child:
                out += stages(child)
    return out


def show(label, filt):
    info = txns.find(filt).explain()
    stats = info["executionStats"]
    print(f"--- {label} ---")
    print("winning plan stages :", " -> ".join(stages(info["queryPlanner"]["winningPlan"])))
    print("docs examined       :", stats["totalDocsExamined"])
    print("index keys examined :", stats["totalKeysExamined"])
    print("docs returned       :", stats["nReturned"])
    print("execution time (ms) :", stats["executionTimeMillis"])
    print()
    return stats


# pick the customer with the most transactions so the query is meaningful
busiest = next(txns.aggregate([{"$group": {"_id": "$customer_id", "n": {"$sum": 1}}},
                               {"$sort": {"n": -1}}, {"$limit": 1}]))
query = {"customer_id": busiest["_id"]}
print(f"Query: all transactions of customer {busiest['_id']}  ({busiest['n']} documents)\n")

# start from a clean slate: drop every index except the default _id one
txns.drop_indexes()
before = show("BEFORE index", query)

txns.create_index("customer_id")
txns.create_index("transaction_time")
print("indexes now:", list(txns.index_information().keys()), "\n")

after = show("AFTER index", query)
print(f"documents examined: {before['totalDocsExamined']} -> {after['totalDocsExamined']}")
