import urllib.request
import json
from datetime import datetime

def query_es(index, query):
    url = f"http://localhost:9200/{index}/_search"
    req = urllib.request.Request(
        url,
        data=json.dumps(query).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())

def main():
    query = {
        "size": 0,
        "aggs": {
            "rate_per_10s": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "10s"
                }
            }
        }
    }
    res = query_es("cdc-api-logs-*", query)
    buckets = res["aggregations"]["rate_per_10s"]["buckets"]
    print(f"Total time buckets: {len(buckets)}")
    print("Recent 10s intervals:")
    for b in buckets[-8:]:
        count = b["doc_count"]
        rps = count / 10.0
        print(f"  {b['key_as_string']}: {count} requests ({rps:.1f} req/sec)")

if __name__ == "__main__":
    main()
