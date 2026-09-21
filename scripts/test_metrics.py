import urllib.request
import json

def es_search(index, query):
    url = f"http://localhost:9200/{index}/_search"
    data = json.dumps(query).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print("ES Error:", e.read().decode())
        raise

def main():
    # 1. API Latency percentiles
    api_query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"log_type.keyword": "api"}},
                    {"term": {"event.keyword": "request_sent"}}
                ]
            }
        },
        "aggs": {
            "latency_p": {
                "percentiles": {
                    "field": "latency_ms",
                    "percents": [50, 90, 95, 99]
                }
            },
            "status_codes": {
                "terms": {
                    "field": "status_code"
                }
            },
            "methods": {
                "terms": {
                    "field": "method.keyword"
                }
            }
        }
    }
    api_res = es_search("cdc-api-logs-*", api_query)
    print("=== API METRICS ===")
    print(f"Total Requests Analyzed: {api_res['hits']['total']['value']}")
    print("Latency Percentiles (ms):", api_res['aggregations']['latency_p']['values'])
    print("Status codes:", [b for b in api_res['aggregations']['status_codes']['buckets']])
    print("Methods:", [b for b in api_res['aggregations']['methods']['buckets']])

    # 2. Consumer Latency percentiles
    consumer_query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"log_type.keyword": "consumer"}}
                ]
            }
        },
        "aggs": {
            "by_consumer": {
                "terms": {
                    "field": "consumer.keyword"
                },
                "aggs": {
                    "proc_latency_p": {
                        "percentiles": {
                            "field": "processing_latency_ms",
                            "percents": [50, 90, 95, 99]
                        }
                    },
                    "sink_latency_p": {
                        "percentiles": {
                            "field": "sink_latency_ms",
                            "percents": [50, 90, 95, 99]
                        }
                    },
                    "events": {
                        "terms": {
                            "field": "event.keyword"
                        }
                    }
                }
            }
        }
    }
    cons_res = es_search("cdc-consumer-logs-*", consumer_query)
    print("\n=== CONSUMER METRICS ===")
    print(f"Total Consumer Events: {cons_res['hits']['total']['value']}")
    for b in cons_res['aggregations']['by_consumer']['buckets']:
        print(f"\nConsumer: {b['key']} (Total Events: {b['doc_count']})")
        print("  Events:", [e for e in b['events']['buckets']])
        print("  Sink Latency Percentiles (ms):", b['sink_latency_p']['values'])
        print("  Proc Latency Percentiles (ms):", b['proc_latency_p']['values'])

if __name__ == "__main__":
    main()
