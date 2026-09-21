import urllib.request
import json

KIBANA_URL = "http://localhost:5601"

def create_saved_object(obj_type, obj_id, attributes, references=None):
    url = f"{KIBANA_URL}/api/saved_objects/{obj_type}/{obj_id}?overwrite=true"
    payload = {"attributes": attributes}
    if references:
        payload["references"] = references
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
        method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        print(f"[OK] Created {obj_type}/{obj_id} -> {resp.status}")

def main():
    print("Building full Kibana visualizations and dashboard...")

    # 1. Total Ingested Orders Metric Card
    vis_total_orders = {
        "title": "Total Orders Ingested",
        "type": "metric",
        "params": {
            "metric": {
                "labels": {"show": True},
                "style": {"fontSize": 32}
            }
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Total Ingested Orders"}}
        ]
    }
    create_saved_object(
        "visualization", "vis-total-orders",
        {
            "title": "Total Orders Ingested",
            "visState": json.dumps(vis_total_orders),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-api-logs"}]
    )

    # 2. P95 API Latency Metric Card
    vis_p95_latency = {
        "title": "P95 API Latency (ms)",
        "type": "metric",
        "params": {
            "metric": {
                "labels": {"show": True},
                "style": {"fontSize": 32}
            }
        },
        "aggs": [
            {
                "id": "1",
                "enabled": True,
                "type": "percentiles",
                "schema": "metric",
                "params": {
                    "field": "latency_ms",
                    "percents": [95],
                    "customLabel": "P95 API Latency"
                }
            }
        ]
    }
    create_saved_object(
        "visualization", "vis-p95-latency",
        {
            "title": "P95 API Latency",
            "visState": json.dumps(vis_p95_latency),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "event:request_sent", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-api-logs"}]
    )

    # 3. Request Volume Over Time (Histogram / Line Chart)
    vis_req_timeline = {
        "title": "API Request Throughput Over Time",
        "type": "line",
        "params": {
            "type": "line",
            "grid": {"categoryLines": False},
            "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom", "show": True}],
            "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left", "show": True}]
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Request Rate"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment", "params": {"field": "@timestamp", "interval": "auto"}}
        ]
    }
    create_saved_object(
        "visualization", "vis-req-timeline",
        {
            "title": "API Request Throughput Timeline",
            "visState": json.dumps(vis_req_timeline),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-api-logs"}]
    )

    # 4. Consumer Events Breakdown (Pie Chart)
    vis_events_pie = {
        "title": "Consumer Sinks Event Distribution",
        "type": "pie",
        "params": {
            "type": "pie",
            "addTooltip": True,
            "addLegend": True,
            "legendPosition": "right",
            "isDonut": True
        },
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {"field": "event.keyword", "size": 5}}
        ]
    }
    create_saved_object(
        "visualization", "vis-events-pie",
        {
            "title": "Consumer Events Distribution",
            "visState": json.dumps(vis_events_pie),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-consumer-logs"}]
    )

    # 5. Live Streams (Saved Searches)
    create_saved_object(
        "search", "search-api-stream",
        {
            "title": "API Live Event Stream",
            "columns": ["method", "status_code", "latency_ms", "event"],
            "sort": [["@timestamp", "desc"]],
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-api-logs"}]
    )

    create_saved_object(
        "search", "search-consumer-stream",
        {
            "title": "Consumer Sinks Live Stream",
            "columns": ["consumer", "event", "order_id", "sink_latency_ms", "offset"],
            "sort": [["@timestamp", "desc"]],
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-consumer-logs"}]
    )

    # 6. Assemble the Complete Dashboard
    panels = [
        # Row 1: Key Metrics
        {
            "version": "8.10.2",
            "type": "visualization",
            "gridData": {"x": 0, "y": 0, "w": 24, "h": 9, "i": "1"},
            "panelIndex": "1",
            "panelRefName": "panel_total_orders"
        },
        {
            "version": "8.10.2",
            "type": "visualization",
            "gridData": {"x": 24, "y": 0, "w": 24, "h": 9, "i": "2"},
            "panelIndex": "2",
            "panelRefName": "panel_p95_latency"
        },
        # Row 2: Charts
        {
            "version": "8.10.2",
            "type": "visualization",
            "gridData": {"x": 0, "y": 9, "w": 30, "h": 14, "i": "3"},
            "panelIndex": "3",
            "panelRefName": "panel_timeline"
        },
        {
            "version": "8.10.2",
            "type": "visualization",
            "gridData": {"x": 30, "y": 9, "w": 18, "h": 14, "i": "4"},
            "panelIndex": "4",
            "panelRefName": "panel_pie"
        },
        # Row 3: Live Event Streams
        {
            "version": "8.10.2",
            "type": "search",
            "gridData": {"x": 0, "y": 23, "w": 24, "h": 15, "i": "5"},
            "panelIndex": "5",
            "panelRefName": "panel_api_stream"
        },
        {
            "version": "8.10.2",
            "type": "search",
            "gridData": {"x": 24, "y": 23, "w": 24, "h": 15, "i": "6"},
            "panelIndex": "6",
            "panelRefName": "panel_consumer_stream"
        }
    ]

    references = [
        {"name": "panel_total_orders", "type": "visualization", "id": "vis-total-orders"},
        {"name": "panel_p95_latency", "type": "visualization", "id": "vis-p95-latency"},
        {"name": "panel_timeline", "type": "visualization", "id": "vis-req-timeline"},
        {"name": "panel_pie", "type": "visualization", "id": "vis-events-pie"},
        {"name": "panel_api_stream", "type": "search", "id": "search-api-stream"},
        {"name": "panel_consumer_stream", "type": "search", "id": "search-consumer-stream"}
    ]

    create_saved_object(
        "dashboard", "cdc-telemetry-dashboard",
        {
            "title": "CDC Pipeline — Distributed Systems Observability & Telemetry",
            "description": "Live production telemetry dashboard for 1,000 req/sec CDC pipeline soak test",
            "hits": 0,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
            "panelsJSON": json.dumps(panels),
            "timeRestore": True,
            "timeFrom": "now-60m",
            "timeTo": "now"
        },
        references
    )
    print("\n[SUCCESS] Entire Kibana dashboard and visual widgets successfully configured!")

if __name__ == "__main__":
    main()
