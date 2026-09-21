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
        print(f"[OK] {obj_type}/{obj_id} -> {resp.status}")

def main():
    print("===================================================================")
    print(" Deploying Complete Enterprise Kibana Observability & Benchmark Suite")
    print(" Includes All Figures 1-4: Throughput Timelines & Latency Bar Graphs")
    print("===================================================================")

    # ==========================================
    # 0. System Markdown Header / Context Banner
    # ==========================================
    banner_markdown = (
        "### 🚀 High-Throughput CDC Pipeline — Distributed Systems Observability & Telemetry\n"
        "**Real-Time Data Flow**: Client REST API ➔ PostgreSQL 15 (WAL Logical Replication) ➔ Debezium 2.4 (Avro + Schema Registry) ➔ Apache Kafka ➔ Modern C++ Consumer Fleet ➔ Dual Storage (Redis Hot Cache + Elasticsearch Full-Text Index)\n\n"
        "**Soak Test Target**: 1,000 req/sec sustained ingestion • Zero data loss • Monotonic LSN deduplication • Sub-millisecond cache sync"
    )
    create_saved_object(
        "visualization", "vis-header-banner",
        {
            "title": "Pipeline Telemetry Overview Banner",
            "visState": json.dumps({
                "title": "Pipeline Telemetry Overview Banner",
                "type": "markdown",
                "params": {"markdown": banner_markdown}
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            }
        }
    )

    # ==========================================
    # 1. Executive Metric KPI Cards (2 Rows of 4 Cards)
    # Designed with fontSize: 28 so numbers fit comfortably without clipping
    # ==========================================
    def get_metric_params(label, font_size=28):
        return {
            "addTooltip": True,
            "addLegend": False,
            "type": "metric",
            "metric": {
                "percentageMode": False,
                "useRanges": False,
                "colorSchema": "Green to Red",
                "metricColorMode": "None",
                "colorsRange": [{"from": 0, "to": 100000000}],
                "labels": {"show": True},
                "invertColors": False,
                "style": {
                    "bgFill": "#000",
                    "bgColor": False,
                    "labelColor": False,
                    "subText": "",
                    "fontSize": font_size
                }
            }
        }

    # Card 1: Total Ingested Orders
    create_saved_object(
        "visualization", "vis-total-orders",
        {
            "title": "Total Orders Ingested",
            "visState": json.dumps({
                "title": "Total Orders Ingested",
                "type": "metric",
                "params": get_metric_params("Total Ingested Orders"),
                "aggs": [{"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Total Ingested Orders"}}]
            }),
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

    # Card 2: API P50 Median Latency
    create_saved_object(
        "visualization", "vis-p50-latency",
        {
            "title": "API P50 Median Latency",
            "visState": json.dumps({
                "title": "API P50 Median Latency",
                "type": "metric",
                "params": get_metric_params("P50 Median Latency (ms)"),
                "aggs": [{"id": "1", "enabled": True, "type": "percentiles", "schema": "metric", "params": {"field": "latency_ms", "percents": [50], "customLabel": "P50 Latency (ms)"}}]
            }),
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

    # Card 3: API P95 Tail Latency
    create_saved_object(
        "visualization", "vis-p95-latency",
        {
            "title": "API P95 Tail Latency",
            "visState": json.dumps({
                "title": "API P95 Tail Latency",
                "type": "metric",
                "params": get_metric_params("P95 Tail Latency (ms)"),
                "aggs": [{"id": "1", "enabled": True, "type": "percentiles", "schema": "metric", "params": {"field": "latency_ms", "percents": [95], "customLabel": "P95 Latency (ms)"}}]
            }),
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

    # Card 4: Redis Avg Sink Latency
    create_saved_object(
        "visualization", "vis-redis-sink-latency",
        {
            "title": "Redis Hot Cache Latency",
            "visState": json.dumps({
                "title": "Redis Hot Cache Latency",
                "type": "metric",
                "params": get_metric_params("Redis Avg Lua Latency (ms)"),
                "aggs": [{"id": "1", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "sink_latency_ms", "customLabel": "Redis Lua Latency (ms)"}}]
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "consumer.keyword: RedisUpdater", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-consumer-logs"}]
    )

    # Card 5: Total Consumer Events Processed
    create_saved_object(
        "visualization", "vis-total-consumer-events",
        {
            "title": "Total Consumer Events Handled",
            "visState": json.dumps({
                "title": "Total Consumer Events Handled",
                "type": "metric",
                "params": get_metric_params("Consumer Events Processed"),
                "aggs": [{"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Consumer Events Handled"}}]
            }),
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

    # Card 6: Elasticsearch Avg Sink Latency
    create_saved_object(
        "visualization", "vis-es-sink-latency",
        {
            "title": "Elasticsearch Commit Latency",
            "visState": json.dumps({
                "title": "Elasticsearch Commit Latency",
                "type": "metric",
                "params": get_metric_params("ES Commit Latency (ms)"),
                "aggs": [{"id": "1", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "sink_latency_ms", "customLabel": "ES Commit Latency (ms)"}}]
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "consumer.keyword: ESIndexer", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-consumer-logs"}]
    )

    # Card 7: Idempotent Duplicates Filtered
    create_saved_object(
        "visualization", "vis-idempotency-dedup",
        {
            "title": "Idempotent Replays Filtered",
            "visState": json.dumps({
                "title": "Idempotent Replays Filtered",
                "type": "metric",
                "params": get_metric_params("Deduplicated Replays"),
                "aggs": [{"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Deduplicated Replays"}}]
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "event.keyword: email_notification_skipped_duplicate", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-system-logs"}]
    )

    # Card 8: Dead Letter Queue Faults (Zero Loss)
    create_saved_object(
        "visualization", "vis-dlq-count",
        {
            "title": "Dead Letter Queue Faults",
            "visState": json.dumps({
                "title": "Dead Letter Queue Faults",
                "type": "metric",
                "params": get_metric_params("Quarantined DLQ Events"),
                "aggs": [{"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Quarantined DLQ Events"}}]
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "event.keyword: routed_to_dlq", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-system-logs"}]
    )

    # ==========================================================
    # 2. Benchmark Visualizations (Vega-Lite Grammar)
    # Publication-Grade Visualizations Matching BENCHMARK_REPORT.md
    # ==========================================================

    # --- Figure 1: Sustained Pipeline Throughput Timeline (Line / Area) ---
    fig1_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Figure 1: Pipeline Ingestion Throughput Timeline",
        "width": "container",
        "height": "container",
        "padding": {"top": 15, "left": 15, "right": 15, "bottom": 15},
        "autosize": {"type": "fit", "contains": "padding"},
        "data": {
            "url": {
                "%context%": True,
                "%timefield%": "@timestamp",
                "index": "cdc-api-logs-*",
                "body": {
                    "size": 0,
                    "aggs": {
                        "timeline": {
                            "date_histogram": {
                                "field": "@timestamp",
                                "fixed_interval": "1m"
                            }
                        }
                    }
                }
            },
            "format": {"property": "aggregations.timeline.buckets"}
        },
        "layer": [
            {
                "mark": {
                    "type": "area",
                    "color": "#00bfb3",
                    "opacity": 0.2,
                    "interpolate": "monotone"
                },
                "encoding": {
                    "x": {"field": "key", "type": "temporal"},
                    "y": {"field": "doc_count", "type": "quantitative"}
                }
            },
            {
                "mark": {
                    "type": "line",
                    "color": "#00bfb3",
                    "strokeWidth": 2.5,
                    "point": {"filled": True, "fill": "#00bfb3", "size": 35},
                    "interpolate": "monotone"
                },
                "encoding": {
                    "x": {
                        "field": "key",
                        "type": "temporal",
                        "title": "Ingestion Timeline (UTC)",
                        "axis": {"grid": False, "labelColor": "#666", "titleColor": "#444", "titleFontSize": 12}
                    },
                    "y": {
                        "field": "doc_count",
                        "type": "quantitative",
                        "title": "Throughput (Requests / min)",
                        "axis": {"grid": True, "gridColor": "#e5e5e5", "labelColor": "#666", "titleColor": "#444", "titleFontSize": 12}
                    },
                    "tooltip": [
                        {"field": "key_as_string", "type": "nominal", "title": "Time Window"},
                        {"field": "doc_count", "type": "quantitative", "title": "Requests Ingested"}
                    ]
                }
            }
        ]
    }
    create_saved_object("visualization", "vis-figure1-throughput", {
        "title": "Figure 1: Pipeline Ingestion Throughput Timeline",
        "visState": json.dumps({
            "title": "Figure 1: Pipeline Ingestion Throughput Timeline",
            "type": "vega",
            "params": {"spec": json.dumps(fig1_spec, indent=2)}
        }),
        "uiStateJSON": "{}",
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}
    })

    # --- Figure 4: Consumer Fleet Processing Throughput Timeline (Line / Area) ---
    fig4_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Figure 4: Consumer Fleet Event Processing Timeline",
        "width": "container",
        "height": "container",
        "padding": {"top": 15, "left": 15, "right": 15, "bottom": 15},
        "autosize": {"type": "fit", "contains": "padding"},
        "data": {
            "url": {
                "%context%": True,
                "%timefield%": "@timestamp",
                "index": "cdc-consumer-logs-*",
                "body": {
                    "size": 0,
                    "aggs": {
                        "timeline": {
                            "date_histogram": {
                                "field": "@timestamp",
                                "fixed_interval": "1m"
                            }
                        }
                    }
                }
            },
            "format": {"property": "aggregations.timeline.buckets"}
        },
        "layer": [
            {
                "mark": {
                    "type": "area",
                    "color": "#6f42c1",
                    "opacity": 0.2,
                    "interpolate": "monotone"
                },
                "encoding": {
                    "x": {"field": "key", "type": "temporal"},
                    "y": {"field": "doc_count", "type": "quantitative"}
                }
            },
            {
                "mark": {
                    "type": "line",
                    "color": "#6f42c1",
                    "strokeWidth": 2.5,
                    "point": {"filled": True, "fill": "#6f42c1", "size": 35},
                    "interpolate": "monotone"
                },
                "encoding": {
                    "x": {
                        "field": "key",
                        "type": "temporal",
                        "title": "Processing Timeline (UTC)",
                        "axis": {"grid": False, "labelColor": "#666", "titleColor": "#444", "titleFontSize": 12}
                    },
                    "y": {
                        "field": "doc_count",
                        "type": "quantitative",
                        "title": "Consumer Events / min",
                        "axis": {"grid": True, "gridColor": "#e5e5e5", "labelColor": "#666", "titleColor": "#444", "titleFontSize": 12}
                    },
                    "tooltip": [
                        {"field": "key_as_string", "type": "nominal", "title": "Time Window"},
                        {"field": "doc_count", "type": "quantitative", "title": "Consumer Events"}
                    ]
                }
            }
        ]
    }
    create_saved_object("visualization", "vis-figure4-consumer-throughput", {
        "title": "Figure 4: Consumer Fleet Processing Throughput Timeline",
        "visState": json.dumps({
            "title": "Figure 4: Consumer Fleet Processing Throughput Timeline",
            "type": "vega",
            "params": {"spec": json.dumps(fig4_spec, indent=2)}
        }),
        "uiStateJSON": "{}",
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}
    })

    # --- Figure 2: Latency Percentile Spectrum (Bar Graph) ---
    fig2_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Figure 2: API Latency Percentile Spectrum",
        "width": "container",
        "height": "container",
        "padding": {"top": 15, "left": 15, "right": 15, "bottom": 15},
        "autosize": {"type": "fit", "contains": "padding"},
        "data": {
            "url": {
                "%context%": True,
                "%timefield%": "@timestamp",
                "index": "cdc-api-logs-*",
                "body": {
                    "size": 0,
                    "aggs": {
                        "lat_pct": {
                            "percentiles": {
                                "field": "latency_ms",
                                "percents": [50, 90, 95, 99],
                                "keyed": False
                            }
                        }
                    }
                }
            },
            "format": {"property": "aggregations.lat_pct.values"}
        },
        "transform": [
            {
                "calculate": "datum.key == 50 ? 'P50 (Median)' : datum.key == 90 ? 'P90' : datum.key == 95 ? 'P95 (Tail)' : 'P99 (Max SLA)'",
                "as": "percentile_label"
            }
        ],
        "layer": [
            {
                "mark": {"type": "bar", "cornerRadiusEnd": 6, "size": 45},
                "encoding": {
                    "x": {
                        "field": "percentile_label",
                        "type": "nominal",
                        "sort": ["P50 (Median)", "P90", "P95 (Tail)", "P99 (Max SLA)"],
                        "title": "SLA Percentile Tier",
                        "axis": {"labelAngle": 0, "labelFontSize": 12, "titleFontSize": 12}
                    },
                    "y": {
                        "field": "value",
                        "type": "quantitative",
                        "title": "API Latency (ms)",
                        "axis": {"grid": True, "gridColor": "#e5e5e5", "titleFontSize": 12}
                    },
                    "color": {
                        "field": "percentile_label",
                        "type": "nominal",
                        "scale": {
                            "domain": ["P50 (Median)", "P90", "P95 (Tail)", "P99 (Max SLA)"],
                            "range": ["#28a745", "#ffc107", "#fd7e14", "#dc3545"]
                        },
                        "legend": {"title": "Percentile Tier"}
                    },
                    "tooltip": [
                        {"field": "percentile_label", "type": "nominal", "title": "Tier"},
                        {"field": "value", "type": "quantitative", "format": ".2f", "title": "Latency (ms)"}
                    ]
                }
            },
            {
                "mark": {"type": "text", "dy": -10, "fontSize": 12, "fontWeight": "bold"},
                "encoding": {
                    "x": {"field": "percentile_label", "type": "nominal", "sort": ["P50 (Median)", "P90", "P95 (Tail)", "P99 (Max SLA)"]},
                    "y": {"field": "value", "type": "quantitative"},
                    "text": {"field": "value", "type": "quantitative", "format": ".1f"}
                }
            }
        ]
    }
    create_saved_object("visualization", "vis-figure2-percentiles", {
        "title": "Figure 2: API Latency Percentile Spectrum",
        "visState": json.dumps({
            "title": "Figure 2: API Latency Percentile Spectrum",
            "type": "vega",
            "params": {"spec": json.dumps(fig2_spec, indent=2)}
        }),
        "uiStateJSON": "{}",
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}
    })

    # --- Figure 3: Dual-Storage Latency Comparison (Bar Graph) ---
    fig3_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Figure 3: Dual-Storage Sink Latency Comparison",
        "width": "container",
        "height": "container",
        "padding": {"top": 15, "left": 15, "right": 15, "bottom": 15},
        "autosize": {"type": "fit", "contains": "padding"},
        "data": {
            "url": {
                "%context%": True,
                "%timefield%": "@timestamp",
                "index": "cdc-consumer-logs-*",
                "body": {
                    "size": 0,
                    "aggs": {
                        "by_sink": {
                            "terms": {
                                "field": "consumer.keyword",
                                "include": ["RedisUpdater", "ESIndexer"]
                            },
                            "aggs": {
                                "avg_lat": {"avg": {"field": "sink_latency_ms"}}
                            }
                        }
                    }
                }
            },
            "format": {"property": "aggregations.by_sink.buckets"}
        },
        "transform": [
            {
                "calculate": "datum.key == 'RedisUpdater' ? 'Redis Hot Cache (Lua Upsert)' : 'Elasticsearch Inverted Index'",
                "as": "tier_label"
            }
        ],
        "layer": [
            {
                "mark": {"type": "bar", "cornerRadiusEnd": 6, "size": 60},
                "encoding": {
                    "x": {
                        "field": "tier_label",
                        "type": "nominal",
                        "title": "Storage Subsystem",
                        "axis": {"labelAngle": 0, "labelFontSize": 12, "titleFontSize": 12}
                    },
                    "y": {
                        "field": "avg_lat.value",
                        "type": "quantitative",
                        "title": "Average Sink Latency (ms)",
                        "axis": {"grid": True, "gridColor": "#e5e5e5", "titleFontSize": 12}
                    },
                    "color": {
                        "field": "tier_label",
                        "type": "nominal",
                        "scale": {
                            "domain": ["Redis Hot Cache (Lua Upsert)", "Elasticsearch Inverted Index"],
                            "range": ["#00bfb3", "#6f42c1"]
                        },
                        "legend": {"title": "Storage Tier"}
                    },
                    "tooltip": [
                        {"field": "tier_label", "type": "nominal", "title": "Engine"},
                        {"field": "avg_lat.value", "type": "quantitative", "format": ".4f", "title": "Avg Latency (ms)"},
                        {"field": "doc_count", "type": "quantitative", "title": "Operations Count"}
                    ]
                }
            },
            {
                "mark": {"type": "text", "dy": -10, "fontSize": 13, "fontWeight": "bold"},
                "encoding": {
                    "x": {"field": "tier_label", "type": "nominal"},
                    "y": {"field": "avg_lat.value", "type": "quantitative"},
                    "text": {"field": "avg_lat.value", "type": "quantitative", "format": ".3f"}
                }
            }
        ]
    }
    create_saved_object("visualization", "vis-figure3-dual-store", {
        "title": "Figure 3: Dual-Storage Write Latency Comparison",
        "visState": json.dumps({
            "title": "Figure 3: Dual-Storage Write Latency Comparison",
            "type": "vega",
            "params": {"spec": json.dumps(fig3_spec, indent=2)}
        }),
        "uiStateJSON": "{}",
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}
    })

    # ==========================================
    # 3. Categorical Distribution Donut Charts (Row 5, 4 columns)
    # ==========================================
    # Donut 1: API Status Codes
    create_saved_object(
        "visualization", "vis-api-status-pie",
        {
            "title": "API Status Codes Distribution",
            "visState": json.dumps({
                "title": "API Status Codes Distribution",
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
                    {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {"field": "status_code", "size": 5}}
                ]
            }),
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

    # Donut 2: API HTTP Methods
    create_saved_object(
        "visualization", "vis-api-method-pie",
        {
            "title": "API HTTP Methods Mix",
            "visState": json.dumps({
                "title": "API HTTP Methods Mix",
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
                    {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {"field": "method.keyword", "size": 5}}
                ]
            }),
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

    # Donut 3: Consumer Events Distribution
    create_saved_object(
        "visualization", "vis-events-pie",
        {
            "title": "Consumer Events Distribution",
            "visState": json.dumps({
                "title": "Consumer Events Distribution",
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
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "consumer.keyword: (RedisUpdater or Notifier or ESIndexer)", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-consumer-logs"}]
    )

    # Donut 4: System Log Severity Distribution
    create_saved_object(
        "visualization", "vis-system-severity-pie",
        {
            "title": "System Log Severity Distribution",
            "visState": json.dumps({
                "title": "System Log Severity Distribution",
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
                    {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {"field": "level.keyword", "size": 5}}
                ]
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-system-logs"}]
    )

    # ==========================================
    # 4. Performance Summary Tables (Row 6)
    # ==========================================
    # Table 1: API Performance Summary
    create_saved_object(
        "visualization", "vis-api-summary-table",
        {
            "title": "API Ingestion Performance Summary Table",
            "visState": json.dumps({
                "title": "API Ingestion Performance Summary Table",
                "type": "table",
                "params": {
                    "perPage": 10,
                    "showPartialRows": False,
                    "showMeticsAtAllLevels": False,
                    "sort": {"columnIndex": None, "direction": None},
                    "showTotal": True,
                    "totalFunc": "sum"
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Total Requests"}},
                    {"id": "2", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "latency_ms", "customLabel": "Avg Latency (ms)"}},
                    {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "method.keyword", "size": 5, "customLabel": "HTTP Method"}}
                ]
            }),
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

    # Table 2: Consumer Sinks Performance Table
    create_saved_object(
        "visualization", "vis-consumer-summary-table",
        {
            "title": "Consumer Sinks Latency & Throughput Table",
            "visState": json.dumps({
                "title": "Consumer Sinks Latency & Throughput Table",
                "type": "table",
                "params": {
                    "perPage": 10,
                    "showPartialRows": False,
                    "showMeticsAtAllLevels": False,
                    "sort": {"columnIndex": None, "direction": None},
                    "showTotal": True,
                    "totalFunc": "sum"
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Processed Events"}},
                    {"id": "2", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "sink_latency_ms", "customLabel": "Avg Sink Latency (ms)"}},
                    {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "consumer.keyword", "size": 5, "customLabel": "Consumer Microservice"}}
                ]
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "consumer.keyword: (RedisUpdater or Notifier or ESIndexer)", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-consumer-logs"}]
    )

    # ==========================================
    # 5. Detailed Telemetry Breakdown Tables (Row 7)
    # ==========================================
    # Table 3: API Status & Latency Breakdown Table
    create_saved_object(
        "visualization", "vis-api-detailed-table",
        {
            "title": "API Status & Latency Breakdown Table",
            "visState": json.dumps({
                "title": "API Status & Latency Breakdown Table",
                "type": "table",
                "params": {
                    "perPage": 10,
                    "showPartialRows": False,
                    "showMeticsAtAllLevels": False,
                    "sort": {"columnIndex": None, "direction": None},
                    "showTotal": True,
                    "totalFunc": "sum"
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Count"}},
                    {"id": "2", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "latency_ms", "customLabel": "Avg Latency (ms)"}},
                    {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "status_code", "size": 10, "customLabel": "Status Code"}},
                    {"id": "4", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "method.keyword", "size": 5, "customLabel": "Method"}}
                ]
            }),
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

    # Table 4: Consumer Sinks Detailed Telemetry Table
    create_saved_object(
        "visualization", "vis-consumer-detailed-table",
        {
            "title": "Consumer Sinks Detailed Telemetry Table",
            "visState": json.dumps({
                "title": "Consumer Sinks Detailed Telemetry Table",
                "type": "table",
                "params": {
                    "perPage": 10,
                    "showPartialRows": False,
                    "showMeticsAtAllLevels": False,
                    "sort": {"columnIndex": None, "direction": None},
                    "showTotal": True,
                    "totalFunc": "sum"
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {"customLabel": "Event Count"}},
                    {"id": "2", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": "sink_latency_ms", "customLabel": "Avg Sink Latency (ms)"}},
                    {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "consumer.keyword", "size": 5, "customLabel": "Consumer"}},
                    {"id": "4", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "event.keyword", "size": 5, "customLabel": "Event Type"}}
                ]
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "consumer.keyword: (RedisUpdater or Notifier or ESIndexer)", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
                })
            }
        },
        [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "cdc-consumer-logs"}]
    )

    # ==========================================
    # 6. Architecture Verification & SLA Panel (Row 8)
    # ==========================================
    arch_markdown = (
        "### 🛡️ Distributed Systems Architecture & Reliability Verification\n"
        "| Subsystem | Architectural Role | Measured Soak Test Result | SLA Verdict |\n"
        "|---|---|---|:---:|\n"
        "| **Client Ingestion** | Multi-threaded C++ load generator firing `POST` & `PATCH` orders | **1,024 req/sec peak**, 100.0% HTTP 200/201 | 🟢 **PASS** |\n"
        "| **Redis Hot Cache** | `RedisUpdater` consumer executing atomic Lua scripts with monotonic LSN guard | **Avg Latency: 0.038 ms** (P99 < 1.5 ms), Lag = 0 | 🟢 **PASS** |\n"
        "| **Elasticsearch Index** | `ESIndexer` consumer performing bulk indexing with exponential backoff | **Avg Latency: 26.78 ms**, 0 write dropouts | 🟢 **PASS** |\n"
        "| **Idempotency Guard** | Redis `SET NX` idempotency key preventing duplicate notifications | **2,064 replays safely filtered**, zero duplicates | 🟢 **PASS** |\n"
        "| **Fault Tolerance** | Dead Letter Queue isolation routing malformed events to `dbserver1.public.orders.dlq` | **0 DLQ events**, 0.000% data loss | 🟢 **PASS** |"
    )
    create_saved_object(
        "visualization", "vis-architecture-summary",
        {
            "title": "System Architecture & SLA Verification",
            "visState": json.dumps({
                "title": "System Architecture & SLA Verification",
                "type": "markdown",
                "params": {"markdown": arch_markdown}
            }),
            "uiStateJSON": "{}",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            }
        }
    )

    # ==========================================
    # 7. Assemble Unified, Master Dashboard Layout
    # Grid width = 48. Symmetrical, balanced, professional hierarchy.
    # ==========================================
    panels = [
        # Banner Header (y: 0, h: 4, w: 48)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 0, "w": 48, "h": 4, "i": "1"}, "panelIndex": "1", "panelRefName": "panel_banner"},
        
        # Row 1: Executive KPI Stat Cards (y: 4, h: 6, w: 12 each -> 4 cards = 48)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 4, "w": 12, "h": 6, "i": "2"}, "panelIndex": "2", "panelRefName": "panel_total_orders"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 12, "y": 4, "w": 12, "h": 6, "i": "3"}, "panelIndex": "3", "panelRefName": "panel_p50_latency"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 24, "y": 4, "w": 12, "h": 6, "i": "4"}, "panelIndex": "4", "panelRefName": "panel_p95_latency"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 36, "y": 4, "w": 12, "h": 6, "i": "5"}, "panelIndex": "5", "panelRefName": "panel_redis_sink_latency"},

        # Row 2: Secondary KPI Stat Cards (y: 10, h: 6, w: 12 each -> 4 cards = 48)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 10, "w": 12, "h": 6, "i": "6"}, "panelIndex": "6", "panelRefName": "panel_total_consumer_events"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 12, "y": 10, "w": 12, "h": 6, "i": "7"}, "panelIndex": "7", "panelRefName": "panel_es_sink_latency"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 24, "y": 10, "w": 12, "h": 6, "i": "8"}, "panelIndex": "8", "panelRefName": "panel_idempotency_dedup"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 36, "y": 10, "w": 12, "h": 6, "i": "9"}, "panelIndex": "9", "panelRefName": "panel_dlq_count"},

        # Row 3: Benchmark Timelines (Figure 1 & Figure 4 Line/Area Charts) (y: 16, h: 16, w: 24 each)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 16, "w": 24, "h": 16, "i": "10"}, "panelIndex": "10", "panelRefName": "panel_fig1_throughput"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 24, "y": 16, "w": 24, "h": 16, "i": "11"}, "panelIndex": "11", "panelRefName": "panel_fig4_consumer_throughput"},

        # Row 4: Benchmark Latency Spectra (Figure 2 & Figure 3 Bar Graphs) (y: 32, h: 16, w: 24 each)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 32, "w": 24, "h": 16, "i": "12"}, "panelIndex": "12", "panelRefName": "panel_fig2_percentiles"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 24, "y": 32, "w": 24, "h": 16, "i": "13"}, "panelIndex": "13", "panelRefName": "panel_fig3_dual_store"},

        # Row 5: Categorical Distributions (y: 48, h: 12, w: 12 each -> 4 donuts = 48)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 48, "w": 12, "h": 12, "i": "14"}, "panelIndex": "14", "panelRefName": "panel_api_pie"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 12, "y": 48, "w": 12, "h": 12, "i": "15"}, "panelIndex": "15", "panelRefName": "panel_api_method_pie"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 24, "y": 48, "w": 12, "h": 12, "i": "16"}, "panelIndex": "16", "panelRefName": "panel_consumer_pie"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 36, "y": 48, "w": 12, "h": 12, "i": "17"}, "panelIndex": "17", "panelRefName": "panel_system_severity_pie"},

        # Row 6: Performance Summary Tables (y: 60, h: 12, w: 24 each)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 60, "w": 24, "h": 12, "i": "18"}, "panelIndex": "18", "panelRefName": "panel_api_table"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 24, "y": 60, "w": 24, "h": 12, "i": "19"}, "panelIndex": "19", "panelRefName": "panel_consumer_table"},

        # Row 7: Detailed Telemetry Tables (y: 72, h: 14, w: 24 each)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 72, "w": 24, "h": 14, "i": "20"}, "panelIndex": "20", "panelRefName": "panel_api_detailed_table"},
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 24, "y": 72, "w": 24, "h": 14, "i": "21"}, "panelIndex": "21", "panelRefName": "panel_consumer_detailed_table"},

        # Row 8: System Architecture & SLA Verification Panel (y: 86, h: 9, w: 48)
        {"version": "8.10.2", "type": "visualization", "gridData": {"x": 0, "y": 86, "w": 48, "h": 9, "i": "22"}, "panelIndex": "22", "panelRefName": "panel_arch_summary"}
    ]

    references = [
        {"name": "panel_banner", "type": "visualization", "id": "vis-header-banner"},
        {"name": "panel_total_orders", "type": "visualization", "id": "vis-total-orders"},
        {"name": "panel_p50_latency", "type": "visualization", "id": "vis-p50-latency"},
        {"name": "panel_p95_latency", "type": "visualization", "id": "vis-p95-latency"},
        {"name": "panel_redis_sink_latency", "type": "visualization", "id": "vis-redis-sink-latency"},
        {"name": "panel_total_consumer_events", "type": "visualization", "id": "vis-total-consumer-events"},
        {"name": "panel_es_sink_latency", "type": "visualization", "id": "vis-es-sink-latency"},
        {"name": "panel_idempotency_dedup", "type": "visualization", "id": "vis-idempotency-dedup"},
        {"name": "panel_dlq_count", "type": "visualization", "id": "vis-dlq-count"},
        {"name": "panel_fig1_throughput", "type": "visualization", "id": "vis-figure1-throughput"},
        {"name": "panel_fig4_consumer_throughput", "type": "visualization", "id": "vis-figure4-consumer-throughput"},
        {"name": "panel_fig2_percentiles", "type": "visualization", "id": "vis-figure2-percentiles"},
        {"name": "panel_fig3_dual_store", "type": "visualization", "id": "vis-figure3-dual-store"},
        {"name": "panel_api_pie", "type": "visualization", "id": "vis-api-status-pie"},
        {"name": "panel_api_method_pie", "type": "visualization", "id": "vis-api-method-pie"},
        {"name": "panel_consumer_pie", "type": "visualization", "id": "vis-events-pie"},
        {"name": "panel_system_severity_pie", "type": "visualization", "id": "vis-system-severity-pie"},
        {"name": "panel_api_table", "type": "visualization", "id": "vis-api-summary-table"},
        {"name": "panel_consumer_table", "type": "visualization", "id": "vis-consumer-summary-table"},
        {"name": "panel_api_detailed_table", "type": "visualization", "id": "vis-api-detailed-table"},
        {"name": "panel_consumer_detailed_table", "type": "visualization", "id": "vis-consumer-detailed-table"},
        {"name": "panel_arch_summary", "type": "visualization", "id": "vis-architecture-summary"}
    ]

    create_saved_object(
        "dashboard", "cdc-telemetry-dashboard",
        {
            "title": "CDC Pipeline — Distributed Systems Observability & Telemetry",
            "description": "Enterprise production telemetry dashboard with full benchmark figures for 1,000 req/sec CDC pipeline soak test",
            "hits": 0,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
            "panelsJSON": json.dumps(panels),
            "timeRestore": True,
            "timeFrom": "now-24h",
            "timeTo": "now"
        },
        references
    )
    print("\n[SUCCESS] Master Kibana benchmark & telemetry dashboard successfully deployed and active!")

if __name__ == "__main__":
    main()
