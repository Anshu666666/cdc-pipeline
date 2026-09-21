import urllib.request
import json
import time

KIBANA_URL = "http://localhost:5601"

def create_data_view(view_id, title, name, time_field="@timestamp"):
    url = f"{KIBANA_URL}/api/data_views/data_view"
    dv = {
        "id": view_id,
        "title": title,
        "name": name
    }
    if time_field:
        dv["timeFieldName"] = time_field
    payload = {"data_view": dv}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "kbn-xsrf": "true"
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"[SUCCESS] Created Data View: {name} ({title}) -> {resp.status}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if e.code == 409 or "already exists" in body or "duplicate" in body:
            print(f"[INFO] Data View {name} already exists.")
        else:
            print(f"[ERROR] Failed to create {name}: HTTP {e.code}: {body}")

def main():
    print("Connecting to Kibana to configure Data Views...")
    views = [
        ("cdc-api-logs", "cdc-api-logs-*", "CDC API Logs", "@timestamp"),
        ("cdc-consumer-logs", "cdc-consumer-logs-*", "CDC Consumer Logs", "@timestamp"),
        ("cdc-system-logs", "cdc-system-logs-*", "CDC System Logs", "@timestamp"),
        ("orders", "orders", "Elasticsearch Orders Store", None)
    ]
    for vid, title, name, tfield in views:
        create_data_view(vid, title, name, tfield)

if __name__ == "__main__":
    main()
