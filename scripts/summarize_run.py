#!/usr/bin/env python3
"""Print the key facts from a pipeline run log (the JSON summary the driver prints last)."""
import json
import sys

text = open(sys.argv[1]).read()
start = text.rfind('{\n  "run_id"')
if start < 0:
    sys.exit("no run summary found in log")
s = json.loads(text[start: text.find("\n}", start) + 2])
q = s.get("resolution", {}).get("quality", {})
print(f"  status={s['status']}  duration={s.get('duration_s')}s  sources={s.get('source_rows')}")
print(f"  entity resolution: {s['resolution']['golden_records']} golden records, "
      f"precision={q.get('pairwise_precision')} recall={q.get('pairwise_recall')}")
ice = s.get("iceberg", {})
print(f"  iceberg upsert: inserted={ice.get('rows_inserted')} updated={ice.get('rows_updated')} "
      f"snapshots_total={ice.get('snapshots_total')}")
m = s.get("model", {})
print(f"  model: r2(log revenue)={m.get('r2_log_revenue')} median APE={m.get('mape')} rounds={m.get('boosted_rounds')}")
print(f"  predictions published: {s.get('predictions', {}).get('rows')}")
