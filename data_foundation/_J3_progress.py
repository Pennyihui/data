# -*- coding: utf-8 -*-
"""_J3_progress.py — 紧凑进度读取"""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
st = json.load(open("_J3_state.json", encoding="utf-8"))
targets = [s.strip() for s in open("_J3_targets.csv", encoding="utf-8")
           if s.strip() and not s.startswith("symbol")]
done4 = sum(1 for s in targets if len(st.get("done", {}).get(s, [])) == 4)
done_any = len([s for s in targets if st.get("done", {}).get(s)])
fails = st.get("failures", [])
print(f"done4: {done4}/{len(targets)} | done_any: {done_any} | failures: {len(fails)}")
acc = st.get("accum", {})
for ds, s in acc.items():
    print(f"  {ds}: rows={s['row_count']:,} suspect={s['suspect_count']} "
          f"{str(s['coverage_start'])[:10]} ~ {str(s['coverage_end'])[:10]}")
for f in fails[-10:]:
    print("  FAIL", f["symbol"], f["dataset"], f["error"][:80])
