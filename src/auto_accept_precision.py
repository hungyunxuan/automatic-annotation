import math
import pandas as pd
from pathlib import Path
from yaml import safe_load

def load_cfg():
    return safe_load(Path("configs/config.yaml").read_text())

def wilson_ci(k, n, z=1.96):
    if n == 0:
        return (float("nan"), float("nan"))
    phat = k / n
    denom = 1 + z**2 / n
    centre = phat + z*z/(2*n)
    adj = z * math.sqrt((phat*(1-phat) + z*z/(4*n)) / n)
    lo = (centre - adj) / denom
    hi = (centre + adj) / denom
    return lo, hi

if __name__ == "__main__":
    cfg = load_cfg()
    auto = pd.read_parquet(Path(cfg["paths"]["labels"]) / "auto_accepted.parquet")
    ann_path = Path(cfg["paths"]["labels"]) / "annotations.parquet"
    if not ann_path.exists():
        print("No annotations found. Review some items first.")
        raise SystemExit(0)
    ann = pd.read_parquet(ann_path)

    # only audits that came from auto-accept sampling
    if "origin" in ann.columns:
        audited = ann[ann["origin"] == "auto_accept"].copy()
    else:
        # fallback: treat all annotations as audits (less precise)
        audited = ann.copy()

    if audited.empty:
        print("No audited auto-accepted items yet. Run src/shadow_audit_sample.py and review them.")
        raise SystemExit(0)

    # join on unique text to compare predicted vs final
    cols_needed = {"text","pred_label","final_label"}
    missing = cols_needed - set(audited.columns)
    if missing:
        raise SystemExit(f"Missing columns in annotations: {missing}")

    k = (audited["pred_label"] == audited["final_label"]).sum()
    n = len(audited)
    prec = k / n
    lo, hi = wilson_ci(k, n)

    print("=== AUTO-ACCEPT PRECISION (shadow audit) ===")
    print(f"audited samples: {n}")
    print(f"correct (match pred vs final): {k}")
    print(f"precision: {prec:.3f}  (95% CI: {lo:.3f}–{hi:.3f})")

    # quick label breakdown
    by_label = audited.assign(correct=(audited["pred_label"] == audited["final_label"])) \
                      .groupby("pred_label")["correct"].agg(["sum","count"])
    if not by_label.empty:
        print("\nBy predicted label:")
        for lbl, row in by_label.iterrows():
            c, cnt = int(row["sum"]), int(row["count"])
            p = c/cnt if cnt else float("nan")
            print(f"  {lbl}: {p:.3f}  ({c}/{cnt})")

