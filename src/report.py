import math, json
import pandas as pd
from pathlib import Path
from yaml import safe_load
from datetime import datetime

def load_cfg(): return safe_load(Path("configs/config.yaml").read_text())
def safe_parquet(p): return pd.read_parquet(p) if Path(p).exists() else pd.DataFrame()

def wilson_ci(k, n, z=1.96):
    if n == 0: return (float("nan"), float("nan"))
    phat = k/n; denom = 1 + z*z/n
    centre = phat + z*z/(2*n)
    adj = z*math.sqrt((phat*(1-phat) + z*z/(4*n)) / n)
    lo = (centre - adj)/denom; hi = (centre + adj)/denom
    return lo, hi

if __name__ == "__main__":
    cfg = load_cfg()
    labels = Path(cfg["paths"]["labels"]); queues = Path(cfg["paths"]["queues"])
    auto = safe_parquet(labels / "auto_accepted.parquet")
    ann  = safe_parquet(labels / "annotations.parquet")
    hard = safe_parquet(queues / "hardcase_queue.parquet")
    revq = safe_parquet(queues / "review_queue.parquet")
    pre  = safe_parquet(Path(cfg["paths"]["processed"]) / "prelabels.parquet")

    # Agreement on reviewed items (model vs final)
    agree = None
    if not ann.empty and {"pred_label","final_label"}.issubset(ann.columns):
        agree = float((ann["pred_label"] == ann["final_label"]).mean())

    # Auto-accept precision from shadow audits (origin==auto_accept)
    audited = ann[ann["origin"]=="auto_accept"] if "origin" in ann.columns else ann
    k = int((audited.get("pred_label")==audited.get("final_label")).sum()) if not audited.empty else 0
    n = int(len(audited))
    prec = (k/n) if n else None
    lo, hi = wilson_ci(k, n) if n else (None, None)

    # Confidence quantiles to justify threshold
    conf_q = {}
    if not pre.empty:
        q = pre["pred_conf"].quantile([0.95,0.97,0.98,0.99,0.995,0.999])
        conf_q = {str(i): float(v) for i,v in q.items()}

    out = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "counts": {
            "prelabels": int(len(pre)),
            "auto_accepted": int(len(auto)),
            "review_queue": int(len(revq)),
            "hardcase_queue": int(len(hard)),
            "annotations": int(len(ann)),
            "audited_auto_accept": n
        },
        "agreement_model_vs_reviewer": agree,
        "auto_accept_precision": prec,
        "auto_accept_precision_ci95": [lo, hi],
        "thresholds": {
            "tau_auto_accept": float(cfg["routing"]["tau_auto_accept"]),
            "tau_review": float(cfg["routing"]["tau_review"])
        },
        "confidence_quantiles": conf_q
    }

    Path("reports").mkdir(exist_ok=True)
    Path("reports/metrics.json").write_text(json.dumps(out, indent=2))

        # Write a concise README section (use safe strings for None values)
    agree_s = f"{agree:.3f}" if agree is not None else "n/a"
    if prec is not None:
        prec_s = f"{prec:.3f}"
        lo_s   = f"{lo:.3f}" if lo is not None else "n/a"
        hi_s   = f"{hi:.3f}" if hi is not None else "n/a"
        ci_s   = f"{lo_s}–{hi_s}"
    else:
        prec_s = "n/a"
        ci_s   = "n/a"

    md = f"""# Auto-Annotation (Sentiment) — Results Snapshot

**Data:** {len(pre)} texts  
**Routing thresholds:** `tau_auto_accept={out['thresholds']['tau_auto_accept']}`, `tau_review={out['thresholds']['tau_review']}`

## Current Counts
- Auto-accepted: **{len(auto)}**
- Review queue: **{len(revq)}**
- Hard-case queue: **{len(hard)}**
- Reviewed (total): **{len(ann)}**
- Audited auto-accepts (subset): **{n}**

## Quality
- Model vs reviewer agreement (on all reviewed): **{agree_s}**
- Auto-accept precision (shadow audit): **{prec_s}** (95% CI: **{ci_s}**)

## Threshold justification
Confidence quantiles (pred_conf): `{conf_q}`

*Generated {out['timestamp']}*
"""
    Path("reports").mkdir(exist_ok=True)
    Path("reports/README_results.md").write_text(md)
    print("Wrote reports/metrics.json and reports/README_results.md")
