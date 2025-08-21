import pandas as pd
from pathlib import Path
from yaml import safe_load

def load_cfg():
    return safe_load(Path("configs/config.yaml").read_text())

def safe_read(path):
    p = Path(path)
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()

if __name__ == "__main__":
    cfg = load_cfg()
    auto = safe_read(Path(cfg["paths"]["labels"]) / "auto_accepted.parquet")
    reviewed = safe_read(Path(cfg["paths"]["labels"]) / "annotations.parquet")
    hard = safe_read(Path(cfg["paths"]["queues"]) / "hardcase_queue.parquet")
    audit = safe_read(Path(cfg["paths"]["labels"]) / "audit_log.parquet")
    gold = Path(cfg["paths"]["policy"]) / "goldset.csv"
    gold_df = pd.read_csv(gold) if gold.exists() else pd.DataFrame()

    print("=== COUNTS ===")
    print(f"auto_accepted: {len(auto)}")
    print(f"reviewed:      {len(reviewed)}")
    print(f"hard_case:     {len(hard)}")
    print(f"audit_rows:    {len(audit)}")

    if not reviewed.empty and {"pred_label","final_label"}.issubset(reviewed.columns):
        agree = (reviewed["pred_label"] == reviewed["final_label"]).mean()
        print("\n=== REVIEW AGREEMENT ===")
        print(f"model-vs-reviewer agreement: {agree:.3f}")
    else:
        print("\n=== REVIEW AGREEMENT ===")
        print("Insufficient reviewed items to compute agreement.")

    print("\n=== AUTO-ACCEPT PRECISION (needs audits) ===")
    print("To estimate precision, sample some auto_accepted items for human audit.")

    if not gold_df.empty:
        print("\n=== GOLD SET ===")
        print(f"gold items: {len(gold_df)} (use for reviewer calibration / IAA)")
