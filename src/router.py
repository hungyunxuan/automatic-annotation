import pandas as pd
from pathlib import Path
from yaml import safe_load
import time

def load_config():
    return safe_load(Path("configs/config.yaml").read_text())

if __name__ == "__main__":
    cfg = load_config()
    processed_dir = Path(cfg["paths"]["processed"])
    queues_dir = Path(cfg["paths"]["queues"])
    labels_dir = Path(cfg["paths"]["labels"])

    pre_path = processed_dir / "prelabels.parquet"
    if not pre_path.exists():
        raise FileNotFoundError(f"Missing {pre_path}. Run model_prelabel.py first.")

    df = pd.read_parquet(pre_path)

    t1 = float(cfg["routing"]["tau_auto_accept"])
    t2 = float(cfg["routing"]["tau_review"])

    auto = df[df["pred_conf"] >= t1].copy()
    review = df[(df["pred_conf"] < t1) & (df["pred_conf"] >= t2)].copy()
    hard = df[df["pred_conf"] < t2].copy()

    labels_dir.mkdir(parents=True, exist_ok=True)
    queues_dir.mkdir(parents=True, exist_ok=True)

    auto.to_parquet(labels_dir / "auto_accepted.parquet", index=False)
    review.to_parquet(queues_dir / "review_queue.parquet", index=False)
    hard.to_parquet(queues_dir / "hardcase_queue.parquet", index=False)

    # simple audit log
    audit_row = pd.DataFrame([{
        "ts": int(time.time()),
        "source": "router",
        "model": cfg["model"]["name"],
        "tau_auto_accept": t1,
        "tau_review": t2,
        "auto_count": len(auto),
        "review_count": len(review),
        "hard_count": len(hard),
    }])
    audit_path = labels_dir / "audit_log.parquet"
    if audit_path.exists():
        pd.concat([pd.read_parquet(audit_path), audit_row], ignore_index=True)\
          .to_parquet(audit_path, index=False)
    else:
        audit_row.to_parquet(audit_path, index=False)

    print(f"Routed: auto={len(auto)} review={len(review)} hard={len(hard)}")

