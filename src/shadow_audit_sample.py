import sys
import pandas as pd
from pathlib import Path
from yaml import safe_load

def load_cfg():
    return safe_load(Path("configs/config.yaml").read_text())

if __name__ == "__main__":
    cfg = load_cfg()
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3  # default sample size = 3

    auto_path = Path(cfg["paths"]["labels"]) / "auto_accepted.parquet"
    qpath = Path(cfg["paths"]["queues"]) / "review_queue.parquet"
    ann_path = Path(cfg["paths"]["labels"]) / "annotations.parquet"

    if not auto_path.exists():
        raise FileNotFoundError(f"Missing {auto_path} — run prelabel + router first.")

    auto = pd.read_parquet(auto_path)

    # Exclude items already reviewed to avoid repeats
    already_texts = set()
    if ann_path.exists():
        ann = pd.read_parquet(ann_path)
        if "text" in ann.columns:
            already_texts |= set(ann["text"].dropna().unique())

    # Also exclude anything already in the review queue
    if qpath.exists():
        q = pd.read_parquet(qpath)
        in_queue = set(q.get("text", pd.Series(dtype=str)).dropna().unique())
    else:
        q = pd.DataFrame()
        in_queue = set()

    remaining = auto[~auto["text"].isin(already_texts | in_queue)].copy()
    if remaining.empty:
        print("No new auto-accepted items available for audit.")
        raise SystemExit(0)
    
    remaining = remaining.sort_values("pred_conf", ascending=True)
    k = min(n, len(remaining))

    sample = remaining.head(k).copy()
    sample["origin"] = "auto_accept"

    out = pd.concat([q, sample], ignore_index=True)
    qpath.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(qpath, index=False)
    print(f"Queued {k} auto-accepted items for shadow audit → {qpath}")

