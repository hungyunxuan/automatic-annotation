import pandas as pd
from pathlib import Path
from yaml import safe_load
from transformers import pipeline

def load_config():
    return safe_load(Path("configs/config.yaml").read_text())

def map_label(hf_label: str) -> str:
    # Map model's sentiment labels to your policy labels
    return "SAFE" if "POSITIVE" in hf_label.upper() else "ABUSIVE"

if __name__ == "__main__":
    cfg = load_config()
    processed_dir = Path(cfg["paths"]["processed"])
    in_path = processed_dir / "to_prelabel.csv"
    out_path = processed_dir / "prelabels.parquet"

    df = pd.read_csv(in_path)
    df = df.dropna(subset=["text"]).reset_index(drop=True)

    clf = pipeline(
        task="text-classification",
        model=cfg["model"]["name"],
        top_k=None,
        truncation=True
    )

    preds = clf(df["text"].tolist())
    top = [sorted(p, key=lambda x: x["score"], reverse=True)[0] for p in preds]

    df["pred_label_raw"] = [t["label"] for t in top]
    df["pred_conf"] = [float(t["score"]) for t in top]
    df["pred_label"] = [map_label(t["label"]) for t in top]

    df.to_parquet(out_path, index=False)
    print(f"Saved prelabels to {out_path}")

