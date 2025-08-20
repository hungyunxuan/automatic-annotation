import streamlit as st
import pandas as pd
from pathlib import Path
from yaml import safe_load
import time

st.set_page_config(page_title="Review Queue", layout="wide")

def load_cfg():
    return safe_load(Path("configs/config.yaml").read_text())

cfg = load_cfg()
qpath = Path(cfg["paths"]["queues"]) / "review_queue.parquet"
lpath = Path(cfg["paths"]["labels"]) / "annotations.parquet"

st.title("Human-in-the-Loop Review")

if not qpath.exists():
    st.warning("No review queue found. Run prelabel + router first.")
    st.stop()

df = pd.read_parquet(qpath)

if df.empty:
    st.success("🎉 Review queue is empty.")
    st.stop()

# take first item for simplicity
row = df.iloc[0]
st.subheader("Item")
st.write(row.get("text", ""))

st.subheader("Model Suggestion")
st.write(f"Predicted: **{row.get('pred_label')}**  |  Confidence: **{row.get('pred_conf'):.3f}**")

label_choice = st.selectbox("Select final label", ["SAFE","ABUSIVE"],
                            index=0 if row.get("pred_label")=="SAFE" else 1)
rationale = st.text_input("Optional rationale tags (comma-separated)")

if st.button("Save & Next"):
    out = pd.DataFrame([{
        "ts": int(time.time()),
        "text": row.get("text"),
        "pred_label": row.get("pred_label"),
        "pred_conf": row.get("pred_conf"),
        "final_label": label_choice,
        "reviewer": "user",
        "model": cfg["model"]["name"],
        "policy_version": "v1",
        "rationale": rationale
    }])

    # append to annotations
    if lpath.exists():
        pd.concat([pd.read_parquet(lpath), out], ignore_index=True).to_parquet(lpath, index=False)
    else:
        out.to_parquet(lpath, index=False)

    # drop reviewed row from queue
    df_remaining = df.iloc[1:].reset_index(drop=True)
    df_remaining.to_parquet(qpath, index=False)

    st.success("Saved. Reloading...")
    st.experimental_rerun()

