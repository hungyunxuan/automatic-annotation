# import os
# import time
# import hashlib
# import pandas as pd
# import streamlit as st
# from pathlib import Path
# from yaml import safe_load

# st.set_page_config(page_title="Sentiment Review Queue", layout="wide")

# def load_cfg():
#     return safe_load(Path("configs/config.yaml").read_text())

# def compute_item_id(row):
#     # Stable ID from text + model suggestion + rounded conf
#     key = f"{row.get('text','')}|{row.get('pred_label','')}|{round(float(row.get('pred_conf',0.0)),6)}"
#     return hashlib.sha1(key.encode("utf-8")).hexdigest()

# cfg = load_cfg()
# qpath = Path(cfg["paths"]["queues"]) / "review_queue.parquet"
# lpath = Path(cfg["paths"]["labels"]) / "annotations.parquet"

# st.title("Human-in-the-Loop Sentiment Review")

# # Load queue
# if not qpath.exists():
#     st.warning("No review queue found. Run prelabel + router first.")
#     st.stop()

# df = pd.read_parquet(qpath)

# # Session cap & reviewer identity
# SESSION_LIMIT = int(os.getenv("SESSION_LIMIT", "30"))
# reviewer = os.getenv("REVIEWER_NAME", "user")

# total_queue = len(df)
# df = df.head(SESSION_LIMIT)

# # Load annotations for metrics
# ann = pd.read_parquet(lpath) if lpath.exists() else pd.DataFrame()
# reviewed_all = len(ann)
# reviewed_you = len(ann[ann["reviewer"] == reviewer]) if not ann.empty and "reviewer" in ann.columns else 0

# # Ensure every queue row has item_id (persist to full queue once)
# if "item_id" not in df.columns and not df.empty:
#     df = df.copy()
#     df["item_id"] = df.apply(compute_item_id, axis=1)
#     full_q = pd.read_parquet(qpath)
#     if "item_id" not in full_q.columns and not full_q.empty:
#         full_q = full_q.copy()
#         full_q["item_id"] = full_q.apply(compute_item_id, axis=1)
#         full_q.to_parquet(qpath, index=False)

# # ----- session progress tracking (robust; no auto-increment here) -----
# new_total = min(SESSION_LIMIT, total_queue)

# # if queue size or cap changed, sync and clamp
# if st.session_state["session_total"] != new_total:
#     st.session_state["session_total"] = new_total
#     st.session_state["session_done"] = min(st.session_state["session_done"], new_total)

# # ✅ clamp only — do NOT increment here
# st.session_state["session_done"] = max(
#     0, min(int(st.session_state["session_done"]), int(st.session_state["session_total"]))
# )

# session_total = int(st.session_state["session_total"])
# session_done  = int(st.session_state["session_done"])

# # ✅ end-of-session gate
# if session_total > 0 and session_done >= session_total:
#     st.success("✅ Session complete. Increase `SESSION_LIMIT` or refresh to start a new session.")
#     st.stop()

# # # initialize once
# # if "session_total" not in st.session_state:
# #     st.session_state["session_total"] = new_total
# # if "session_done" not in st.session_state:
# #     st.session_state["session_done"] = 0
# # if "counted_ids" not in st.session_state:
# #     st.session_state["counted_ids"] = set()  # idempotent increment guard

# # # sync if total changed; clamp done
# # if st.session_state["session_total"] != new_total:
# #     st.session_state["session_total"] = new_total
# #     st.session_state["session_done"] = min(st.session_state["session_done"], new_total)
# #     # optional: reset counted_ids when a new session size is detected
# #     st.session_state["counted_ids"] = set()

# # session_total = int(st.session_state["session_total"])
# # session_done  = int(st.session_state["session_done"])

# # # If session finished, show message and stop
# # if session_total > 0 and session_done >= session_total:
# #     st.success("✅ Session complete. Increase `SESSION_LIMIT` or refresh to start a new session.")
# #     st.stop()

# # Top-line metrics + progress bar
# m1, m2, m3, m4 = st.columns(4)
# m1.metric("In queue (now)", total_queue)
# m2.metric(f"Reviewed ({reviewer})", reviewed_you)
# m3.metric("Reviewed (all)", reviewed_all)
# m4.metric("This session", f"{session_done}/{session_total}")

# ratio = 0.0 if session_total == 0 else min(max(session_done / session_total, 0.0), 1.0)
# st.progress(ratio)
# st.caption(f"Serving up to {session_total} items this session (set `SESSION_LIMIT` to change).")


# # Undo last action expander
# with st.expander("Undo last review", expanded=False):
#     la = st.session_state.get("last_action")
#     if not la:
#         st.caption("No action to undo.")
#     else:
#         preview = (la["row"].get("text") or "")[:120].replace("\n", " ")
#         st.markdown(f"**Last saved:** `{preview}`")
#         if st.button("Undo last save", type="secondary", key="undo_btn"):
#             # 1) Remove the annotation
#             if lpath.exists():
#                 ann_df = pd.read_parquet(lpath)
#                 if {"item_id", "ts"}.issubset(ann_df.columns):
#                     mask = ~((ann_df["item_id"] == la["item_id"]) & (ann_df["ts"] == la["ts"]))
#                     ann_df = ann_df[mask]
#                     ann_df.to_parquet(lpath, index=False)

#             # 2) Reinsert the row at the top of the queue
#             one = pd.DataFrame([la["row"]])
#             if "item_id" not in one.columns:
#                 one["item_id"] = one.apply(compute_item_id, axis=1)

#             if qpath.exists() and qpath.stat().st_size > 0:
#                 q = pd.read_parquet(qpath)

#                 # Harmonize columns
#                 for col in q.columns:
#                     if col not in one.columns:
#                         one[col] = None
#                 for col in one.columns:
#                     if col not in q.columns:
#                         q[col] = None

#                 q = pd.concat([one[q.columns], q], ignore_index=True)
#             else:
#                 q = one

#             q.to_parquet(qpath, index=False)

#             # 3) Update session progress & clear last_action
#             # st.session_state["session_done"] = max(0, st.session_state.get("session_done", 0) - 1)
#             # st.session_state["last_action"] = None

#             # 3) Update session progress & clear last_action
#             st.session_state["session_done"] = max(0, st.session_state.get("session_done", 0) - 1)
#             # also remove from counted_ids so it can be counted again
#             try:
#                 st.session_state["counted_ids"].remove(la["item_id"])
#             except Exception:
#                 pass
#             st.session_state["last_action"] = None

#             st.success("Undone. Reloading...")
#             st.rerun()

# # If nothing to review (after cap)
# if df.empty:
#     st.success("🎉 Review queue is empty for this session.")
#     st.stop()

# # Take first item
# row = df.iloc[0]

# # --------- FORM WITH UNIQUE KEYS ----------
# with st.form("review_form"):
#     st.subheader("Item")
#     st.write(row.get("text", ""))

#     st.subheader("Sentiment Suggestion")
#     pred_label = row.get("pred_label")
#     pred_conf = float(row.get("pred_conf") or 0.0)
#     st.write(f"Predicted: **{pred_label}**  |  Confidence: **{pred_conf:.3f}**")

#     label_choice = st.selectbox(
#         "Select final label",
#         ["POSITIVE", "NEGATIVE"],
#         index=0 if pred_label == "POSITIVE" else 1,
#         key="label_choice"
#     )
#     rationale = st.text_input(
#         "Optional rationale tags (comma-separated)",
#         key="rationale"
#     )

#     submit = st.form_submit_button("Save & Next", type="primary", use_container_width=True)

# if submit:
#     # Keep any extra columns from the row
#     base = row.to_dict()
#     if "item_id" not in base:
#         base["item_id"] = compute_item_id(row)

#     base.update({
#         "ts": int(time.time()),
#         "final_label": label_choice,
#         "reviewer": reviewer,
#         "model": cfg["model"]["name"],
#         "policy_version": "v1-sentiment",
#         "rationale": rationale,
#         "origin": base.get("origin", "review"),
#     })
#     out = pd.DataFrame([base])

#     # Append to annotations
#     if lpath.exists():
#         pd.concat([pd.read_parquet(lpath), out], ignore_index=True).to_parquet(lpath, index=False)
#     else:
#         out.to_parquet(lpath, index=False)

#     # Drop reviewed row from queue and persist
#     df_remaining = df.iloc[1:].reset_index(drop=True)

#     # Also drop it from the full queue file (not just session slice)
#     full_q = pd.read_parquet(qpath)
#     if "item_id" in full_q.columns and "item_id" in out.columns:
#         full_q = full_q[full_q["item_id"] != out.iloc[0]["item_id"]]
#     else:
#         # fallback: drop by exact text match
#         full_q = full_q[full_q["text"] != row.get("text")]
#     full_q.to_parquet(qpath, index=False)

#     # Session progress + store undo info
#     # st.session_state["session_done"] = st.session_state.get("session_done", 0) + 1
#     # st.session_state["last_action"] = {
#     #     "ts": base["ts"],
#     #     "item_id": base["item_id"],
#     #     "row": {k: base.get(k) for k in full_q.columns.union(out.columns)}
#     # }

#     # Session progress + store undo info (idempotent)
#     item_id_for_count = base["item_id"]
#     if item_id_for_count not in st.session_state["counted_ids"]:
#         st.session_state["session_done"] = min(
#             st.session_state.get("session_done", 0) + 1,
#             st.session_state.get("session_total", 0)
#         )
#         st.session_state["counted_ids"].add(item_id_for_count)

#     st.success("Saved. Reloading...")
#     st.rerun()

import os
import time
import hashlib
import pandas as pd
import streamlit as st
from pathlib import Path
from yaml import safe_load

st.set_page_config(page_title="Sentiment Review Queue", layout="wide")

def load_cfg():
    return safe_load(Path("configs/config.yaml").read_text())

def compute_item_id(row):
    # Stable ID from text + model suggestion + rounded conf
    key = f"{row.get('text','')}|{row.get('pred_label','')}|{round(float(row.get('pred_conf',0.0)),6)}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

cfg = load_cfg()
qpath = Path(cfg["paths"]["queues"]) / "review_queue.parquet"
lpath = Path(cfg["paths"]["labels"]) / "annotations.parquet"

st.title("Human-in-the-Loop Sentiment Review")

# Load queue
if not qpath.exists():
    st.warning("No review queue found. Run prelabel + router first.")
    st.stop()

df = pd.read_parquet(qpath)

# Session cap & reviewer identity
SESSION_LIMIT = int(os.getenv("SESSION_LIMIT", "30"))
reviewer = os.getenv("REVIEWER_NAME", "user")

total_queue = len(df)
df = df.head(SESSION_LIMIT)

# Load annotations for metrics
ann = pd.read_parquet(lpath) if lpath.exists() else pd.DataFrame()
reviewed_all = len(ann)
reviewed_you = len(ann[ann["reviewer"] == reviewer]) if not ann.empty and "reviewer" in ann.columns else 0

# Ensure every queue row has item_id
if "item_id" not in df.columns and not df.empty:
    df = df.copy()
    df["item_id"] = df.apply(compute_item_id, axis=1)
    # persist item_id to queue so Undo/Edit can work even after reload
    full_q = pd.read_parquet(qpath)
    if "item_id" not in full_q.columns and not full_q.empty:
        full_q = full_q.copy()
        full_q["item_id"] = full_q.apply(compute_item_id, axis=1)
        full_q.to_parquet(qpath, index=False)

# Initialize session progress once
if "session_total" not in st.session_state:
    st.session_state["session_total"] = min(SESSION_LIMIT, total_queue)
if "session_done" not in st.session_state:
    st.session_state["session_done"] = 0

session_total = st.session_state["session_total"]
session_done = st.session_state["session_done"]

# End-of-session gate: show message and stop when cap hit
if session_total > 0 and session_done >= session_total:
    st.success("✅ Session complete. Increase `SESSION_LIMIT` or refresh to start a new session.")
    st.stop()


# Top-line metrics + progress bar
m1, m2, m3, m4 = st.columns(4)
m1.metric("In queue (now)", total_queue)
m2.metric(f"Reviewed ({reviewer})", reviewed_you)
m3.metric("Reviewed (all)", reviewed_all)
m4.metric("This session", f"{session_done}/{session_total}")
st.progress(0 if session_total == 0 else session_done / session_total)
st.caption(f"Serving up to {session_total} items this session "
           f"(set env var `SESSION_LIMIT` to change).")

# Undo last action expander
with st.expander("Undo last review", expanded=False):
    la = st.session_state.get("last_action")
    if not la:
        st.caption("No action to undo.")
    else:
        preview = (la["row"].get("text") or "")[:120].replace("\n", " ")
        st.markdown(f"**Last saved:** `{preview}`")
        if st.button("Undo last save", type="secondary", key="undo_btn"):
            # 1) Remove the annotation
            if lpath.exists():
                ann_df = pd.read_parquet(lpath)
                if {"item_id", "ts"}.issubset(ann_df.columns):
                    mask = ~((ann_df["item_id"] == la["item_id"]) & (ann_df["ts"] == la["ts"]))
                    ann_df = ann_df[mask]
                    ann_df.to_parquet(lpath, index=False)

            # 2) Reinsert the row at the top of the queue
            one = pd.DataFrame([la["row"]])
            if "item_id" not in one.columns:
                one["item_id"] = one.apply(compute_item_id, axis=1)

            if qpath.exists() and qpath.stat().st_size > 0:
                q = pd.read_parquet(qpath)

                # Harmonize columns
                for col in q.columns:
                    if col not in one.columns:
                        one[col] = None
                for col in one.columns:
                    if col not in q.columns:
                        q[col] = None

                q = pd.concat([one[q.columns], q], ignore_index=True)
            else:
                q = one

            q.to_parquet(qpath, index=False)

            # 3) Update session progress & clear last_action
            st.session_state["session_done"] = max(0, st.session_state.get("session_done", 0) - 1)
            st.session_state["last_action"] = None

            st.success("Undone. Reloading...")
            st.rerun()

# If nothing to review (after cap)
if df.empty:
    st.success("🎉 Review queue is empty for this session.")
    st.stop()

# Take first item
row = df.iloc[0]

# --------- FORM WITH UNIQUE KEYS ----------
with st.form("review_form"):
    st.subheader("Item")
    st.write(row.get("text", ""))

    st.subheader("Sentiment Suggestion")
    pred_label = row.get("pred_label")
    pred_conf = float(row.get("pred_conf") or 0.0)
    st.write(f"Predicted: **{pred_label}**  |  Confidence: **{pred_conf:.3f}**")

    label_choice = st.selectbox(
        "Select final label",
        ["POSITIVE", "NEGATIVE"],
        index=0 if pred_label == "POSITIVE" else 1,
        key="label_choice"
    )
    rationale = st.text_input(
        "Optional rationale tags (comma-separated)",
        key="rationale"
    )

    submit = st.form_submit_button("Save & Next", type="primary", use_container_width=True)

if submit:
    # Keep any extra columns from the row
    base = row.to_dict()
    if "item_id" not in base:
        base["item_id"] = compute_item_id(row)

    base.update({
        "ts": int(time.time()),
        "final_label": label_choice,
        "reviewer": reviewer,
        "model": cfg["model"]["name"],
        "policy_version": "v1-sentiment",
        "rationale": rationale,
        "origin": base.get("origin", "review"),
    })
    out = pd.DataFrame([base])

    # Append to annotations
    if lpath.exists():
        pd.concat([pd.read_parquet(lpath), out], ignore_index=True).to_parquet(lpath, index=False)
    else:
        out.to_parquet(lpath, index=False)

    # Drop reviewed row from queue and persist
    df_remaining = df.iloc[1:].reset_index(drop=True)

    # Also drop it from the full queue file (not just session slice)
    full_q = pd.read_parquet(qpath)
    if "item_id" in full_q.columns and "item_id" in out.columns:
        full_q = full_q[full_q["item_id"] != out.iloc[0]["item_id"]]
    else:
        # fallback: drop by exact text match
        full_q = full_q[full_q["text"] != row.get("text")]
    full_q.to_parquet(qpath, index=False)

    # Session progress + store undo info
    # st.session_state["session_done"] = st.session_state.get("session_done", 0) + 1
    st.session_state["session_done"] = min(
    st.session_state.get("session_done", 0) + 1,
    st.session_state.get("session_total", 0)
)
    st.session_state["last_action"] = {
        "ts": base["ts"],
        "item_id": base["item_id"],
        "row": {k: base.get(k) for k in full_q.columns.union(out.columns)}
    }

    st.success("Saved. Reloading...")
    st.rerun()
