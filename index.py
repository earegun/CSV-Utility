"""
Email Suite — unified CSV email processor + cross-file deduplicator
=======================================================================
Tool 1 · Email Expander
  - Upload one CSV → extract emails from up to 4 source columns
  - Each unique email gets its own row
  - Verified flag, duplicate flag, source column tracked
  - Search/filter preview, download full / verified / duplicates / filtered

Tool 2 · Bulk Deduplicator
  - Upload many CSVs → cross-file dedup on PRIMARY_EMAIL
  - Two-pass: (1) build global best-row index, (2) strip dupes per file
  - "Richest" row wins (fewest nulls, longest text)
  - Download ZIP of all deduplicated files
"""

import io
import re
import zipfile

import pandas as pd
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Email Suite",
    page_icon="✉️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
#  GLOBAL CSS  — dark ash background, green accent, white text
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    font-size: 14px;
}
.stApp {
    background: #111312;
    color: #e8f0e9;
}
.main .block-container {
    padding-top: 24px;
    padding-bottom: 48px;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #0d0f0e;
    border-right: 1px solid #1e2d20;
}
section[data-testid="stSidebar"] * { color: #9ab89d !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: #4ade80 !important;
    font-weight: 600 !important;
    letter-spacing: -0.3px;
}
section[data-testid="stSidebar"] hr { border-color: #1e2d20 !important; }

/* ── Headings ── */
h1, h2, h3, h4 { color: #e8f0e9 !important; }

/* ── Hero banner ── */
.hero {
    background: linear-gradient(135deg, #0d1f10 0%, #162b1a 60%, #0d1f10 100%);
    border: 1px solid #2d5230;
    border-radius: 14px;
    padding: 30px 36px 26px;
    margin-bottom: 28px;
    position: relative;
    overflow: hidden;
}
.hero::before {
    content: '';
    position: absolute;
    top: -60px; right: -60px;
    width: 260px; height: 260px;
    background: radial-gradient(circle, rgba(74,222,128,0.08) 0%, transparent 65%);
    border-radius: 50%;
}
.hero::after {
    content: '';
    position: absolute;
    bottom: -40px; left: 40px;
    width: 160px; height: 160px;
    background: radial-gradient(circle, rgba(74,222,128,0.05) 0%, transparent 65%);
    border-radius: 50%;
}
.hero-badge {
    display: inline-block;
    background: rgba(74,222,128,0.12);
    border: 1px solid rgba(74,222,128,0.3);
    color: #4ade80;
    font-size: 0.72rem;
    font-family: 'JetBrains Mono', monospace;
    padding: 3px 10px;
    border-radius: 20px;
    margin-bottom: 10px;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}
.hero-title {
    font-size: 1.75rem;
    font-weight: 700;
    color: #ffffff;
    margin: 0 0 6px 0;
    letter-spacing: -0.6px;
    line-height: 1.2;
}
.hero-sub {
    color: #6b9970;
    font-size: 0.875rem;
    margin: 0;
    line-height: 1.5;
}

/* ── Stat cards ── */
.stat-card {
    background: #161a17;
    border: 1px solid #1e2d20;
    border-radius: 12px;
    padding: 18px 16px;
    text-align: center;
    transition: border-color 0.2s, transform 0.15s;
}
.stat-card:hover { border-color: #2d5230; transform: translateY(-2px); }
.stat-number {
    font-size: 1.9rem;
    font-weight: 700;
    color: #4ade80;
    line-height: 1;
    font-family: 'JetBrains Mono', monospace;
    letter-spacing: -1px;
}
.stat-label {
    font-size: 0.72rem;
    color: #5a7a5d;
    margin-top: 6px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    font-weight: 500;
}
.stat-card.white  .stat-number { color: #e8f0e9; }
.stat-card.green  .stat-number { color: #4ade80; }
.stat-card.yellow .stat-number { color: #facc15; }
.stat-card.red    .stat-number { color: #f87171; }
.stat-card.dim    .stat-number { color: #6b9970; }

/* ── Section headers ── */
.section-header {
    font-size: 0.75rem;
    font-weight: 600;
    color: #4ade80;
    margin: 32px 0 14px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid #1e2d20;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    font-family: 'JetBrains Mono', monospace;
}

/* ── Message boxes ── */
.info-box {
    background: #0d1a0f;
    border: 1px solid #1e2d20;
    border-left: 3px solid #4ade80;
    border-radius: 8px;
    padding: 11px 15px;
    font-size: 0.875rem;
    color: #9ab89d;
    margin: 10px 0;
}
.warn-box {
    background: #1a1500;
    border: 1px solid #3d3000;
    border-left: 3px solid #facc15;
    border-radius: 8px;
    padding: 11px 15px;
    font-size: 0.875rem;
    color: #b89d4d;
    margin: 10px 0;
}
.success-box {
    background: #0a1f0d;
    border: 1px solid #1e4020;
    border-left: 3px solid #4ade80;
    border-radius: 8px;
    padding: 11px 15px;
    font-size: 0.875rem;
    color: #4ade80;
    margin: 10px 0;
}
.danger-box {
    background: #1f0a0a;
    border: 1px solid #3d1010;
    border-left: 3px solid #f87171;
    border-radius: 8px;
    padding: 11px 15px;
    font-size: 0.875rem;
    color: #f87171;
    margin: 10px 0;
}

/* ── Step badges ── */
.step-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: #1a3d1e;
    border: 1px solid #2d5230;
    color: #4ade80 !important;
    border-radius: 50%;
    width: 22px; height: 22px;
    font-weight: 700; font-size: 11px;
    margin-right: 8px;
    flex-shrink: 0;
    font-family: 'JetBrains Mono', monospace;
}

/* ── Buttons ── */
.stButton > button {
    background: #1a3d1e;
    color: #4ade80;
    border: 1px solid #2d5230;
    border-radius: 8px;
    font-weight: 600;
    font-size: 14px;
    padding: 10px 26px;
    transition: all 0.2s;
    letter-spacing: 0.2px;
    font-family: 'Inter', sans-serif;
}
.stButton > button:hover {
    background: #22522a;
    border-color: #4ade80;
    color: #ffffff;
    transform: translateY(-1px);
    box-shadow: 0 4px 20px rgba(74,222,128,0.2);
}
.stButton > button:active { transform: translateY(0); }

/* Download buttons */
.stDownloadButton > button {
    background: #0d2410 !important;
    color: #4ade80 !important;
    border: 1px solid #2d5230 !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    padding: 10px 26px !important;
    width: 100%;
    transition: all 0.2s !important;
    font-family: 'Inter', sans-serif !important;
}
.stDownloadButton > button:hover {
    background: #163020 !important;
    border-color: #4ade80 !important;
    color: #ffffff !important;
    box-shadow: 0 4px 20px rgba(74,222,128,0.2) !important;
}

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    border: 1px dashed #2d5230;
    border-radius: 12px;
    background: #0d1a0f;
    padding: 8px;
    transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #4ade80; }
[data-testid="stFileUploader"] * { color: #6b9970 !important; }
[data-testid="stFileUploader"] svg { stroke: #4ade80 !important; }

/* ── Progress bar ── */
.stProgress > div > div > div {
    background: linear-gradient(90deg, #22c55e, #4ade80) !important;
    border-radius: 4px;
}
.stProgress > div > div {
    background: #1a2d1c !important;
    border-radius: 4px;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    border: 1px solid #1e2d20 !important;
}
[data-testid="stDataFrame"] * { color: #c8d8ca !important; }

/* ── Streamlit tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: #161a17;
    border-radius: 10px;
    padding: 4px;
    border: 1px solid #1e2d20;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 7px;
    font-weight: 500;
    color: #5a7a5d !important;
    padding: 8px 18px;
}
.stTabs [aria-selected="true"] {
    background: #1a3d1e !important;
    color: #4ade80 !important;
    font-weight: 600;
    border: 1px solid #2d5230;
}

/* ── Expanders ── */
.streamlit-expanderHeader {
    background: #161a17 !important;
    border: 1px solid #1e2d20 !important;
    border-radius: 8px !important;
    color: #9ab89d !important;
    font-weight: 500 !important;
}
.streamlit-expanderContent {
    background: #111312 !important;
    border: 1px solid #1e2d20 !important;
    border-top: none !important;
}

/* ── Inputs ── */
[data-testid="stRadio"] label { color: #9ab89d !important; font-size: 0.9rem !important; }
[data-testid="stCheckbox"] label { color: #9ab89d !important; }
[data-testid="stTextInput"] input, textarea {
    background: #161a17 !important;
    border: 1px solid #2d5230 !important;
    color: #e8f0e9 !important;
    border-radius: 7px !important;
}
[data-testid="stTextInput"] input:focus, textarea:focus {
    border-color: #4ade80 !important;
    box-shadow: 0 0 0 2px rgba(74,222,128,0.15) !important;
}
.stSelectbox > div > div {
    background: #161a17 !important;
    border: 1px solid #2d5230 !important;
    color: #e8f0e9 !important;
    border-radius: 7px !important;
}
[data-testid="stNumberInput"] input {
    background: #161a17 !important;
    border: 1px solid #2d5230 !important;
    color: #4ade80 !important;
    border-radius: 7px !important;
    font-family: 'JetBrains Mono', monospace !important;
}

/* ── Toggle ── */
[data-testid="stToggle"] { color: #9ab89d !important; }
[data-testid="stToggle"] label { color: #9ab89d !important; }

/* ── Empty state ── */
.empty-state {
    background: #0d1a0f;
    border: 1px dashed #2d5230;
    border-radius: 14px;
    padding: 56px 32px;
    text-align: center;
    margin-top: 16px;
}
.empty-state .icon { font-size: 2.8rem; margin-bottom: 14px; }
.empty-state p { color: #5a7a5d; font-size: 0.9rem; margin: 0; line-height: 1.6; }

/* ── Log box ── */
.log-box {
    background: #090d0a;
    border: 1px solid #1e2d20;
    border-radius: 8px;
    padding: 14px 18px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: #4a6b4d;
    max-height: 240px;
    overflow-y: auto;
    line-height: 2;
}
.log-ok   { color: #4ade80; }
.log-warn { color: #facc15; }
.log-err  { color: #f87171; }

/* ── Divider ── */
hr { border-color: #1e2d20 !important; }

/* ── Caption ── */
.stCaption, small { color: #4a6b4d !important; }

/* ── Native alerts ── */
[data-testid="stAlert"] {
    background: #0d1a0f !important;
    border: 1px solid #2d5230 !important;
    color: #9ab89d !important;
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_COLS = [
    "BUSINESS_EMAIL",
    "PERSONAL_EMAILS",
    "PERSONAL_VERIFIED_EMAILS",
    "BUSINESS_VERIFIED_EMAILS",
]
VERIFIED_COLS     = {"PERSONAL_VERIFIED_EMAILS", "BUSINESS_VERIFIED_EMAILS"}
EMAIL_RE          = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PRIMARY_EMAIL_COL = "PRIMARY_EMAIL"
CHUNK_SIZE        = 50_000

# ─────────────────────────────────────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────
for key, default in [
    ("result_df",     None),
    ("stats",         None),
    ("original_name", "output"),
    ("active_tool",   "expander"),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ═════════════════════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def normalize_email(email) -> str:
    if pd.isna(email):
        return ""
    return str(email).strip().lower()


# ─────────────────────────────────────────────────────────────────────────────
#  EMAIL EXPANDER HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def extract_emails(cell_value) -> list[str]:
    if pd.isna(cell_value) or str(cell_value).strip() == "":
        return []
    found = EMAIL_RE.findall(str(cell_value))
    seen, result = set(), []
    for e in found:
        e = e.lower().strip()
        if e not in seen:
            seen.add(e)
            result.append(e)
    return result


def has_verified(row: pd.Series, email: str) -> bool:
    for col in VERIFIED_COLS:
        if col in row.index and email in extract_emails(row.get(col, "")):
            return True
    return False


def detect_source(row: pd.Series, email: str, present_cols: list[str]) -> str:
    for col in present_cols:
        if email in extract_emails(row.get(col, "")):
            return col
    return "unknown"


def load_csv(uploaded_file) -> tuple[pd.DataFrame, bool]:
    try:
        return pd.read_csv(uploaded_file), False
    except Exception:
        uploaded_file.seek(0)
        df = pd.read_csv(
            uploaded_file, engine="python",
            on_bad_lines="skip", encoding_errors="replace",
        )
        return df, True


def highlight_duplicates(row: pd.Series):
    if row.get("DUPLICATE_FLAG", False):
        return ["background-color: #1f0a0a; color: #f87171"] * len(row)
    return [""] * len(row)


def process_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    present_email_cols = [c for c in EMAIL_COLS if c in df.columns]
    total_input_rows   = len(df)
    total_emails_found = 0
    cross_row_dupes    = 0
    duplicate_emails   = set()
    email_registry: dict[str, dict] = {}

    progress = st.progress(0, text="Scanning rows…")

    for idx, (_, row) in enumerate(df.iterrows()):
        row_emails, seen_this_row = [], set()
        for col in present_email_cols:
            for em in extract_emails(row.get(col, "")):
                if em not in seen_this_row:
                    seen_this_row.add(em)
                    row_emails.append(em)

        total_emails_found += len(row_emails)

        for em in row_emails:
            is_ver = has_verified(row, em)
            if em not in email_registry:
                email_registry[em] = {"row": row, "is_verified": is_ver, "count": 1}
            else:
                cross_row_dupes += 1
                email_registry[em]["count"] += 1
                duplicate_emails.add(em)
                if is_ver and not email_registry[em]["is_verified"]:
                    email_registry[em].update({"row": row, "is_verified": is_ver})

        progress.progress(
            min(int((idx + 1) / total_input_rows * 80), 80),
            text=f"Scanning row {idx + 1:,} / {total_input_rows:,}…",
        )

    progress.progress(82, text="Building output rows…")
    expanded_rows = []
    for email, entry in email_registry.items():
        new_row = entry["row"].copy()
        new_row["PRIMARY_EMAIL"]    = email
        new_row["IS_VERIFIED"]      = entry["is_verified"]
        new_row["EMAIL_SOURCE"]     = detect_source(entry["row"], email, present_email_cols)
        new_row["DUPLICATE_FLAG"]   = email in duplicate_emails
        new_row["OCCURRENCE_COUNT"] = entry["count"]
        expanded_rows.append(new_row)

    progress.progress(95, text="Assembling final table…")

    if not expanded_rows:
        progress.empty()
        return pd.DataFrame(), {
            "input_rows": total_input_rows, "output_rows": 0,
            "unique_emails": 0, "cross_dupes": cross_row_dupes,
            "emails_found": total_emails_found,
            "dropped_cols": [], "dup_count": 0,
        }

    result = pd.DataFrame(expanded_rows).reset_index(drop=True)
    cols_to_drop = [c for c in EMAIL_COLS if c in result.columns]
    result.drop(columns=cols_to_drop, inplace=True)

    front  = ["PRIMARY_EMAIL", "IS_VERIFIED", "EMAIL_SOURCE", "DUPLICATE_FLAG", "OCCURRENCE_COUNT"]
    rest   = [c for c in result.columns if c not in front]
    result = result[front + rest]

    progress.progress(100, text="Done!")
    progress.empty()

    return result, {
        "input_rows"   : total_input_rows,
        "output_rows"  : len(result),
        "unique_emails": len(email_registry),
        "cross_dupes"  : cross_row_dupes,
        "emails_found" : total_emails_found,
        "dropped_cols" : cols_to_drop,
        "dup_count"    : len(duplicate_emails),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  BULK DEDUPLICATOR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def score_row(row: pd.Series) -> tuple:
    non_null    = row.notna().sum()
    total_chars = sum(len(str(v)) for v in row if pd.notna(v))
    return (non_null, total_chars)


def read_csv_chunked(file_like, filename: str) -> pd.DataFrame | None:
    try:
        chunks = []
        for chunk in pd.read_csv(file_like, chunksize=CHUNK_SIZE, low_memory=False, dtype=str):
            chunks.append(chunk)
        return pd.concat(chunks, ignore_index=True)
    except Exception as e:
        st.error(f"Could not read {filename}: {e}")
        return None


def validate_email_column(df: pd.DataFrame, filename: str) -> bool:
    cols_lower = {c.strip().lower(): c for c in df.columns}
    if PRIMARY_EMAIL_COL.lower() in cols_lower:
        actual = cols_lower[PRIMARY_EMAIL_COL.lower()]
        if actual != PRIMARY_EMAIL_COL:
            df.rename(columns={actual: PRIMARY_EMAIL_COL}, inplace=True)
        return True
    return False


def build_global_best(dataframes, progress_bar, status_text) -> dict:
    best_map: dict[str, tuple] = {}
    total = len(dataframes)
    for i, (fname, df) in enumerate(dataframes.items()):
        status_text.markdown(f"**Pass 1/{total}** — indexing `{fname}` …")
        progress_bar.progress((i + 1) / total * 0.5)
        if PRIMARY_EMAIL_COL not in df.columns:
            continue
        for idx, row in df.iterrows():
            norm = normalize_email(row[PRIMARY_EMAIL_COL])
            if not norm:
                continue
            sc = score_row(row)
            if norm not in best_map or sc > best_map[norm][2]:
                best_map[norm] = (fname, idx, sc)
    return {k: (v[0], v[1]) for k, v in best_map.items()}


def deduplicate_files(dataframes, best_map, progress_bar, status_text) -> tuple[dict, dict]:
    results, stats = {}, {}
    total = len(dataframes)
    for i, (fname, df) in enumerate(dataframes.items()):
        status_text.markdown(f"**Pass 2/{total}** — deduplicating `{fname}` …")
        progress_bar.progress(0.5 + (i + 1) / total * 0.45)
        original_count = len(df)

        if PRIMARY_EMAIL_COL not in df.columns:
            results[fname] = df
            stats[fname]   = {"original": original_count, "kept": original_count,
                               "removed": 0, "no_email_col": True}
            continue

        keep_mask = []
        for idx, row in df.iterrows():
            norm = normalize_email(row[PRIMARY_EMAIL_COL])
            if not norm:
                keep_mask.append(True)
            else:
                owner_fname, owner_idx = best_map.get(norm, (fname, idx))
                keep_mask.append(owner_fname == fname and owner_idx == idx)

        filtered       = df[keep_mask].reset_index(drop=True)
        results[fname] = filtered
        stats[fname]   = {
            "original": original_count, "kept": len(filtered),
            "removed": original_count - len(filtered), "no_email_col": False,
        }
    return results, stats


# ═════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ═════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ✉️ Email Suite")
    st.markdown("---")

    active = st.session_state.active_tool

    st.markdown("### Tools")
    if st.button("📧  Email Expander", width="stretch",
                 type="primary" if active == "expander" else "secondary"):
        st.session_state.active_tool = "expander"
        st.session_state.result_df   = None
        st.session_state.stats       = None
        st.rerun()

    if st.button("🗂️  Bulk Deduplicator", width="stretch",
                 type="primary" if active == "dedup" else "secondary"):
        st.session_state.active_tool = "dedup"
        st.rerun()

    st.markdown("---")

    if active == "expander":
        st.markdown("### How it works")
        st.markdown("""
<div style='font-size:0.84rem; line-height:2.1'>
<span class='step-badge'>1</span> Upload your CSV<br>
<span class='step-badge'>2</span> All 4 email columns merged per row<br>
<span class='step-badge'>3</span> Each unique email → its own row<br>
<span class='step-badge'>4</span> Source columns auto-removed<br>
<span class='step-badge'>5</span> Verified row wins on duplicates<br>
<span class='step-badge'>6</span> Search, filter &amp; download
</div>
""", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("### Email columns")
        for c in EMAIL_COLS:
            icon = "✅" if c in VERIFIED_COLS else "📧"
            st.markdown(
                f"<span style='font-size:0.8rem;font-family:JetBrains Mono,monospace'>"
                f"{icon} {c}</span>",
                unsafe_allow_html=True,
            )
        st.markdown("---")
        st.markdown("""
<div style='font-size:0.8rem; color:#4a6b4d; line-height:1.7'>
<b style='color:#4ade80'>Dedup rule</b><br>
Same email in 2 rows → keep the verified row. If neither is verified, first row wins.
</div>
""", unsafe_allow_html=True)

    else:
        st.markdown("### How it works")
        st.markdown("""
<div style='font-size:0.84rem; line-height:2.1'>
<span class='step-badge'>1</span> Upload all CSV files<br>
<span class='step-badge'>2</span> Pass 1: index richest row per email<br>
<span class='step-badge'>3</span> Pass 2: strip duplicates per file<br>
<span class='step-badge'>4</span> Download ZIP of results
</div>
""", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("""
<div style='font-size:0.8rem; color:#4a6b4d; line-height:1.7'>
<b style='color:#4ade80'>Richest row</b><br>
Fewest empty cells wins. Longest total text as tiebreaker.<br><br>
Requires a <b style='color:#9ab89d'>PRIMARY_EMAIL</b> column in each CSV.
</div>
""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  TOOL 1 — EMAIL EXPANDER
# ═════════════════════════════════════════════════════════════════════════════

active = st.session_state.active_tool

if active == "expander":

    st.markdown("""
<div class='hero'>
  <div class='hero-badge'>Tool 01 · Email Expander</div>
  <div class='hero-title'>📧 Email Expander</div>
  <div class='hero-sub'>Upload a CSV → extract, deduplicate &amp; expand emails · one row per unique address</div>
</div>
""", unsafe_allow_html=True)

    st.markdown('<p class="section-header">Upload CSV</p>', unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Drop your CSV file here",
        type=["csv"],
        help="Must contain at least one of the 4 email columns listed in the sidebar.",
        label_visibility="collapsed",
    )

    if not uploaded:
        st.markdown("""
<div class='empty-state'>
  <div class='icon'>📂</div>
  <p>Drop a CSV file above to get started.<br>
  The file must contain at least one of the 4 email columns.</p>
</div>
""", unsafe_allow_html=True)

    else:
        st.markdown('<p class="section-header">Raw File Preview</p>', unsafe_allow_html=True)

        try:
            raw_df, had_bad_rows = load_csv(uploaded)
        except Exception as e:
            st.error(f"Could not read the file: {e}")
            st.stop()

        if had_bad_rows:
            st.markdown(
                "<div class='warn-box'>⚠️ A few rows had unclosed quotes and were skipped. "
                "All other rows loaded successfully.</div>",
                unsafe_allow_html=True,
            )

        found_cols = [c for c in EMAIL_COLS if c in raw_df.columns]
        missing    = [c for c in EMAIL_COLS if c not in raw_df.columns]

        col_prev, col_info = st.columns([3, 1])
        with col_prev:
            st.dataframe(raw_df.head(10), width="stretch", height=240)
        with col_info:
            st.markdown(
                f"<div class='stat-card white'>"
                f"<div class='stat-number'>{len(raw_df):,}</div>"
                f"<div class='stat-label'>Total rows</div></div>",
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(
                f"<div class='stat-card dim'>"
                f"<div class='stat-number'>{len(raw_df.columns)}</div>"
                f"<div class='stat-label'>Columns</div></div>",
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)
            for c in found_cols:
                st.markdown(
                    f"<span style='color:#4ade80;font-size:0.8rem;font-family:JetBrains Mono,monospace'>✔ {c}</span>",
                    unsafe_allow_html=True,
                )
            for c in missing:
                st.markdown(
                    f"<span style='color:#3a5a3d;font-size:0.8rem;font-family:JetBrains Mono,monospace'>✗ {c}</span>",
                    unsafe_allow_html=True,
                )

        if not found_cols:
            st.markdown(
                "<div class='danger-box'>❌ No email columns found. Please check your CSV headers.</div>",
                unsafe_allow_html=True,
            )
            st.stop()

        if missing:
            st.markdown(
                f"<div class='warn-box'>⚠️ Columns {missing} not found — they will be skipped.</div>",
                unsafe_allow_html=True,
            )

        st.markdown("---")
        if st.button("⚡ Process & Expand Emails", width="stretch"):
            result_df, stats = process_dataframe(raw_df.copy())
            if result_df.empty:
                st.warning("No emails found in this file.")
                st.stop()
            st.session_state.result_df     = result_df
            st.session_state.stats         = stats
            st.session_state.original_name = uploaded.name.replace(".csv", "")
            st.rerun()

    # ── Results ──────────────────────────────────────────────────────────────
    if st.session_state.result_df is not None:
        result_df = st.session_state.result_df
        stats     = st.session_state.stats

        st.markdown('<p class="section-header">Processing Summary</p>', unsafe_allow_html=True)

        cards = [
            (stats["input_rows"],    "Input rows",      "white"),
            (stats["emails_found"],  "Emails found",    "dim"),
            (stats["unique_emails"], "Unique emails",   "green"),
            (stats["cross_dupes"],   "Cross-row dupes", "yellow"),
            (stats["dup_count"],     "In 2+ rows",      "red"),
            (stats["output_rows"],   "Output rows",     "green"),
        ]
        cols = st.columns(6)
        for col, (val, label, color) in zip(cols, cards):
            with col:
                st.markdown(
                    f"<div class='stat-card {color}'>"
                    f"<div class='stat-number'>{val:,}</div>"
                    f"<div class='stat-label'>{label}</div></div>",
                    unsafe_allow_html=True,
                )

        if stats.get("dropped_cols"):
            dropped_str = ", ".join(stats["dropped_cols"])
            st.markdown(
                f"<div class='success-box'>✔ Auto-removed source columns: {dropped_str}</div>",
                unsafe_allow_html=True,
            )

        # Preview & Search
        st.markdown('<p class="section-header">Preview & Search</p>', unsafe_allow_html=True)

        srch1, srch2, srch3 = st.columns([2, 2, 1])
        with srch1:
            search_query = st.text_input(
                "search_query", placeholder="Search across columns…",
                label_visibility="collapsed",
            )
        with srch2:
            search_col = st.selectbox(
                "search_col", ["All columns"] + list(result_df.columns),
                label_visibility="collapsed", key="search_col_sel",
            )
        with srch3:
            show_dupes_only = st.checkbox("Dupes only", key="dupes_chk")

        preview_df = result_df.copy()
        if show_dupes_only and "DUPLICATE_FLAG" in preview_df.columns:
            preview_df = preview_df[preview_df["DUPLICATE_FLAG"] == True]
        if search_query.strip():
            q = search_query.strip().lower()
            if search_col == "All columns":
                mask = preview_df.apply(
                    lambda row: row.astype(str).str.lower().str.contains(q).any(), axis=1
                )
            else:
                mask = preview_df[search_col].astype(str).str.lower().str.contains(q, na=False)
            preview_df = preview_df[mask]

        is_filtered = bool(search_query.strip() or show_dupes_only)
        st.markdown(
            f"<div class='info-box'>Showing <b style='color:#4ade80'>{len(preview_df):,}</b> of "
            f"<b style='color:#4ade80'>{len(result_df):,}</b> rows"
            f"{' &nbsp;·&nbsp; filtered' if is_filtered else ''}</div>",
            unsafe_allow_html=True,
        )

        default_cols = ["PRIMARY_EMAIL", "IS_VERIFIED", "EMAIL_SOURCE",
                        "DUPLICATE_FLAG", "OCCURRENCE_COUNT",
                        "FIRST_NAME", "LAST_NAME", "COMPANY_NAME", "JOB_TITLE"]
        display_cols = [c for c in default_cols if c in preview_df.columns]
        show_100     = preview_df[display_cols].head(100)

        if "DUPLICATE_FLAG" in show_100.columns:
            styled = show_100.style.apply(highlight_duplicates, axis=1)
            st.dataframe(styled, width="stretch", height=380)
        else:
            st.dataframe(show_100, width="stretch", height=380)

        with st.expander("Show all columns — first 20 rows"):
            st.dataframe(preview_df.head(20), width="stretch", height=300)

        st.markdown(
            f"<div class='info-box'>Working dataset: "
            f"<b style='color:#4ade80'>{len(result_df):,} rows</b> × "
            f"<b style='color:#4ade80'>{len(result_df.columns)} columns</b></div>",
            unsafe_allow_html=True,
        )

        # Download
        st.markdown('<p class="section-header">Download</p>', unsafe_allow_html=True)
        original_name = st.session_state.original_name

        dl1, dl2, dl3 = st.columns(3)
        with dl1:
            st.download_button(
                label=f"⬇  Full output  ({len(result_df):,} rows)",
                data=df_to_csv_bytes(result_df),
                file_name=f"{original_name}_email_expanded.csv",
                mime="text/csv", key="dl_full",
            )
        with dl2:
            if "IS_VERIFIED" in result_df.columns:
                ver_df = result_df[result_df["IS_VERIFIED"] == True]
                if not ver_df.empty:
                    st.download_button(
                        label=f"⬇  Verified only  ({len(ver_df):,} rows)",
                        data=df_to_csv_bytes(ver_df),
                        file_name=f"{original_name}_verified_only.csv",
                        mime="text/csv", key="dl_verified",
                    )
                else:
                    st.markdown("<div class='info-box'>No verified emails.</div>", unsafe_allow_html=True)
        with dl3:
            if "DUPLICATE_FLAG" in result_df.columns:
                dup_df = result_df[result_df["DUPLICATE_FLAG"] == True]
                if not dup_df.empty:
                    st.download_button(
                        label=f"⬇  Duplicates  ({len(dup_df):,} rows)",
                        data=df_to_csv_bytes(dup_df),
                        file_name=f"{original_name}_duplicates.csv",
                        mime="text/csv", key="dl_dupes",
                    )
                else:
                    st.markdown("<div class='info-box'>No duplicate emails.</div>", unsafe_allow_html=True)

        if is_filtered:
            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label=f"⬇  Filtered view  ({len(preview_df):,} rows)",
                data=df_to_csv_bytes(preview_df),
                file_name=f"{original_name}_filtered.csv",
                mime="text/csv", key="dl_filtered",
            )


# ═════════════════════════════════════════════════════════════════════════════
#  TOOL 2 — BULK DEDUPLICATOR
# ═════════════════════════════════════════════════════════════════════════════

elif active == "dedup":

    st.markdown("""
<div class='hero'>
  <div class='hero-badge'>Tool 02 · Bulk Deduplicator</div>
  <div class='hero-title'>🗂️ Bulk Deduplicator</div>
  <div class='hero-sub'>Upload many CSVs → cross-file PRIMARY_EMAIL deduplication · keeps richest row · outputs ZIP</div>
</div>
""", unsafe_allow_html=True)

    up_col, cfg_col = st.columns([3, 1])
    with up_col:
        st.markdown('<p class="section-header">Upload CSV Files</p>', unsafe_allow_html=True)
        uploaded_files = st.file_uploader(
            "Drop CSV files here — 100+ files, 30–40 MB each supported",
            type=["csv"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )
    with cfg_col:
        st.markdown('<p class="section-header">Settings</p>', unsafe_allow_html=True)
        chunk_size = st.number_input(
            "Chunk size (rows)",
            min_value=10_000, max_value=200_000,
            value=CHUNK_SIZE, step=10_000,
            help="Lower = less RAM. Higher = faster.",
        )
        show_preview = st.toggle("Show previews", value=True)

    if not uploaded_files:
        st.markdown("""
<div class='empty-state'>
  <div class='icon'>📂</div>
  <p>Upload one or more CSV files to begin.<br>
  Each file must contain a <b>PRIMARY_EMAIL</b> column.</p>
</div>
""", unsafe_allow_html=True)

    else:
        total_size_mb = sum(f.size for f in uploaded_files) / (1024 ** 2)
        m1, m2, m3 = st.columns(3)
        with m1:
            st.markdown(
                f"<div class='stat-card white'><div class='stat-number'>{len(uploaded_files)}</div>"
                f"<div class='stat-label'>Files uploaded</div></div>",
                unsafe_allow_html=True,
            )
        with m2:
            st.markdown(
                f"<div class='stat-card dim'><div class='stat-number'>{total_size_mb:.1f} MB</div>"
                f"<div class='stat-label'>Total size</div></div>",
                unsafe_allow_html=True,
            )
        with m3:
            est_rows = int(total_size_mb / 0.00015 / 1000)
            st.markdown(
                f"<div class='stat-card green'><div class='stat-number'>~{est_rows}K</div>"
                f"<div class='stat-label'>Est. rows</div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("---")

        if st.button("🚀 Run Deduplication", width="stretch"):

            log_lines: list[str] = []

            def log(msg: str, kind: str = "ok"):
                icon = {"ok": "✔", "warn": "⚠", "err": "✖"}.get(kind, "•")
                log_lines.append(f'<span class="log-{kind}">{icon}  {msg}</span>')

            st.markdown('<p class="section-header">Processing</p>', unsafe_allow_html=True)
            progress_bar  = st.progress(0)
            status_text   = st.empty()
            log_container = st.empty()

            dataframes: dict[str, pd.DataFrame] = {}
            total_files = len(uploaded_files)
            status_text.markdown("**Loading files into memory…**")

            for i, uf in enumerate(uploaded_files):
                progress_bar.progress((i + 1) / total_files * 0.3)
                status_text.markdown(
                    f"**Reading {i+1}/{total_files}** — `{uf.name}` ({uf.size/1e6:.1f} MB)"
                )
                df = read_csv_chunked(io.BytesIO(uf.read()), uf.name)
                if df is None:
                    log(f"SKIP {uf.name} — could not read", "err")
                    continue
                has_col = validate_email_column(df, uf.name)
                if not has_col:
                    log(f"WARN {uf.name} — no PRIMARY_EMAIL column; kept unchanged", "warn")
                else:
                    log(f"OK   {uf.name} — {len(df):,} rows loaded")
                dataframes[uf.name] = df
                log_container.markdown(
                    f'<div class="log-box">{"<br>".join(log_lines[-30:])}</div>',
                    unsafe_allow_html=True,
                )

            if not dataframes:
                st.error("No valid files could be loaded.")
                st.stop()

            status_text.markdown("**Pass 1 — building global email index…**")
            best_map = build_global_best(dataframes, progress_bar, status_text)
            log(f"Index built — {len(best_map):,} unique emails found", "ok")

            status_text.markdown("**Pass 2 — deduplicating files…**")
            results, dedup_stats = deduplicate_files(dataframes, best_map, progress_bar, status_text)
            log(f"Done — {len(results)} files processed", "ok")

            progress_bar.progress(1.0)
            status_text.markdown("**✔ Complete**")
            log_container.markdown(
                f'<div class="log-box">{"<br>".join(log_lines)}</div>',
                unsafe_allow_html=True,
            )

            st.markdown('<p class="section-header">Results Summary</p>', unsafe_allow_html=True)

            total_original = sum(s["original"] for s in dedup_stats.values())
            total_kept     = sum(s["kept"]     for s in dedup_stats.values())
            total_removed  = sum(s["removed"]  for s in dedup_stats.values())
            pct            = total_removed / max(total_original, 1) * 100

            r1, r2, r3, r4 = st.columns(4)
            for col, val, label, color in [
                (r1, len(results),   "Files processed",    "white"),
                (r2, total_original, "Total rows in",      "dim"),
                (r3, total_kept,     "Total rows out",     "green"),
                (r4, total_removed,  "Duplicates removed", "red"),
            ]:
                with col:
                    st.markdown(
                        f"<div class='stat-card {color}'>"
                        f"<div class='stat-number'>{val:,}</div>"
                        f"<div class='stat-label'>{label}</div></div>",
                        unsafe_allow_html=True,
                    )

            st.markdown(
                f"<div class='success-box'>✔ Overall reduction: <b>{pct:.1f}%</b> — "
                f"{total_removed:,} duplicate rows removed across {len(results)} files.</div>",
                unsafe_allow_html=True,
            )

            st.markdown('<p class="section-header">Per-File Breakdown</p>', unsafe_allow_html=True)
            summary_rows = []
            for fname, s in dedup_stats.items():
                summary_rows.append({
                    "File":      fname,
                    "Rows in":   s["original"],
                    "Rows out":  s["kept"],
                    "Removed":   s["removed"],
                    "% removed": f"{s['removed']/max(s['original'],1)*100:.1f}%",
                    "Status":    "⚠ No email col" if s.get("no_email_col") else "✔ OK",
                })
            st.dataframe(pd.DataFrame(summary_rows), width="stretch", hide_index=True)

            if show_preview:
                st.markdown('<p class="section-header">Output Previews</p>', unsafe_allow_html=True)
                for fname, df_out in list(results.items())[:5]:
                    with st.expander(f"📄 {fname}  ({len(df_out):,} rows)"):
                        st.dataframe(df_out.head(50), width="stretch", hide_index=True)
                if len(results) > 5:
                    st.caption(f"… and {len(results)-5} more files")

            st.markdown('<p class="section-header">Download</p>', unsafe_allow_html=True)
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for fname, df_out in results.items():
                    csv_bytes = df_out.to_csv(index=False).encode("utf-8")
                    out_name  = fname if fname.lower().endswith(".csv") else fname + ".csv"
                    zf.writestr(out_name, csv_bytes)
            zip_buffer.seek(0)

            st.download_button(
                label="⬇  Download all deduplicated CSVs as ZIP",
                data=zip_buffer,
                file_name="deduplicated_output.zip",
                mime="application/zip",
                width="stretch",
            )