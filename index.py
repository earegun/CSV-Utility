"""
Email Suite — unified CSV email processor + cross-file deduplicator
=======================================================================
Tab 1 · Email Expander  (from app.py)
  - Upload one CSV → extract emails from up to 4 source columns
  - Each unique email gets its own row
  - Verified flag, duplicate flag, source column tracked
  - Post-processing: delete cols/rows, rename, filter by verified
  - Search/filter preview, download full / verified / duplicates / filtered

Tab 2 · Bulk Deduplicator  (from dup.py)
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
#  GLOBAL CSS  — clean, professional light theme
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    font-size: 14px;
}
.stApp {
    background: #f4f6f9;
    color: #1a1d2e;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #1a1d2e;
    border-right: none;
}
section[data-testid="stSidebar"] * { color: #c8cfe8 !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 { color: #ffffff !important; font-weight: 600 !important; }
section[data-testid="stSidebar"] .stMarkdown a { color: #7b9cff !important; }

/* ── Hero strip ── */
.hero {
    background: linear-gradient(135deg, #1a1d2e 0%, #2d3561 100%);
    border-radius: 14px;
    padding: 28px 36px 24px;
    margin-bottom: 24px;
    color: #fff;
    position: relative;
    overflow: hidden;
}
.hero::before {
    content: '';
    position: absolute;
    top: -40px; right: -40px;
    width: 200px; height: 200px;
    background: radial-gradient(circle, rgba(123,156,255,0.18) 0%, transparent 70%);
    border-radius: 50%;
}
.hero h1 { color: #fff; font-size: 1.7rem; font-weight: 700; margin: 0 0 6px 0; letter-spacing: -0.5px; }
.hero p  { color: #a0aec8; font-size: 0.88rem; margin: 0; }

/* ── Metric / stat cards ── */
.stat-card {
    background: #ffffff;
    border-radius: 12px;
    padding: 18px 20px;
    border: 1px solid #e4e9f0;
    text-align: center;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
    transition: box-shadow 0.2s;
}
.stat-card:hover { box-shadow: 0 4px 14px rgba(0,0,0,0.08); }
.stat-number { font-size: 2rem; font-weight: 700; color: #1a1d2e; line-height: 1; font-family: 'DM Mono', monospace; }
.stat-label  { font-size: 0.78rem; color: #6b7280; margin-top: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
.stat-card.green .stat-number { color: #059669; }
.stat-card.blue  .stat-number { color: #2563eb; }
.stat-card.amber .stat-number { color: #d97706; }
.stat-card.red   .stat-number { color: #dc2626; }
.stat-card.purple .stat-number { color: #7c3aed; }

/* ── Section headers ── */
.section-header {
    font-size: 0.95rem;
    font-weight: 600;
    color: #1a1d2e;
    margin: 28px 0 14px 0;
    padding-bottom: 8px;
    border-bottom: 2px solid #e4e9f0;
    letter-spacing: -0.2px;
}

/* ── Message boxes ── */
.info-box {
    background: #eff6ff; border-left: 4px solid #2563eb; border-radius: 6px;
    padding: 11px 15px; font-size: 0.875rem; color: #1e40af; margin: 10px 0;
}
.warn-box {
    background: #fffbeb; border-left: 4px solid #d97706; border-radius: 6px;
    padding: 11px 15px; font-size: 0.875rem; color: #92400e; margin: 10px 0;
}
.success-box {
    background: #ecfdf5; border-left: 4px solid #059669; border-radius: 6px;
    padding: 11px 15px; font-size: 0.875rem; color: #065f46; margin: 10px 0;
}
.danger-box {
    background: #fef2f2; border-left: 4px solid #dc2626; border-radius: 6px;
    padding: 11px 15px; font-size: 0.875rem; color: #991b1b; margin: 10px 0;
}

/* ── Step badges (sidebar) ── */
.step-badge {
    display: inline-flex; align-items: center; justify-content: center;
    background: #3b50a0; color: #ffffff !important;
    border-radius: 50%; width: 22px; height: 22px;
    font-weight: 700; font-size: 12px; margin-right: 8px;
    flex-shrink: 0;
}

/* ── Buttons ── */
.stButton > button {
    background: #2563eb;
    color: #ffffff;
    border-radius: 8px;
    font-weight: 600;
    font-size: 14px;
    padding: 10px 26px;
    border: none;
    transition: background 0.2s, transform 0.1s, box-shadow 0.2s;
    letter-spacing: 0.2px;
}
.stButton > button:hover {
    background: #1d4ed8;
    transform: translateY(-1px);
    box-shadow: 0 4px 14px rgba(37,99,235,0.3);
}
.stButton > button:active { transform: translateY(0); }

/* Download buttons */
.stDownloadButton > button {
    background: #059669 !important;
    color: #ffffff !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    padding: 10px 26px !important;
    border: none !important;
    width: 100%;
    transition: background 0.2s !important;
}
.stDownloadButton > button:hover { background: #047857 !important; }

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    border: 2px dashed #c7d2fe;
    border-radius: 12px;
    background: #f5f7ff;
    padding: 8px;
    transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #2563eb; }

/* ── Progress bar ── */
.stProgress > div > div > div { background: #2563eb !important; border-radius: 4px; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border-radius: 10px; border: 1px solid #e4e9f0 !important; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: #ffffff;
    border-radius: 10px;
    padding: 4px;
    border: 1px solid #e4e9f0;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 7px;
    font-weight: 500;
    color: #6b7280;
    padding: 8px 18px;
}
.stTabs [aria-selected="true"] {
    background: #2563eb !important;
    color: #ffffff !important;
    font-weight: 600;
}

/* ── Main nav tabs ── */
.main-tab-bar {
    display: flex;
    gap: 8px;
    margin-bottom: 24px;
}
.main-tab-btn {
    flex: 1;
    background: #ffffff;
    border: 2px solid #e4e9f0;
    border-radius: 12px;
    padding: 16px 20px;
    cursor: pointer;
    text-align: center;
    transition: all 0.2s;
    font-family: 'DM Sans', sans-serif;
    font-weight: 600;
    color: #6b7280;
    font-size: 15px;
}
.main-tab-btn:hover { border-color: #2563eb; color: #2563eb; }
.main-tab-btn.active { background: #2563eb; border-color: #2563eb; color: #fff; }

/* ── Log box (bulk dedup) ── */
.log-box {
    background: #1a1d2e;
    border: 1px solid #2d3561;
    border-radius: 8px;
    padding: 14px 18px;
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #8a9ab8;
    max-height: 240px;
    overflow-y: auto;
    line-height: 1.9;
}
.log-ok   { color: #4ade80; }
.log-warn { color: #facc15; }
.log-err  { color: #f87171; }

/* ── Empty state ── */
.empty-state {
    background: #ffffff;
    border: 2px dashed #e4e9f0;
    border-radius: 14px;
    padding: 48px 32px;
    text-align: center;
    color: #9ca3af;
    margin-top: 16px;
}
.empty-state .icon { font-size: 2.5rem; margin-bottom: 12px; }
.empty-state p { font-size: 0.9rem; margin: 0; }

/* ── Expanders ── */
.streamlit-expanderHeader {
    background: #ffffff !important;
    border: 1px solid #e4e9f0 !important;
    border-radius: 8px !important;
    color: #1a1d2e !important;
    font-weight: 500 !important;
}

/* ── Radio / checkbox ── */
[data-testid="stRadio"] label { font-size: 0.9rem !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  SHARED CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
# ── Expander constants (app.py) ──
EMAIL_COLS = [
    "BUSINESS_EMAIL",
    "PERSONAL_EMAILS",
    "PERSONAL_VERIFIED_EMAILS",
    "BUSINESS_VERIFIED_EMAILS",
]
VERIFIED_COLS  = {"PERSONAL_VERIFIED_EMAILS", "BUSINESS_VERIFIED_EMAILS"}
PROTECTED_COLS = {"PRIMARY_EMAIL", "IS_VERIFIED", "EMAIL_SOURCE"}
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# ── Dedup constants (dup.py) ──
PRIMARY_EMAIL_COL = "PRIMARY_EMAIL"
CHUNK_SIZE        = 50_000


# ─────────────────────────────────────────────────────────────────────────────
#  SESSION STATE INIT
# ─────────────────────────────────────────────────────────────────────────────
for key, default in [
    ("result_df",     None),
    ("stats",         None),
    ("original_name", "output"),
    ("active_tool",   "expander"),   # "expander" | "dedup"
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ═════════════════════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Serialize DataFrame to CSV bytes."""
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def normalize_email(email) -> str:
    """Lowercase + strip for comparison key."""
    if pd.isna(email):
        return ""
    return str(email).strip().lower()


# ─────────────────────────────────────────────────────────────────────────────
#  EMAIL EXPANDER HELPERS  (from app.py)
# ─────────────────────────────────────────────────────────────────────────────

def extract_emails(cell_value) -> list[str]:
    """Find all valid email addresses in a cell, deduplicated, lowercased."""
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
    """Return True if email appears in any verified column of the row."""
    for col in VERIFIED_COLS:
        if col in row.index and email in extract_emails(row.get(col, "")):
            return True
    return False


def detect_source(row: pd.Series, email: str, present_cols: list[str]) -> str:
    """Return the name of the column where this email first appears."""
    for col in present_cols:
        if email in extract_emails(row.get(col, "")):
            return col
    return "unknown"


def load_csv(uploaded_file) -> tuple[pd.DataFrame, bool]:
    """Load a CSV with fallback for bad rows; returns (df, had_bad_rows)."""
    try:
        return pd.read_csv(uploaded_file), False
    except Exception:
        uploaded_file.seek(0)
        df = pd.read_csv(
            uploaded_file,
            engine="python",
            on_bad_lines="skip",
            encoding_errors="replace",
        )
        return df, True


def highlight_duplicates(row: pd.Series):
    """Pandas Styler row-highlighter — red background for duplicates."""
    if row.get("DUPLICATE_FLAG", False):
        return ["background-color: #fef2f2; color: #991b1b"] * len(row)
    return [""] * len(row)


def process_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Core Email Expander logic (from app.py):
      - Scan all 4 source email columns
      - Build registry: email → best row (verified wins)
      - Expand to one row per unique email
      - Add PRIMARY_EMAIL, IS_VERIFIED, EMAIL_SOURCE, DUPLICATE_FLAG, OCCURRENCE_COUNT
      - Drop original source email columns
    """
    present_email_cols = [c for c in EMAIL_COLS if c in df.columns]
    total_input_rows   = len(df)
    total_emails_found = 0
    cross_row_dupes    = 0
    duplicate_emails   = set()

    # email -> {row, is_verified, count}
    email_registry: dict[str, dict] = {}

    progress = st.progress(0, text="Scanning rows…")

    for idx, (_, row) in enumerate(df.iterrows()):
        row_emails    = []
        seen_this_row = set()
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
                # Verified row wins
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

    # Auto-drop the source email columns
    cols_to_drop = [c for c in EMAIL_COLS if c in result.columns]
    result.drop(columns=cols_to_drop, inplace=True)

    # Move key columns to front
    front = ["PRIMARY_EMAIL", "IS_VERIFIED", "EMAIL_SOURCE", "DUPLICATE_FLAG", "OCCURRENCE_COUNT"]
    rest  = [c for c in result.columns if c not in front]
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
#  BULK DEDUPLICATOR HELPERS  (from dup.py)
# ─────────────────────────────────────────────────────────────────────────────

def score_row(row: pd.Series) -> tuple:
    """Returns (non_null_count, total_char_length) — higher is richer."""
    non_null    = row.notna().sum()
    total_chars = sum(len(str(v)) for v in row if pd.notna(v))
    return (non_null, total_chars)


def read_csv_chunked(file_like, filename: str) -> pd.DataFrame | None:
    """Read a large CSV in chunks, return full DataFrame or None on error."""
    try:
        chunks = []
        for chunk in pd.read_csv(file_like, chunksize=CHUNK_SIZE, low_memory=False, dtype=str):
            chunks.append(chunk)
        return pd.concat(chunks, ignore_index=True)
    except Exception as e:
        st.error(f"❌ Could not read **{filename}**: {e}")
        return None


def validate_email_column(df: pd.DataFrame, filename: str) -> bool:
    """
    Check that PRIMARY_EMAIL column exists (case-insensitive).
    Renames the column in-place if found under a different casing.
    """
    cols_lower = {c.strip().lower(): c for c in df.columns}
    if PRIMARY_EMAIL_COL.lower() in cols_lower:
        actual = cols_lower[PRIMARY_EMAIL_COL.lower()]
        if actual != PRIMARY_EMAIL_COL:
            df.rename(columns={actual: PRIMARY_EMAIL_COL}, inplace=True)
        return True
    return False


def build_global_best(
    dataframes: dict[str, pd.DataFrame],
    progress_bar,
    status_text,
) -> dict[str, tuple[str, int]]:
    """
    Pass 1 — scan every file, build a global map of norm_email → (source_file, row_index).
    Keeps only the 'richest' row (most non-null cells, then longest text).
    """
    best_map: dict[str, tuple[str, int, tuple]] = {}
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


def deduplicate_files(
    dataframes: dict[str, pd.DataFrame],
    best_map: dict[str, tuple[str, int]],
    progress_bar,
    status_text,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict]]:
    """
    Pass 2 — for each file, keep only rows that are the globally best for their email.
    Rows with no email are always kept.
    """
    results: dict[str, pd.DataFrame] = {}
    stats:   dict[str, dict]         = {}
    total = len(dataframes)

    for i, (fname, df) in enumerate(dataframes.items()):
        status_text.markdown(f"**Pass 2/{total}** — deduplicating `{fname}` …")
        progress_bar.progress(0.5 + (i + 1) / total * 0.45)
        original_count = len(df)

        if PRIMARY_EMAIL_COL not in df.columns:
            results[fname] = df
            stats[fname] = {
                "original": original_count, "kept": original_count,
                "removed": 0, "no_email_col": True,
            }
            continue

        keep_mask = []
        for idx, row in df.iterrows():
            norm = normalize_email(row[PRIMARY_EMAIL_COL])
            if not norm:
                keep_mask.append(True)  # blank email → always keep
            else:
                owner_fname, owner_idx = best_map.get(norm, (fname, idx))
                keep_mask.append(owner_fname == fname and owner_idx == idx)

        filtered = df[keep_mask].reset_index(drop=True)
        results[fname] = filtered
        stats[fname] = {
            "original": original_count,
            "kept":     len(filtered),
            "removed":  original_count - len(filtered),
            "no_email_col": False,
        }

    return results, stats


# ═════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ═════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ✉️ Email Suite")
    st.markdown("---")

    active = st.session_state.active_tool

    st.markdown("### 🛠️ Tools")
    if st.button("📧 Email Expander",    use_container_width=True,
                 type="primary" if active == "expander" else "secondary"):
        st.session_state.active_tool = "expander"
        st.session_state.result_df   = None
        st.session_state.stats       = None
        st.rerun()
    if st.button("🗂️ Bulk Deduplicator", use_container_width=True,
                 type="primary" if active == "dedup" else "secondary"):
        st.session_state.active_tool = "dedup"
        st.rerun()

    st.markdown("---")

    if active == "expander":
        st.markdown("### 📧 Email Expander")
        st.markdown("""
<div style='font-size:0.85rem; line-height:2'>
<span class='step-badge'>1</span> Upload your CSV<br>
<span class='step-badge'>2</span> All 4 email columns merged per row<br>
<span class='step-badge'>3</span> Each unique email → its own row<br>
<span class='step-badge'>4</span> Source columns auto-removed<br>
<span class='step-badge'>5</span> Cross-row dupes: verified wins<br>
<span class='step-badge'>6</span> Filter, rename, search, download
</div>
""", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("**Email columns scanned**")
        for c in EMAIL_COLS:
            icon = "✅" if c in VERIFIED_COLS else "📧"
            st.markdown(f"{icon} `{c}`")
        st.markdown("---")
        st.markdown("""
<div style='font-size:0.82rem; color:#6b7c9d'>
<b>Dedup rule</b><br>
Same email in two rows → keep the row where it appears in a <b>verified</b> column.
If neither verified, first row wins.
</div>
""", unsafe_allow_html=True)

    else:
        st.markdown("### 🗂️ Bulk Deduplicator")
        st.markdown("""
<div style='font-size:0.85rem; line-height:2'>
<span class='step-badge'>1</span> Upload all CSV files<br>
<span class='step-badge'>2</span> Pass 1: find richest row per email<br>
<span class='step-badge'>3</span> Pass 2: strip duplicates from each<br>
<span class='step-badge'>4</span> Download ZIP of results
</div>
""", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("""
<div style='font-size:0.82rem; color:#6b7c9d'>
<b>Richest row</b> = fewest empty cells, then longest total text (tiebreaker).
<br><br>
Requires a <b>PRIMARY_EMAIL</b> column in each CSV (case-insensitive).
</div>
""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  HERO HEADER
# ═════════════════════════════════════════════════════════════════════════════

active = st.session_state.active_tool

if active == "expander":
    st.markdown("""
<div class='hero'>
  <h1>📧 Email Expander</h1>
  <p>Upload a CSV → extract, deduplicate & expand emails · one row per unique address</p>
</div>
""", unsafe_allow_html=True)
else:
    st.markdown("""
<div class='hero'>
  <h1>🗂️ Bulk Deduplicator</h1>
  <p>Upload many CSVs → cross-file PRIMARY_EMAIL deduplication · keeps richest row · outputs ZIP</p>
</div>
""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  TOOL 1 — EMAIL EXPANDER
# ═════════════════════════════════════════════════════════════════════════════

if active == "expander":

    # ── Upload ───────────────────────────────────────────────────────────────
    st.markdown('<p class="section-header">📂 Upload CSV</p>', unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Drop your CSV file here",
        type=["csv"],
        help="Must contain at least one of the 4 email columns listed in the sidebar.",
        label_visibility="collapsed",
    )

    if not uploaded:
        st.markdown("""
<div class='empty-state'>
  <div class='icon'>⬆️</div>
  <p>Upload a CSV file above to get started.<br>
  The file must contain at least one of the 4 email columns.</p>
</div>
""", unsafe_allow_html=True)

    else:
        # ── Raw file preview ─────────────────────────────────────────────────
        st.markdown('<p class="section-header">📄 Raw File Preview</p>', unsafe_allow_html=True)

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

        found_cols  = [c for c in EMAIL_COLS if c in raw_df.columns]
        missing     = [c for c in EMAIL_COLS if c not in raw_df.columns]

        col_preview, col_info = st.columns([3, 1])
        with col_preview:
            st.dataframe(raw_df.head(10), use_container_width=True, height=250)
        with col_info:
            st.markdown(
                f"<div class='stat-card blue'>"
                f"<div class='stat-number'>{len(raw_df):,}</div>"
                f"<div class='stat-label'>Rows</div></div>",
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(
                f"<div class='stat-card blue'>"
                f"<div class='stat-number'>{len(raw_df.columns)}</div>"
                f"<div class='stat-label'>Columns</div></div>",
                unsafe_allow_html=True,
            )
            st.markdown("<br>", unsafe_allow_html=True)
            for c in found_cols:
                st.markdown(f"✅ `{c}`")
            for c in missing:
                st.markdown(f"⚠️ `{c}`")

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
        if st.button("⚡ Process & Expand Emails", use_container_width=True):
            result_df, stats = process_dataframe(raw_df.copy())
            if result_df.empty:
                st.warning("No emails were found in the file.")
                st.stop()
            st.session_state.result_df     = result_df
            st.session_state.stats         = stats
            st.session_state.original_name = uploaded.name.replace(".csv", "")
            st.rerun()

    # ── Post-processing (shown once processing is done) ──────────────────────
    if st.session_state.result_df is not None:
        result_df = st.session_state.result_df
        stats     = st.session_state.stats

        # Stats summary
        st.markdown('<p class="section-header">📊 Processing Summary</p>', unsafe_allow_html=True)

        cards = [
            (stats["input_rows"],    "Input rows",           "blue"),
            (stats["emails_found"],  "Emails found",         "blue"),
            (stats["unique_emails"], "Unique emails",        "green"),
            (stats["cross_dupes"],   "Cross-row duplicates", "amber"),
            (stats["dup_count"],     "Seen in 2+ rows",      "red"),
            (stats["output_rows"],   "Output rows",          "green"),
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
            dropped_str = ", ".join(f"`{c}`" for c in stats["dropped_cols"])
            st.markdown(
                f"<div class='success-box'>✅ Auto-removed source columns: {dropped_str}</div>",
                unsafe_allow_html=True,
            )

        # ── Data Management ───────────────────────────────────────────────────
        st.markdown('<p class="section-header">🛠️ Data Management</p>', unsafe_allow_html=True)

        tab_cols, tab_rows, tab_rename, tab_verified = st.tabs([
            "🗂️ Delete Columns",
            "🗑️ Delete Rows",
            "✏️ Rename Columns",
            "🔵 Filter by Verified",
        ])

        # Delete columns
        with tab_cols:
            st.markdown(
                "<div class='warn-box'>⚠️ Protected columns "
                "(<b>PRIMARY_EMAIL</b>, <b>IS_VERIFIED</b>, <b>EMAIL_SOURCE</b>) cannot be deleted.</div>",
                unsafe_allow_html=True,
            )
            deletable = [c for c in result_df.columns if c not in PROTECTED_COLS]
            to_delete = st.multiselect("Select columns to delete", options=deletable,
                                       placeholder="Choose one or more columns…")
            if st.button("🗑️ Delete Selected Columns", disabled=len(to_delete) == 0,
                         key="del_cols_btn"):
                st.session_state.result_df = result_df.drop(columns=to_delete)
                st.success(f"Deleted: {', '.join(to_delete)}")
                st.rerun()
            st.markdown(
                f"<div class='info-box'>Current column count: <b>{len(result_df.columns)}</b></div>",
                unsafe_allow_html=True,
            )

        # Delete rows
        with tab_rows:
            method = st.radio(
                "Delete rows by:",
                ["Row index", "Email address", "Filter condition"],
                horizontal=True,
            )
            st.markdown("")

            if method == "Row index":
                max_idx = len(result_df) - 1
                st.markdown(f"Valid range: **0 – {max_idx}**")
                idx_input = st.text_input("Indices (comma-separated, e.g. 0, 5, 12)")
                if st.button("🗑️ Delete by Index",
                             disabled=not idx_input.strip(), key="del_idx_btn"):
                    try:
                        indices = [int(x.strip()) for x in idx_input.split(",") if x.strip()]
                        invalid = [i for i in indices if i < 0 or i > max_idx]
                        if invalid:
                            st.error(f"Out-of-range indices: {invalid}  (valid 0–{max_idx})")
                        else:
                            st.session_state.result_df = (
                                result_df.drop(index=indices).reset_index(drop=True)
                            )
                            st.success(f"Deleted {len(indices)} row(s).")
                            st.rerun()
                    except ValueError:
                        st.error("Enter valid integers separated by commas.")

            elif method == "Email address":
                emails_input = st.text_area("Emails to delete (one per line or comma-separated)")
                if st.button("🗑️ Delete by Email",
                             disabled=not emails_input.strip(), key="del_email_btn"):
                    raw_emails    = re.split(r"[\n,]+", emails_input)
                    target_emails = {e.strip().lower() for e in raw_emails if e.strip()}
                    mask          = result_df["PRIMARY_EMAIL"].str.lower().isin(target_emails)
                    n             = mask.sum()
                    if n == 0:
                        st.warning("None of those emails were found.")
                    else:
                        st.session_state.result_df = result_df[~mask].reset_index(drop=True)
                        st.success(f"Deleted {n} row(s).")
                        st.rerun()

            elif method == "Filter condition":
                fc = st.selectbox("Column", list(result_df.columns), key="fc_col")
                fo = st.selectbox(
                    "Condition",
                    ["equals", "contains", "does not contain", "is empty", "is not empty"],
                    key="fc_op",
                )
                fv = ""
                if fo not in ("is empty", "is not empty"):
                    fv = st.text_input("Value", key="fc_val")

                if st.button("🗑️ Delete Matching Rows",
                             disabled=(fo not in ("is empty", "is not empty") and not fv.strip()),
                             key="del_filter_btn"):
                    cd = result_df[fc].astype(str)
                    if   fo == "equals":          mask = cd.str.lower() == fv.strip().lower()
                    elif fo == "contains":        mask = cd.str.lower().str.contains(fv.strip().lower(), na=False)
                    elif fo == "does not contain":mask = ~cd.str.lower().str.contains(fv.strip().lower(), na=False)
                    elif fo == "is empty":        mask = result_df[fc].isna() | (cd.str.strip() == "") | (cd == "nan")
                    elif fo == "is not empty":    mask = ~(result_df[fc].isna() | (cd.str.strip() == "") | (cd == "nan"))
                    n = mask.sum()
                    if n == 0:
                        st.warning("No rows matched.")
                    else:
                        st.session_state.result_df = result_df[~mask].reset_index(drop=True)
                        st.success(f"Deleted {n} row(s).")
                        st.rerun()

        # Rename columns
        with tab_rename:
            st.markdown(
                "<div class='info-box'>✏️ Type a new name for any column. "
                "Leave blank to keep the original. Click <b>Apply Renames</b> when ready.</div>",
                unsafe_allow_html=True,
            )
            rename_map  = {}
            all_cols    = list(result_df.columns)
            half        = (len(all_cols) + 1) // 2
            rc1, rc2    = st.columns(2)
            with rc1:
                for col in all_cols[:half]:
                    nv = st.text_input(f"`{col}`", value="", placeholder=col, key=f"rn_{col}_l")
                    if nv.strip() and nv.strip() != col:
                        rename_map[col] = nv.strip()
            with rc2:
                for col in all_cols[half:]:
                    nv = st.text_input(f"`{col}`", value="", placeholder=col, key=f"rn_{col}_r")
                    if nv.strip() and nv.strip() != col:
                        rename_map[col] = nv.strip()

            if st.button("✏️ Apply Renames", disabled=len(rename_map) == 0, key="rename_btn"):
                existing   = set(result_df.columns) - set(rename_map.keys())
                collisions = [v for v in rename_map.values() if v in existing]
                if collisions:
                    st.error(f"Name(s) already exist: {collisions}.")
                else:
                    st.session_state.result_df = result_df.rename(columns=rename_map)
                    st.success(", ".join(f"`{k}` → `{v}`" for k, v in rename_map.items()))
                    st.rerun()

        # Filter by verified
        with tab_verified:
            if "IS_VERIFIED" not in result_df.columns:
                st.warning("IS_VERIFIED column not found.")
            else:
                ver_n   = int(result_df["IS_VERIFIED"].sum())
                unver_n = len(result_df) - ver_n
                va, vb  = st.columns(2)
                with va:
                    st.markdown(
                        f"<div class='stat-card green'><div class='stat-number'>{ver_n:,}</div>"
                        f"<div class='stat-label'>✅ Verified emails</div></div>",
                        unsafe_allow_html=True,
                    )
                with vb:
                    st.markdown(
                        f"<div class='stat-card amber'><div class='stat-number'>{unver_n:,}</div>"
                        f"<div class='stat-label'>⚠️ Unverified emails</div></div>",
                        unsafe_allow_html=True,
                    )
                st.markdown("")
                choice = st.radio(
                    "Apply filter to working dataset:",
                    ["Keep all (no filter)", "Keep verified only", "Keep unverified only"],
                    horizontal=True, key="ver_radio",
                )
                if st.button("Apply Filter",
                             disabled=(choice == "Keep all (no filter)"),
                             key="ver_filter_btn"):
                    if choice == "Keep verified only":
                        st.session_state.result_df = (
                            result_df[result_df["IS_VERIFIED"] == True].reset_index(drop=True)
                        )
                        st.success(f"Kept {ver_n:,} verified, removed {unver_n:,} unverified.")
                    else:
                        st.session_state.result_df = (
                            result_df[result_df["IS_VERIFIED"] == False].reset_index(drop=True)
                        )
                        st.success(f"Kept {unver_n:,} unverified, removed {ver_n:,} verified.")
                    st.rerun()

        # Refresh after any tab edits
        result_df = st.session_state.result_df

        # ── Preview & Search ─────────────────────────────────────────────────
        st.markdown('<p class="section-header">🔎 Preview & Search</p>', unsafe_allow_html=True)

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
            show_dupes_only = st.checkbox("🔴 Dupes only", key="dupes_chk")

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
            f"<div class='info-box'>Showing <b>{len(preview_df):,}</b> of "
            f"<b>{len(result_df):,}</b> rows{' (filtered)' if is_filtered else ' (all)'}</div>",
            unsafe_allow_html=True,
        )

        default_cols = ["PRIMARY_EMAIL", "IS_VERIFIED", "EMAIL_SOURCE",
                        "DUPLICATE_FLAG", "OCCURRENCE_COUNT",
                        "FIRST_NAME", "LAST_NAME", "COMPANY_NAME", "JOB_TITLE"]
        display_cols = [c for c in default_cols if c in preview_df.columns]
        show_100 = preview_df[display_cols].head(100)

        if "DUPLICATE_FLAG" in show_100.columns:
            styled = show_100.style.apply(highlight_duplicates, axis=1)
            st.dataframe(styled, use_container_width=True, height=380)
        else:
            st.dataframe(show_100, use_container_width=True, height=380)

        with st.expander("🔍 Show all columns (first 20 rows)"):
            st.dataframe(preview_df.head(20), use_container_width=True, height=300)

        st.markdown(
            f"<div class='info-box'>Working dataset: "
            f"<b>{len(result_df):,} rows</b> × <b>{len(result_df.columns)} columns</b></div>",
            unsafe_allow_html=True,
        )

        # ── Download ─────────────────────────────────────────────────────────
        st.markdown('<p class="section-header">💾 Download</p>', unsafe_allow_html=True)
        original_name = st.session_state.original_name

        dl1, dl2, dl3 = st.columns(3)
        with dl1:
            st.download_button(
                label=f"⬇️ Full output ({len(result_df):,} rows)",
                data=df_to_csv_bytes(result_df),
                file_name=f"{original_name}_email_expanded.csv",
                mime="text/csv", key="dl_full",
            )
        with dl2:
            if "IS_VERIFIED" in result_df.columns:
                ver_df = result_df[result_df["IS_VERIFIED"] == True]
                if not ver_df.empty:
                    st.download_button(
                        label=f"⬇️ Verified only ({len(ver_df):,} rows)",
                        data=df_to_csv_bytes(ver_df),
                        file_name=f"{original_name}_verified_only.csv",
                        mime="text/csv", key="dl_verified",
                    )
                else:
                    st.info("No verified emails.")
        with dl3:
            if "DUPLICATE_FLAG" in result_df.columns:
                dup_df = result_df[result_df["DUPLICATE_FLAG"] == True]
                if not dup_df.empty:
                    st.download_button(
                        label=f"⬇️ Duplicates ({len(dup_df):,} rows)",
                        data=df_to_csv_bytes(dup_df),
                        file_name=f"{original_name}_duplicates.csv",
                        mime="text/csv", key="dl_dupes",
                    )
                else:
                    st.info("No duplicate emails.")

        if is_filtered:
            st.markdown("")
            st.download_button(
                label=f"⬇️ Download filtered view ({len(preview_df):,} rows)",
                data=df_to_csv_bytes(preview_df),
                file_name=f"{original_name}_filtered.csv",
                mime="text/csv", key="dl_filtered",
            )


# ═════════════════════════════════════════════════════════════════════════════
#  TOOL 2 — BULK DEDUPLICATOR
# ═════════════════════════════════════════════════════════════════════════════

elif active == "dedup":

    # ── Settings ─────────────────────────────────────────────────────────────
    set_col1, set_col2 = st.columns([2, 1])
    with set_col1:
        st.markdown('<p class="section-header">📂 Upload CSV Files</p>', unsafe_allow_html=True)
        uploaded_files = st.file_uploader(
            "Drop your CSV files here (100+ files, 30–40 MB each supported)",
            type=["csv"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )
    with set_col2:
        st.markdown('<p class="section-header">⚙️ Settings</p>', unsafe_allow_html=True)
        chunk_size = st.number_input(
            "Chunk size (rows per read)",
            min_value=10_000, max_value=200_000,
            value=CHUNK_SIZE, step=10_000,
            help="Lower = less RAM, slower. Higher = more RAM, faster.",
        )
        show_preview = st.toggle("Show output previews", value=True)

    if not uploaded_files:
        st.markdown("""
<div class='empty-state'>
  <div class='icon'>⬆️</div>
  <p>Upload one or more CSV files to begin.<br>
  Each file must contain a <b>PRIMARY_EMAIL</b> column.</p>
</div>
""", unsafe_allow_html=True)

    else:
        # File summary metrics
        total_size_mb = sum(f.size for f in uploaded_files) / (1024 ** 2)
        m1, m2, m3 = st.columns(3)
        with m1:
            st.markdown(
                f"<div class='stat-card blue'><div class='stat-number'>{len(uploaded_files)}</div>"
                f"<div class='stat-label'>Files uploaded</div></div>",
                unsafe_allow_html=True,
            )
        with m2:
            st.markdown(
                f"<div class='stat-card blue'><div class='stat-number'>{total_size_mb:.1f} MB</div>"
                f"<div class='stat-label'>Total size</div></div>",
                unsafe_allow_html=True,
            )
        with m3:
            est_rows = int(total_size_mb / 0.00015 / 1000)
            st.markdown(
                f"<div class='stat-card purple'><div class='stat-number'>~{est_rows}K</div>"
                f"<div class='stat-label'>Est. rows</div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("---")

        if st.button("🚀 Run Deduplication", use_container_width=True):

            log_lines: list[str] = []

            def log(msg: str, kind: str = "ok"):
                icon = {"ok": "✔", "warn": "⚠", "err": "✖"}.get(kind, "•")
                log_lines.append(f'<span class="log-{kind}">{icon}  {msg}</span>')

            st.markdown('<p class="section-header">⚙️ Processing</p>', unsafe_allow_html=True)
            progress_bar  = st.progress(0)
            status_text   = st.empty()
            log_container = st.empty()

            # Step 1 — read all files
            dataframes: dict[str, pd.DataFrame] = {}
            total = len(uploaded_files)
            status_text.markdown("**Loading files into memory …**")

            for i, uf in enumerate(uploaded_files):
                progress_bar.progress((i + 1) / total * 0.3)
                status_text.markdown(
                    f"**Reading {i+1}/{total}** — `{uf.name}` ({uf.size/1e6:.1f} MB)"
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

            # Step 2 — build global best map
            status_text.markdown("**Pass 1 — building global email index …**")
            best_map = build_global_best(dataframes, progress_bar, status_text)
            log(f"Global index built — {len(best_map):,} unique emails found", "ok")

            # Step 3 — deduplicate
            status_text.markdown("**Pass 2 — deduplicating files …**")
            results, dedup_stats = deduplicate_files(dataframes, best_map, progress_bar, status_text)
            log(f"Deduplication complete — {len(results)} files processed", "ok")

            progress_bar.progress(1.0)
            status_text.markdown("**✅ Done**")
            log_container.markdown(
                f'<div class="log-box">{"<br>".join(log_lines)}</div>',
                unsafe_allow_html=True,
            )

            # Step 4 — summary stats
            st.markdown('<p class="section-header">📊 Results Summary</p>', unsafe_allow_html=True)

            total_original = sum(s["original"] for s in dedup_stats.values())
            total_kept     = sum(s["kept"]     for s in dedup_stats.values())
            total_removed  = sum(s["removed"]  for s in dedup_stats.values())

            r1, r2, r3, r4 = st.columns(4)
            stat_blocks = [
                (r1, len(results),      "Files processed",    "blue"),
                (r2, total_original,    "Total rows in",      "blue"),
                (r3, total_kept,        "Total rows out",     "green"),
                (r4, total_removed,     "Duplicates removed", "red"),
            ]
            for col, val, label, color in stat_blocks:
                with col:
                    st.markdown(
                        f"<div class='stat-card {color}'>"
                        f"<div class='stat-number'>{val:,}</div>"
                        f"<div class='stat-label'>{label}</div></div>",
                        unsafe_allow_html=True,
                    )

            # Reduction bar
            pct = total_removed / max(total_original, 1) * 100
            st.markdown(
                f"<div class='success-box'>🎯 Overall reduction: <b>{pct:.1f}%</b> — "
                f"{total_removed:,} duplicate rows eliminated across {len(results)} files.</div>",
                unsafe_allow_html=True,
            )

            # Per-file breakdown
            st.markdown("#### Per-file breakdown")
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
            st.dataframe(
                pd.DataFrame(summary_rows),
                use_container_width=True,
                hide_index=True,
            )

            # Previews
            if show_preview:
                st.markdown("#### Output previews")
                for fname, df_out in list(results.items())[:5]:
                    with st.expander(f"📄 {fname}  ({len(df_out):,} rows)"):
                        st.dataframe(df_out.head(50), use_container_width=True, hide_index=True)
                if len(results) > 5:
                    st.caption(f"… and {len(results)-5} more files (toggle preview off in sidebar to hide)")

            # Download ZIP
            st.markdown('<p class="section-header">📦 Download Results</p>', unsafe_allow_html=True)
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for fname, df_out in results.items():
                    csv_bytes = df_out.to_csv(index=False).encode("utf-8")
                    out_name  = fname if fname.lower().endswith(".csv") else fname + ".csv"
                    zf.writestr(out_name, csv_bytes)
            zip_buffer.seek(0)

            st.download_button(
                label="⬇️  Download all deduplicated CSVs as ZIP",
                data=zip_buffer,
                file_name="deduplicated_output.zip",
                mime="application/zip",
                use_container_width=True,
            )
