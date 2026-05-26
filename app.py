#!/usr/bin/env python3
"""
NSE Screener — Streamlit Dashboard
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from dashboard_filters import (
    SKIP_FILTER_COLUMNS,
    SCORE_STYLE_COLUMNS,
    apply_column_filters,
    detect_column_filter_kind,
    leaderboard_to_display_df,
    score_color,
)

st.set_page_config(
    page_title="NSE Stock Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

RECOMMENDATION_ALIASES = {
    "buy": "Buy Candidate",
    "buy candidate": "Buy Candidate",
    "hold": "Watchlist",
    "watch": "Watchlist",
    "watchlist": "Watchlist",
    "avoid": "Avoid",
    "unsupported": "Unsupported",
}
RECOMMENDATION_ORDER = ["Buy Candidate", "Watchlist", "Avoid", "Unsupported", "Insufficient Data"]


def normalize_recommendation(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return RECOMMENDATION_ALIASES.get(text.lower(), text) if text else ""


@st.cache_data(ttl=300)
def load_run_data(run_dir: str) -> dict:
    rd = Path(run_dir)
    if not rd.is_absolute():
        rd = PROJECT_ROOT / run_dir
    data: dict = {
        "run_dir": str(rd),
        "stocks": [],
        "leaderboard": None,
        "user_filtered_leaderboard": None,
        "action_sheet": None,
        "buy_candidates": None,
        "top_picks": None,
        "top_picks_next": None,
        "search_summary": None,
        "macro_context": None,
        "analyst_queue": None,
        "run_log": None,
        "bias_audit": None,
        "sector_summary": None,
        "source_registry": None,
        "metric_provenance": None,
        "data_quality_summary": None,
    }
    for sf in sorted(rd.glob("stock_*.json")):
        try:
            data["stocks"].append(json.loads(sf.read_text()))
        except (json.JSONDecodeError, OSError):
            continue
    for key, fname in [
        ("leaderboard", "leaderboard.csv"),
        ("user_filtered_leaderboard", "user_filtered_leaderboard.csv"),
        ("action_sheet", "action_sheet.csv"),
        ("buy_candidates", "buy_candidates.csv"),
        ("top_picks", "top_picks.csv"),
        ("top_picks_next", "top_picks_next_tier.csv"),
        ("sector_summary", "sector_summary.csv"),
        ("source_registry", "source_registry.csv"),
        ("metric_provenance", "metric_provenance.csv"),
        ("data_quality_summary", "data_quality_summary.csv"),
        ("analyst_queue", "analyst_research_queue.csv"),
    ]:
        p = rd / fname
        if p.exists():
            data[key] = pd.read_csv(p)
    for jname, key in [
        ("run_log.json", "run_log"),
        ("search_summary.json", "search_summary"),
        ("macro_context.json", "macro_context"),
        ("bias_audit.json", "bias_audit"),
    ]:
        p = rd / jname
        if p.exists():
            try:
                data[key] = json.loads(p.read_text())
            except json.JSONDecodeError:
                pass
    iq = rd / "input_quality.json"
    if iq.exists():
        try:
            data["input_quality"] = json.loads(iq.read_text())
        except json.JSONDecodeError:
            pass
    return data


def discover_runs() -> list[tuple[str, str]]:
    """Return (label, path) for each runnable output folder."""
    runs_dir = PROJECT_ROOT / "runs"
    if not runs_dir.exists():
        return []
    found: list[tuple[str, str]] = []
    for d in sorted(runs_dir.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        profiles = d / "profiles"
        if profiles.is_dir():
            for p in sorted(profiles.iterdir(), reverse=True):
                if p.is_dir() and (p / "leaderboard.csv").exists():
                    found.append((f"{d.name} / {p.name}", str(p.relative_to(PROJECT_ROOT))))
        if (d / "leaderboard.csv").exists() or list(d.glob("stock_*.json")):
            found.append((d.name, str(d.relative_to(PROJECT_ROOT))))
    seen = set()
    out: list[tuple[str, str]] = []
    for label, path in found:
        if path not in seen:
            seen.add(path)
            out.append((label, path))
    return out


def build_display_frame(data: dict) -> pd.DataFrame:
    """Prefer full leaderboard CSV; fall back to stock JSON summaries."""
    lb = data.get("user_filtered_leaderboard")
    if lb is None or (isinstance(lb, pd.DataFrame) and lb.empty):
        lb = data.get("leaderboard")
    if isinstance(lb, pd.DataFrame) and not lb.empty:
        df = leaderboard_to_display_df(lb.copy())
        if "recommendation" in lb.columns:
            df["Recommendation"] = lb["recommendation"].map(normalize_recommendation)
        top = data.get("top_picks")
        if isinstance(top, pd.DataFrame) and not top.empty and "ticker" in top.columns:
            top_set = set(top["ticker"].astype(str).str.upper())
            df["Top Pick"] = df["Ticker"].astype(str).str.upper().isin(top_set)
        else:
            df["Top Pick"] = False
        return df
    rows = []
    for stock in data.get("stocks") or []:
        cards = stock.get("cards", {})
        rows.append({
            "Ticker": stock.get("ticker", ""),
            "Name": stock.get("stock_name", ""),
            "Sector": (stock.get("classification") or {}).get("sector", ""),
            "Industry": (stock.get("classification") or {}).get("basic_industry", ""),
            "Score": stock.get("final_opportunity_score"),
            "Selection": stock.get("selection_score"),
            "Recommendation": normalize_recommendation(stock.get("recommendation")),
            "Research Status": stock.get("research_status", ""),
            "Research Tier": stock.get("research_tier", ""),
            "Data Quality": stock.get("data_quality_status", ""),
            "Gate Passed": stock.get("investability_gate_passed", False),
            "Performance": (cards.get("performance") or {}).get("score"),
            "Valuation": (cards.get("valuation") or {}).get("score"),
            "Growth": (cards.get("growth") or {}).get("score"),
            "Profitability": (cards.get("profitability") or {}).get("score"),
            "Red Flags": (cards.get("red_flags") or {}).get("score"),
            "Upside %": stock.get("expected_upside_pct"),
            "Risk/Reward": stock.get("risk_reward_ratio"),
        })
    return pd.DataFrame(rows)


def _filter_state_key(run_dir: str) -> str:
    return f"column_filters::{run_dir}"


def render_column_filter_widgets(df: pd.DataFrame, run_dir: str) -> dict:
    """Build sidebar widgets; return filter spec dict."""
    if df.empty:
        return {}
    key = _filter_state_key(run_dir)
    if key not in st.session_state:
        st.session_state[key] = {}

    filterable = [
        c for c in df.columns
        if c not in SKIP_FILTER_COLUMNS and detect_column_filter_kind(df[c], c) != "skip"
    ]
    chosen = st.multiselect(
        "Columns to filter",
        filterable,
        default=st.session_state.get(f"{key}::chosen", [])[:5],
        key=f"{key}::chosen_select",
    )
    st.session_state[f"{key}::chosen"] = chosen

    specs: dict = {}
    for col in chosen:
        kind = detect_column_filter_kind(df[col], col)
        with st.container():
            st.caption(f"**{col}**")
            if kind == "boolean":
                opt = st.selectbox(
                    f"{col}",
                    ["Any", "Yes", "No"],
                    key=f"{key}::bool::{col}",
                    label_visibility="collapsed",
                )
                if opt == "Yes":
                    specs[col] = {"kind": "boolean", "value": True}
                elif opt == "No":
                    specs[col] = {"kind": "boolean", "value": False}
            elif kind == "categorical":
                values = sorted(df[col].dropna().astype(str).unique().tolist())
                selected = st.multiselect(
                    f"Values",
                    values,
                    default=values,
                    key=f"{key}::cat::{col}",
                    label_visibility="collapsed",
                )
                if selected and len(selected) < len(values):
                    specs[col] = {"kind": "categorical", "values": selected}
            elif kind == "numeric":
                series = pd.to_numeric(df[col], errors="coerce").dropna()
                if series.empty:
                    continue
                lo, hi = float(series.min()), float(series.max())
                rng = st.slider(
                    f"Range",
                    lo,
                    hi,
                    (lo, hi),
                    key=f"{key}::num::{col}",
                    label_visibility="collapsed",
                )
                if rng[0] > lo or rng[1] < hi:
                    specs[col] = {"kind": "numeric", "min": rng[0], "max": rng[1]}
            else:
                txt = st.text_input(
                    f"Contains",
                    "",
                    key=f"{key}::txt::{col}",
                    label_visibility="collapsed",
                )
                if txt.strip():
                    specs[col] = {"kind": "text", "contains": txt.strip()}
    st.session_state[key] = specs
    return specs


def apply_quick_filters(df: pd.DataFrame, preset: str) -> pd.DataFrame:
    if preset == "gate_passed":
        return df[df["Gate Passed"] == True] if "Gate Passed" in df.columns else df
    if preset == "buy_only":
        return df[df["Recommendation"] == "Buy Candidate"] if "Recommendation" in df.columns else df
    if preset == "watchlist":
        return df[df["Recommendation"] == "Watchlist"] if "Recommendation" in df.columns else df
    if preset == "top_picks":
        return df[df["Top Pick"] == True] if "Top Pick" in df.columns else df
    if preset == "gate_failed_watchlist":
        if "Recommendation" in df.columns and "Gate Passed" in df.columns:
            return df[(df["Recommendation"] == "Watchlist") & (df["Gate Passed"] != True)]
        return df
    if preset == "high_confidence":
        if "Research Tier" in df.columns:
            return df[df["Research Tier"] == "High Confidence Research"]
        return df
    return df


def style_dataframe(display: pd.DataFrame):
    cols = [c for c in display.columns if c in SCORE_STYLE_COLUMNS]
    if cols:
        return display.style.map(score_color, subset=cols)
    return display


def main() -> None:
    st.title("NSE Long-Term Stock Research")
    st.caption("Research assistant — not investment advice. Gates are never relaxed in the engine.")

    runs = discover_runs()
    if not runs:
        st.error("No runs found. Run `make demo-run` or `make daily-run` first.")
        return

    with st.sidebar:
        st.header("Run")
        labels = [r[0] for r in runs]
        idx = st.selectbox("Output folder", range(len(labels)), format_func=lambda i: labels[i])
        run_dir = runs[idx][1]
        data = load_run_data(run_dir)

        st.divider()
        st.header("Quick filters")
        min_score = st.slider("Min opportunity score", 0, 100, 0)
        gate_only = st.checkbox("Gate passed only", value=False)
        base_for_recs = build_display_frame(data)
        recs = sorted(
            {normalize_recommendation(x) for x in base_for_recs.get("Recommendation", pd.Series()).unique()}
        )
        recs = [r for r in RECOMMENDATION_ORDER if r in recs] + [r for r in recs if r and r not in RECOMMENDATION_ORDER]
        rec_filter = st.multiselect("Recommendation", recs, default=[])

        preset_key = f"preset::{run_dir}"
        if preset_key not in st.session_state:
            st.session_state[preset_key] = "none"
        st.caption("Presets")
        p1, p2 = st.columns(2)
        with p1:
            if st.button("Top picks", use_container_width=True):
                st.session_state[preset_key] = "top_picks"
            if st.button("Buy only", use_container_width=True):
                st.session_state[preset_key] = "buy_only"
        with p2:
            if st.button("Gate passed", use_container_width=True):
                st.session_state[preset_key] = "gate_passed"
            if st.button("Watch + gate fail", use_container_width=True):
                st.session_state[preset_key] = "gate_failed_watchlist"
        if st.button("Clear preset", use_container_width=True):
            st.session_state[preset_key] = "none"
        preset = st.session_state[preset_key]

        st.divider()
        with st.expander("Column filters", expanded=False):
            st.caption("Filter by values in each column (like Excel).")
            col_specs = render_column_filter_widgets(
                build_display_frame(data), run_dir
            )

        st.divider()
        st.header("Summary")
        base_df = build_display_frame(data)
        filtered = base_df.copy()
        if min_score > 0 and "Score" in filtered.columns:
            filtered = filtered[filtered["Score"].fillna(0) >= min_score]
        if gate_only and "Gate Passed" in filtered.columns:
            filtered = filtered[filtered["Gate Passed"] == True]
        if rec_filter and "Recommendation" in filtered.columns:
            filtered = filtered[filtered["Recommendation"].isin(rec_filter)]
        if preset != "none":
            filtered = apply_quick_filters(filtered, preset)
        filtered = apply_column_filters(filtered, col_specs)
        st.metric("Rows shown", len(filtered))
        st.metric("Universe", len(base_df))
        if "Gate Passed" in filtered.columns:
            st.metric("Gate passed", int(filtered["Gate Passed"].sum()))

    df = build_display_frame(data)
    if min_score > 0 and "Score" in df.columns:
        df = df[df["Score"].fillna(0) >= min_score]
    if gate_only and "Gate Passed" in df.columns:
        df = df[df["Gate Passed"] == True]
    if rec_filter and "Recommendation" in df.columns:
        df = df[df["Recommendation"].isin(rec_filter)]
    if preset != "none":
        df = apply_quick_filters(df, preset)
    df = apply_column_filters(df, col_specs)

    tabs = st.tabs([
        "Overview",
        "Top picks",
        "Research table",
        "Stock detail",
        "Analyst queue",
        "Sector",
        "Run quality",
    ])

    with tabs[0]:
        st.subheader("Overview")
        macro = data.get("macro_context") or (data.get("search_summary") or {}).get("macro_context")
        if macro:
            st.info(macro.get("regime_guidance", ""))
            with st.expander("Macro & shock checklist (manual)"):
                for line in macro.get("manual_shock_checklist", []):
                    st.markdown(f"- {line}")
                st.caption(macro.get("shock_event_modeling", ""))
        run_log = data.get("run_log")
        if run_log:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Universe", run_log.get("universe_size", "—"))
            c2.metric("Market mode", str(run_log.get("market_mode", "—")).title())
            c3.metric("Rankable", run_log.get("rankable_count", "—"))
            c4.metric("Runtime (s)", f"{run_log.get('elapsed_sec', 0):.1f}")
        if "Score" in df.columns and not df["Score"].dropna().empty:
            fig = px.histogram(df["Score"].dropna(), nbins=20, labels={"value": "Score", "count": "Count"})
            fig.update_layout(height=280, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        if "Recommendation" in df.columns:
            rc = df["Recommendation"].value_counts()
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(
                    px.pie(values=rc.values, names=rc.index, title="Recommendations"),
                    use_container_width=True,
                )
            with c2:
                st.dataframe(rc.reset_index().rename(columns={"index": "Label", "Recommendation": "Count"}), hide_index=True)

    with tabs[1]:
        st.subheader("Top picks (engine shortlist)")
        sm = (data.get("search_summary") or {}).get("summary") or {}
        if sm:
            st.write(
                f"Mode: **{sm.get('mode_label', sm.get('research_mode', '—'))}** · "
                f"Persona: **{sm.get('return_persona', '—')}** · "
                f"Primary: **{sm.get('primary_count', 0)}** · Next: **{sm.get('secondary_count', 0)}**"
            )
            if sm.get("empty_reason"):
                st.warning(sm["empty_reason"])
        for title, key in [("Primary (up to 3)", "top_picks"), ("Next tier", "top_picks_next")]:
            part = data.get(key)
            if isinstance(part, pd.DataFrame) and not part.empty:
                st.markdown(f"**{title}**")
                show = [c for c in [
                    "ticker", "name", "sector", "recommendation", "research_tier",
                    "selection_score", "gate_passed", "shortlist_caveat", "missing_critical_fields",
                ] if c in part.columns]
                st.dataframe(part[show], hide_index=True, use_container_width=True)

    with tabs[2]:
        st.subheader("Research table")
        st.caption(f"{len(df)} rows after sidebar filters. Use **Column filters** in the sidebar for per-column values.")
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            sort_options = [
                c for c in df.columns
                if c in SCORE_STYLE_COLUMNS or c in {"Ticker", "Selection", "Upside %"}
            ]
            default_sort = "Score" if "Score" in sort_options else (sort_options[0] if sort_options else "Ticker")
            sort_by = st.selectbox(
                "Sort by",
                sort_options or ["Ticker"],
                index=(sort_options.index(default_sort) if default_sort in sort_options else 0),
            )
        with c2:
            row_limit = st.number_input("Max rows", 10, 5000, min(100, max(100, len(df))), step=50)
        with c3:
            show_gate_cols = st.checkbox("Show gate / tier columns", value=True)

        default_cols = [
            "Ticker", "Name", "Sector", "Score", "Selection", "Recommendation",
            "Research Tier", "Research Status", "Gate Passed", "Data Quality",
            "Performance", "Valuation", "Growth", "Profitability", "Red Flags", "Upside %",
        ]
        if show_gate_cols:
            default_cols += ["Gate Fail Reasons", "Missing Critical Fields", "Value Trap", "Top Pick"]
        visible = [c for c in default_cols if c in df.columns]
        extra = st.multiselect(
            "Additional columns",
            [c for c in df.columns if c not in visible],
            default=[],
        )
        visible = visible + [c for c in extra if c in df.columns]
        table = df.sort_values(sort_by, ascending=False, na_position="last")[visible].head(int(row_limit))
        st.dataframe(style_dataframe(table), use_container_width=True, height=620)
        st.download_button(
            "Download filtered CSV",
            df.to_csv(index=False),
            file_name="filtered_research.csv",
            mime="text/csv",
        )

    with tabs[3]:
        st.subheader("Stock detail")
        tickers = sorted(df["Ticker"].dropna().unique()) if "Ticker" in df.columns else []
        if not tickers:
            st.info("No tickers in current filter. Widen sidebar filters.")
        else:
            ticker = st.selectbox("Ticker", tickers)
            stock = next((s for s in data["stocks"] if s.get("ticker") == ticker), None)
            if not stock:
                st.warning("No JSON detail for this ticker.")
            else:
                st.markdown(f"### {stock.get('stock_name', ticker)}")
                cls = stock.get("classification") or {}
                st.write(
                    f"**Sector:** {cls.get('sector', '—')} · **Industry:** {cls.get('basic_industry', '—')} · "
                    f"**Template:** {stock.get('template_used', '—')}"
                )
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Score", stock.get("final_opportunity_score"))
                m2.metric("Recommendation", normalize_recommendation(stock.get("recommendation")))
                m3.metric("Research tier", stock.get("research_tier", "—"))
                m4.metric("Upside %", stock.get("expected_upside_pct"))
                m5.metric("Gate", "Pass" if stock.get("investability_gate_passed") else "Fail")
                cards = stock.get("cards") or {}
                card_cols = st.columns(6)
                for i, key in enumerate(["performance", "valuation", "growth", "profitability", "entry_point", "red_flags"]):
                    card = cards.get(key) or {}
                    card_cols[i].metric(key.replace("_", " ").title(), card.get("score"), card.get("label"))
                if stock.get("gate_fail_reasons"):
                    st.error("Gate failures")
                    for r in stock["gate_fail_reasons"]:
                        st.markdown(f"- {r}")
                if stock.get("recommendation_risk_flags"):
                    st.warning("Risk flags")
                    for r in stock["recommendation_risk_flags"]:
                        st.markdown(f"- {r}")
                with st.expander("Raw JSON"):
                    st.json(stock)

    with tabs[4]:
        st.subheader("Analyst research queue")
        st.caption("Manual worksheet: docs/ANALYST_WORKSHEET_TEMPLATE.md")
        aq = data.get("analyst_queue")
        if isinstance(aq, pd.DataFrame) and not aq.empty:
            st.dataframe(aq, use_container_width=True, hide_index=True, height=400)
        else:
            st.info("Re-run engine to generate `analyst_research_queue.csv`.")
        action = data.get("action_sheet")
        if isinstance(action, pd.DataFrame) and not action.empty:
            with st.expander("Full action sheet"):
                st.dataframe(action, use_container_width=True, height=360)

    with tabs[5]:
        st.subheader("Sector view")
        if "Sector" in df.columns and "Score" in df.columns:
            sec = df.groupby("Sector").agg(
                Count=("Ticker", "count"),
                Avg_Score=("Score", "mean"),
                Buy=("Recommendation", lambda s: (s == "Buy Candidate").sum()),
            ).sort_values("Avg_Score", ascending=False)
            st.dataframe(sec, use_container_width=True)
            avg = df.groupby("Sector")["Score"].mean().dropna().sort_values()
            if not avg.empty:
                fig = px.bar(x=avg.values, y=avg.index, orientation="h", labels={"x": "Avg score", "y": ""})
                fig.update_layout(height=max(300, len(avg) * 22))
                st.plotly_chart(fig, use_container_width=True)

    with tabs[6]:
        st.subheader("Run quality")
        if data.get("run_log"):
            with st.expander("Run log"):
                st.json(data["run_log"])
        if isinstance(data.get("data_quality_summary"), pd.DataFrame):
            st.markdown("**Data quality**")
            st.dataframe(data["data_quality_summary"], use_container_width=True, hide_index=True)
        iq = data.get("input_quality")
        if iq:
            with st.expander("Input quality"):
                st.json(iq)
        bc = data.get("buy_candidates")
        st.metric("Buy candidates (strict)", len(bc) if isinstance(bc, pd.DataFrame) else 0)


if __name__ == "__main__":
    main()
