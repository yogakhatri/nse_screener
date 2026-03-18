#!/usr/bin/env python3
"""
NSE Screener — Streamlit Dashboard
====================================
Interactive web UI for browsing engine results, filtering stocks,
viewing individual analysis, and sector breakdowns.

Usage:
  streamlit run app.py
  streamlit run app.py -- --run-dir runs/2026-03-12
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── Configuration ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NSE Stock Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Helpers ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_run_data(run_dir: str) -> dict:
    """Load all data from a run directory."""
    rd = Path(run_dir)
    data = {"stocks": [], "leaderboard": None, "run_log": None,
            "bias_audit": None, "sector_summary": None, "buy_candidates": None}

    # Load stock JSONs
    for sf in sorted(rd.glob("stock_*.json")):
        try:
            stock = json.loads(sf.read_text())
            data["stocks"].append(stock)
        except (json.JSONDecodeError, IOError):
            continue

    # Load leaderboard
    lb_path = rd / "leaderboard.csv"
    if lb_path.exists():
        data["leaderboard"] = pd.read_csv(lb_path)

    # Load buy candidates
    bc_path = rd / "buy_candidates.csv"
    if bc_path.exists():
        data["buy_candidates"] = pd.read_csv(bc_path)

    # Load run log
    rl_path = rd / "run_log.json"
    if rl_path.exists():
        try:
            data["run_log"] = json.loads(rl_path.read_text())
        except json.JSONDecodeError:
            pass

    # Load bias audit
    ba_path = rd / "bias_audit.json"
    if ba_path.exists():
        try:
            data["bias_audit"] = json.loads(ba_path.read_text())
        except json.JSONDecodeError:
            pass

    # Load sector summary
    ss_path = rd / "sector_summary.csv"
    if ss_path.exists():
        data["sector_summary"] = pd.read_csv(ss_path)

    return data


def get_available_runs() -> list[str]:
    """Get list of available run directories."""
    runs_dir = PROJECT_ROOT / "runs"
    if not runs_dir.exists():
        return []
    return sorted(
        [d.name for d in runs_dir.iterdir() if d.is_dir() and (d / "run_log.json").exists()],
        reverse=True,
    )


def stock_to_row(stock: dict) -> dict:
    """Convert stock JSON to a flat row for the dataframe."""
    cards = stock.get("cards", {})
    return {
        "Ticker": stock.get("ticker", ""),
        "Name": stock.get("stock_name", ""),
        "Sector": stock.get("classification", {}).get("sector", ""),
        "Industry": stock.get("classification", {}).get("basic_industry", ""),
        "Score": stock.get("final_opportunity_score"),
        "Selection": stock.get("selection_score"),
        "Recommendation": stock.get("recommendation", ""),
        "Entry Signal": stock.get("entry_signal", ""),
        "Performance": cards.get("performance", {}).get("score"),
        "Valuation": cards.get("valuation", {}).get("score"),
        "Growth": cards.get("growth", {}).get("score"),
        "Profitability": cards.get("profitability", {}).get("score"),
        "Entry Point": cards.get("entry_point", {}).get("score"),
        "Red Flags": cards.get("red_flags", {}).get("score"),
        "Upside %": stock.get("expected_upside_pct"),
        "Risk/Reward": stock.get("risk_reward_ratio"),
        "Market Mode": stock.get("market_mode", ""),
        "Gate Passed": stock.get("investability_gate_passed", False),
    }


def score_color(val):
    """Return color based on score value."""
    if val is None or pd.isna(val):
        return "background-color: #f0f0f0"
    if val >= 70:
        return "background-color: #c6efce; color: #006100"
    if val >= 50:
        return "background-color: #ffeb9c; color: #9c6500"
    return "background-color: #ffc7ce; color: #9c0006"


def render_score_badge(score, label=None):
    """Render a colored score badge."""
    if score is None:
        return "—"
    color = "#c6efce" if score >= 70 else "#ffeb9c" if score >= 50 else "#ffc7ce"
    text_color = "#006100" if score >= 70 else "#9c6500" if score >= 50 else "#9c0006"
    lbl = f" ({label})" if label else ""
    return f'<span style="background:{color};color:{text_color};padding:2px 8px;border-radius:4px;font-weight:bold">{score:.1f}{lbl}</span>'


# ── Main App ───────────────────────────────────────────────────────────────
def main():
    st.title("📊 NSE Stock Screener")
    st.markdown("*Sector-wise Investment Engine V2*")

    # Sidebar: Run Selection
    with st.sidebar:
        st.header("🔧 Settings")

        available_runs = get_available_runs()
        if not available_runs:
            st.warning("No engine runs found. Run `make daily-run` first.")
            # Try to load from any run dir with stock files
            runs_dir = PROJECT_ROOT / "runs"
            if runs_dir.exists():
                available_runs = sorted(
                    [d.name for d in runs_dir.iterdir() if d.is_dir()],
                    reverse=True,
                )

        if not available_runs:
            st.error("No data available. Please run the engine first.")
            return

        selected_run = st.selectbox("Select Run Date", available_runs, index=0)
        run_dir = str(PROJECT_ROOT / "runs" / selected_run)

        st.divider()

        # Filters
        st.header("🔍 Filters")
        min_score = st.slider("Min Opportunity Score", 0, 100, 0)
        gate_filter = st.checkbox("Only Gate-Passed Stocks", value=False)
        rec_filter = st.multiselect(
            "Recommendation",
            ["Buy", "Hold", "Avoid", "Unsupported", "Watch"],
            default=[],
        )

    # Load data
    data = load_run_data(run_dir)
    stocks = data["stocks"]

    if not stocks:
        st.warning(f"No stock data found in {run_dir}")
        return

    # Convert to DataFrame
    rows = [stock_to_row(s) for s in stocks]
    df = pd.DataFrame(rows)

    # Apply filters
    if min_score > 0:
        df = df[df["Score"].fillna(0) >= min_score]
    if gate_filter:
        df = df[df["Gate Passed"] == True]
    if rec_filter:
        df = df[df["Recommendation"].isin(rec_filter)]

    # Sidebar stats
    with st.sidebar:
        st.divider()
        st.header("📈 Quick Stats")
        st.metric("Total Stocks", len(stocks))
        st.metric("Filtered", len(df))
        gate_passed = df["Gate Passed"].sum()
        st.metric("Gate Passed", int(gate_passed))
        buy_count = len(df[df["Recommendation"] == "Buy"])
        st.metric("Buy Candidates", buy_count)

    # Tabs
    tab_overview, tab_leaderboard, tab_stock, tab_sector, tab_quality = st.tabs(
        ["📋 Overview", "🏆 Leaderboard", "🔎 Stock Detail", "📊 Sector View", "⚙️ Run Quality"]
    )

    # ── Tab 1: Overview ──────────────────────────────────────────────
    with tab_overview:
        st.header("Market Overview")

        # Run log metrics
        run_log = data.get("run_log")
        if run_log:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Universe Size", run_log.get("universe_size", "—"))
            with col2:
                st.metric("Market Mode", run_log.get("market_mode", "—").title())
            with col3:
                st.metric("Rankable Stocks", run_log.get("rankable_count", "—"))
            with col4:
                st.metric("Runtime", f"{run_log.get('elapsed_sec', 0):.1f}s")

        # Score distribution
        st.subheader("Score Distribution")
        score_col = df["Score"].dropna()
        if not score_col.empty:
            import plotly.express as px
            fig = px.histogram(
                score_col, nbins=20,
                labels={"value": "Opportunity Score", "count": "Stocks"},
                title="Opportunity Score Distribution",
            )
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)

        # Recommendation breakdown
        st.subheader("Recommendations")
        rec_counts = df["Recommendation"].value_counts()
        if not rec_counts.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(
                    values=rec_counts.values, names=rec_counts.index,
                    title="Recommendation Breakdown",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.dataframe(
                    rec_counts.reset_index().rename(
                        columns={"index": "Recommendation", "Recommendation": "Recommendation", "count": "Count"}
                    ),
                    hide_index=True,
                )

    # ── Tab 2: Leaderboard ───────────────────────────────────────────
    with tab_leaderboard:
        st.header("🏆 Stock Leaderboard")

        sort_col = st.selectbox(
            "Sort by",
            ["Score", "Selection", "Performance", "Valuation", "Growth",
             "Profitability", "Entry Point", "Red Flags", "Upside %", "Risk/Reward"],
            index=0,
        )

        display_df = df.sort_values(sort_col, ascending=False, na_position="last")

        # Display columns
        show_cols = ["Ticker", "Name", "Sector", "Score", "Recommendation",
                     "Performance", "Valuation", "Growth", "Profitability",
                     "Entry Point", "Red Flags", "Upside %"]
        display_df_show = display_df[show_cols].head(100)

        st.dataframe(
            display_df_show.style.map(
                score_color,
                subset=["Score", "Performance", "Valuation", "Growth",
                        "Profitability", "Entry Point", "Red Flags"],
            ),
            use_container_width=True,
            height=600,
        )

        # Download button
        csv_data = display_df.to_csv(index=False)
        st.download_button("📥 Download Full Data", csv_data, "leaderboard.csv", "text/csv")

    # ── Tab 3: Stock Detail ──────────────────────────────────────────
    with tab_stock:
        st.header("🔎 Stock Analysis")

        # Stock selector
        tickers = sorted(df["Ticker"].unique())
        selected_ticker = st.selectbox("Select Stock", tickers, index=0 if tickers else None)

        if selected_ticker:
            # Find stock data
            stock_data = None
            for s in stocks:
                if s.get("ticker") == selected_ticker:
                    stock_data = s
                    break

            if stock_data:
                st.subheader(f"{stock_data.get('stock_name', selected_ticker)} ({selected_ticker})")

                # Classification
                cls = stock_data.get("classification", {})
                st.markdown(
                    f"**Sector:** {cls.get('sector', '—')} | "
                    f"**Industry:** {cls.get('basic_industry', '—')} | "
                    f"**Template:** {stock_data.get('template_used', '—')}"
                )

                # Key metrics row
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Opportunity Score", f"{stock_data.get('final_opportunity_score', 0):.1f}")
                with col2:
                    st.metric("Recommendation", stock_data.get("recommendation", "—"))
                with col3:
                    st.metric("Entry Signal", stock_data.get("entry_signal", "—"))
                with col4:
                    upside = stock_data.get("expected_upside_pct")
                    st.metric("Expected Upside", f"{upside:.1f}%" if upside else "—")
                with col5:
                    rr = stock_data.get("risk_reward_ratio")
                    st.metric("Risk/Reward", f"{rr:.2f}" if rr else "—")

                # Thesis
                thesis = stock_data.get("thesis")
                if thesis:
                    st.info(thesis)

                # Card scores radar-style
                st.subheader("Card Scores")
                cards = stock_data.get("cards", {})
                card_names = ["Performance", "Valuation", "Growth", "Profitability", "Entry Point", "Red Flags"]
                card_keys = ["performance", "valuation", "growth", "profitability", "entry_point", "red_flags"]

                cols = st.columns(6)
                for i, (name, key) in enumerate(zip(card_names, card_keys)):
                    with cols[i]:
                        card = cards.get(key, {})
                        score = card.get("score")
                        label = card.get("label", "—")
                        if score is not None:
                            st.metric(name, f"{score:.1f}", label)
                        else:
                            st.metric(name, "—", label)

                # Sub-scores detail
                st.subheader("Detailed Sub-Scores")
                for card_name, card_key in zip(card_names, card_keys):
                    card = cards.get(card_key, {})
                    subs = card.get("sub_scores", {})
                    if subs:
                        with st.expander(f"{card_name} ({card.get('score', '—')})"):
                            sub_df = pd.DataFrame([
                                {"Metric": k.replace("_", " ").title(), "Score": v}
                                for k, v in subs.items()
                            ])
                            st.dataframe(
                                sub_df.style.map(
                                    score_color, subset=["Score"]
                                ),
                                hide_index=True,
                                use_container_width=True,
                            )
                            if card.get("reason"):
                                st.caption(card["reason"])

                # Gate status
                st.subheader("Investability Gate")
                if stock_data.get("investability_gate_passed"):
                    st.success("✅ Gate PASSED")
                else:
                    st.error("❌ Gate FAILED")
                    reasons = stock_data.get("gate_fail_reasons", [])
                    for r in reasons:
                        st.markdown(f"- {r}")

                # Raw JSON
                with st.expander("📄 Raw JSON Data"):
                    st.json(stock_data)

    # ── Tab 4: Sector View ───────────────────────────────────────────
    with tab_sector:
        st.header("📊 Sector Analysis")

        sector_df = data.get("sector_summary")
        if sector_df is not None and not sector_df.empty:
            st.dataframe(sector_df, use_container_width=True, height=400)
        else:
            # Build sector summary from stocks
            sector_stats = df.groupby("Sector").agg(
                Count=("Ticker", "count"),
                Avg_Score=("Score", "mean"),
                Top_Score=("Score", "max"),
                Buy_Count=("Recommendation", lambda x: (x == "Buy").sum()),
                Gate_Passed=("Gate Passed", "sum"),
            ).sort_values("Avg_Score", ascending=False)

            st.dataframe(sector_stats, use_container_width=True, height=400)

        # Sector score comparison
        st.subheader("Sector Score Comparison")
        sector_avg = df.groupby("Sector")["Score"].mean().dropna().sort_values(ascending=True)
        if not sector_avg.empty:
            import plotly.express as px
            fig = px.bar(
                x=sector_avg.values, y=sector_avg.index,
                orientation="h",
                labels={"x": "Average Score", "y": "Sector"},
                title="Average Opportunity Score by Sector",
            )
            fig.update_layout(height=max(300, len(sector_avg) * 25))
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 5: Run Quality ───────────────────────────────────────────
    with tab_quality:
        st.header("⚙️ Run Quality & Diagnostics")

        run_log = data.get("run_log")
        if run_log:
            st.subheader("Run Metadata")
            st.json(run_log)

        bias_audit = data.get("bias_audit")
        if bias_audit:
            st.subheader("Bias Audit")
            st.json(bias_audit)

        # Coverage analysis
        st.subheader("Card Coverage")
        coverage_data = []
        for s in stocks:
            cards = s.get("cards", {})
            for card_key in ["performance", "valuation", "growth", "profitability", "entry_point", "red_flags"]:
                card = cards.get(card_key, {})
                coverage_data.append({
                    "Card": card_key.replace("_", " ").title(),
                    "Rankable": card.get("is_rankable", False),
                    "Coverage": card.get("data_coverage", "0%"),
                })

        cov_df = pd.DataFrame(coverage_data)
        if not cov_df.empty:
            cov_summary = cov_df.groupby("Card").agg(
                Rankable_Pct=("Rankable", lambda x: f"{x.mean()*100:.1f}%"),
                Total=("Rankable", "count"),
            )
            st.dataframe(cov_summary, use_container_width=True)

        # Input quality
        iq_path = Path(run_dir) / "input_quality.json"
        if iq_path.exists():
            st.subheader("Input Quality")
            try:
                st.json(json.loads(iq_path.read_text()))
            except json.JSONDecodeError:
                pass


if __name__ == "__main__":
    main()
