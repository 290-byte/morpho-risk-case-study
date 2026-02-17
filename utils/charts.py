"""
Reusable Plotly chart configurations - Morpho-inspired light theme.
"""

import plotly.express as px
import plotly.graph_objects as go

# -- Theme constants (Morpho-inspired) -----------------------
BG_COLOR = "#FFFFFF"
GRID_COLOR = "rgba(0,0,0,0.06)"
TEXT_COLOR = "#000000"
MUTED_TEXT = "#000000"
ACCENT = "#2470FF"
RED = "#DC2626"
GREEN = "#16A34A"
YELLOW = "#CA8A04"
ORANGE = "#EA580C"
BLUE = "#2470FF"
PURPLE = "#7C3AED"
LIGHT_BLUE = "#93C5FD"

SEVERITY_COLORS = {
    "critical": RED,
    "warning": ORANGE,
    "positive": GREEN,
    "info": BLUE,
}

RESPONSE_COLORS = {
    "PROACTIVE": GREEN,
    "EARLY_REACTOR": BLUE,
    "DURING_DEPEG": "#F59E0B",
    "SLOW_REACTOR": YELLOW,
    "VERY_LATE": RED,
    "STAYED_EXPOSED": "#991B1B",
    "EXITED_TIMING_UNKNOWN": "#9CA3AF",
    "NO_EXIT": "#991B1B",
}


def apply_layout(fig, title=None, height=400, show_legend=True):
    """Apply Morpho light theme layout to any Plotly figure."""
    fig.update_layout(
        title=dict(
            text=title or "",
            font=dict(size=14, color=TEXT_COLOR, family="Inter, Helvetica Neue, sans-serif"),
        ),
        plot_bgcolor=BG_COLOR,
        paper_bgcolor=BG_COLOR,
        font=dict(color=MUTED_TEXT, size=11, family="Inter, Helvetica Neue, sans-serif"),
        height=height,
        margin=dict(l=40, r=20, t=40 if title else 20, b=40),
        showlegend=show_legend,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=10, color=MUTED_TEXT),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        xaxis=dict(gridcolor=GRID_COLOR, zeroline=False, linecolor="rgba(0,0,0,0.1)"),
        yaxis=dict(gridcolor=GRID_COLOR, zeroline=False, linecolor="rgba(0,0,0,0.1)"),
    )
    return fig


def depeg_vline(fig, date="2025-11-04", label="Depeg"):
    """Add vertical dashed lines marking both depeg events."""
    import pandas as pd
    PURPLE = "#9333ea"

    # xUSD depeg - Nov 4
    dt1 = pd.Timestamp("2025-11-04")
    fig.add_vline(x=dt1, line_dash="dash", line_color=RED, opacity=0.5)
    fig.add_annotation(
        x=dt1, y=1, yref="paper",
        text="xUSD depeg (Nov 4)", showarrow=False,
        font=dict(size=10, color=RED),
        yshift=14,
    )

    # deUSD depeg - Nov 6
    dt2 = pd.Timestamp("2025-11-06")
    fig.add_vline(x=dt2, line_dash="dash", line_color=PURPLE, opacity=0.5)
    fig.add_annotation(
        x=dt2, y=1, yref="paper",
        text="deUSD depeg (Nov 6)", showarrow=False,
        font=dict(size=10, color=PURPLE),
        yshift=-6,
    )
    return fig


def time_series(df, x, y, color=None, title=None, height=400, y_format=None):
    """Standard time series line chart."""
    fig = px.line(df, x=x, y=y, color=color)
    fig = apply_layout(fig, title=title, height=height)
    if y_format:
        fig.update_yaxes(tickformat=y_format)
    return fig


def bar_chart(df, x, y, color=None, title=None, height=400, horizontal=False, text=None):
    """Standard bar chart."""
    if horizontal:
        fig = px.bar(df, y=x, x=y, color=color, orientation="h", text=text)
    else:
        fig = px.bar(df, x=x, y=y, color=color, text=text)
    fig = apply_layout(fig, title=title, height=height)
    fig.update_traces(textposition="outside", textfont_size=10)
    return fig


def donut_chart(values, names, title=None, height=350, colors=None):
    """Donut / pie chart."""
    fig = go.Figure(go.Pie(
        values=values,
        labels=names,
        hole=0.55,
        marker_colors=colors,
        textinfo="label+percent",
        textposition="outside",
    ))
    fig = apply_layout(fig, title=title, height=height, show_legend=False)
    return fig


def heatmap(df, x, y, z, title=None, height=400, color_scale="RdYlGn_r"):
    """Heatmap chart."""
    fig = go.Figure(go.Heatmap(
        x=df[x],
        y=df[y],
        z=df[z],
        colorscale=color_scale,
        text=df[z].round(2),
        texttemplate="%{text}",
        textfont=dict(size=9),
    ))
    fig = apply_layout(fig, title=title, height=height, show_legend=False)
    return fig


def format_usd(value):
    """Format a number as USD string."""
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:,.1f}M"
    elif abs(value) >= 1_000:
        return f"${value / 1_000:,.1f}K"
    else:
        return f"${value:,.2f}"


def md_usd(value):
    """format_usd but with escaped $ for safe use in Streamlit markdown, st.error, st.warning, st.info."""
    return format_usd(value).replace("$", r"\$")


def fmt_usd_cols(df, cols=None):
    """Pre-format USD columns as readable strings ($1,234,567).
    
    If cols is None, auto-detects columns containing USD values.
    Returns a copy; does not mutate the original.
    """
    import pandas as pd
    out = df.copy()
    if cols is None:
        usd_patterns = ("_usd", "_lost", "_depeg", "_now", "_locked",
                        "supply_at", "bad_debt", "capital_lost", "true_loss",
                        "total_borrow", "total_supply", "start_tvl", "end_tvl",
                        "min_flow", "max_flow")
        cols = [c for c in out.columns 
                if any(c.endswith(p) or c.startswith(p) or p in c for p in usd_patterns)
                and out[c].dtype in ("float64", "int64", "float32", "int32")]
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).apply(
                lambda v: f"${v:,.0f}" if abs(v) >= 1 else "$0"
            )
    return out
