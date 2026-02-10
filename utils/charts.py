"""
Reusable Plotly chart configurations — Morpho-inspired light theme.
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
    "SLOW_REACTOR": YELLOW,
    "VERY_LATE": RED,
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
    """Add a vertical dashed line marking the depeg event."""
    import pandas as pd
    dt = pd.Timestamp(date)
    fig.add_vline(
        x=dt,
        line_dash="dash",
        line_color=RED,
        opacity=0.5,
    )
    fig.add_annotation(
        x=dt, y=1, yref="paper",
        text=label, showarrow=False,
        font=dict(size=10, color=RED),
        yshift=10,
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
