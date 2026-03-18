"""
Stromify KPI Monitor - Chart-Funktionen
Wiederverwendbare Plotly-Charts für das Dashboard.
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


# Farbpalette
COLORS = {
    "primary": "#FF6B35",
    "secondary": "#4ECDC4",
    "accent": "#FFE66D",
    "success": "#2ECC71",
    "warning": "#F39C12",
    "danger": "#E74C3C",
    "bg": "#0E1117",
    "card_bg": "#1A1F2E",
    "text": "#FAFAFA",
    "text_muted": "#8899AA",
    "grid": "#2A3040",
}

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=COLORS["text"], size=12),
    margin=dict(l=20, r=20, t=40, b=20),
    xaxis=dict(gridcolor=COLORS["grid"], showgrid=True),
    yaxis=dict(gridcolor=COLORS["grid"], showgrid=True),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    hoverlabel=dict(bgcolor=COLORS["card_bg"], font_size=13),
)


def line_chart(
    df: pd.DataFrame,
    x: str,
    y: list[str],
    labels: dict | None = None,
    title: str = "",
) -> go.Figure:
    """Erstellt ein Liniendiagramm mit einer oder mehreren Linien."""
    colors = [COLORS["primary"], COLORS["secondary"], COLORS["accent"], COLORS["success"]]

    fig = go.Figure()
    for i, col in enumerate(y):
        display_name = labels.get(col, col) if labels else col
        fig.add_trace(go.Scatter(
            x=df[x],
            y=df[col],
            mode="lines",
            name=display_name,
            line=dict(color=colors[i % len(colors)], width=2.5),
            hovertemplate=f"<b>{display_name}</b><br>%{{x}}<br>%{{y:,.0f}}<extra></extra>",
        ))

    fig.update_layout(title=title, **CHART_LAYOUT)
    fig.update_layout(height=350)
    return fig


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: list[str],
    labels: dict | None = None,
    title: str = "",
    barmode: str = "group",
) -> go.Figure:
    """Erstellt ein Balkendiagramm."""
    colors = [COLORS["primary"], COLORS["secondary"], COLORS["success"]]

    fig = go.Figure()
    for i, col in enumerate(y):
        display_name = labels.get(col, col) if labels else col
        fig.add_trace(go.Bar(
            x=df[x],
            y=df[col],
            name=display_name,
            marker_color=colors[i % len(colors)],
            hovertemplate=f"<b>{display_name}</b><br>%{{x}}<br>%{{y:,.0f}}<extra></extra>",
        ))

    fig.update_layout(title=title, barmode=barmode, **CHART_LAYOUT)
    fig.update_layout(height=350)
    return fig


def gauge_chart(
    value: float,
    target: float,
    title: str = "",
    suffix: str = "",
) -> go.Figure:
    """Erstellt ein Gauge-Diagramm für Soll/Ist-Vergleich."""
    percentage = (value / target * 100) if target > 0 else 0

    if percentage >= 100:
        bar_color = COLORS["success"]
    elif percentage >= 70:
        bar_color = COLORS["warning"]
    else:
        bar_color = COLORS["danger"]

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        number={"suffix": f" {suffix}" if suffix else "", "font": {"size": 28}},
        delta={"reference": target, "relative": False, "position": "bottom"},
        title={"text": title, "font": {"size": 14}},
        gauge={
            "axis": {"range": [0, target * 1.2], "tickcolor": COLORS["text_muted"]},
            "bar": {"color": bar_color, "thickness": 0.7},
            "bgcolor": COLORS["card_bg"],
            "borderwidth": 0,
            "steps": [
                {"range": [0, target * 0.7], "color": "rgba(231,76,60,0.15)"},
                {"range": [target * 0.7, target], "color": "rgba(243,156,18,0.15)"},
                {"range": [target, target * 1.2], "color": "rgba(46,204,113,0.15)"},
            ],
            "threshold": {
                "line": {"color": COLORS["text"], "width": 2},
                "thickness": 0.8,
                "value": target,
            },
        },
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=COLORS["text"]),
        margin=dict(l=30, r=30, t=80, b=20),
        height=280,
    )
    return fig


def progress_bar_data(value: float, target: float) -> dict:
    """Berechnet Daten für einen Fortschrittsbalken."""
    percentage = min((value / target * 100) if target > 0 else 0, 120)
    if percentage >= 100:
        color = COLORS["success"]
    elif percentage >= 70:
        color = COLORS["warning"]
    else:
        color = COLORS["danger"]

    return {
        "value": value,
        "target": target,
        "percentage": percentage,
        "color": color,
    }


def funnel_chart(
    stages: list[str],
    values: list[int],
    title: str = "",
) -> go.Figure:
    """Erstellt ein Funnel-Diagramm für die Lead-Pipeline."""
    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(
            color=[COLORS["primary"], COLORS["secondary"], COLORS["accent"], COLORS["success"]],
        ),
        connector={"line": {"color": COLORS["grid"], "width": 1}},
    ))

    fig.update_layout(title=title, **CHART_LAYOUT)
    fig.update_layout(height=350, showlegend=False)
    return fig


def area_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str = "",
    color: str | None = None,
) -> go.Figure:
    """Erstellt ein Area-Chart."""
    fill_color = color or COLORS["primary"]

    # Transparente Füllfarbe berechnen (Plotly akzeptiert nur rgba, nicht 8-digit hex)
    if fill_color.startswith("rgb"):
        fill_alpha = fill_color.replace(")", ",0.2)").replace("rgb", "rgba")
    elif fill_color.startswith("#") and len(fill_color) == 7:
        r = int(fill_color[1:3], 16)
        g = int(fill_color[3:5], 16)
        b = int(fill_color[5:7], 16)
        fill_alpha = f"rgba({r},{g},{b},0.2)"
    else:
        fill_alpha = "rgba(255,107,53,0.2)"

    fig = go.Figure(go.Scatter(
        x=df[x],
        y=df[y],
        fill="tozeroy",
        mode="lines",
        line=dict(color=fill_color, width=2),
        fillcolor=fill_alpha,
        hovertemplate="<b>%{x}</b><br>%{y:,.0f}<extra></extra>",
    ))

    fig.update_layout(title=title, **CHART_LAYOUT)
    fig.update_layout(height=350)
    return fig
