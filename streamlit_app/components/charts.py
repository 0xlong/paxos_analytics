import plotly.express as px
import plotly.graph_objects as go

# Minimal sleek color palette
COLORS = {
    "primary": "#0F172A",    # Sleek dark slate
    "secondary": "#64748B",  # Cool gray
    "tertiary": "#38BDF8",   # Vibrant sky accent
    "warning": "#F59E0B",    # Amber
    "danger": "#EF4444",     # Red
    "background": "rgba(0,0,0,0)", # Transparent background
    "text": "#334155"        # Slate text
}

def style_fig(fig):
    """Apply minimalistic highly-sleek styling to Plotly figures."""
    fig.update_layout(
        plot_bgcolor=COLORS["background"],
        paper_bgcolor=COLORS["background"],
        font=dict(color=COLORS["text"], family="Roboto, sans-serif"),
        margin=dict(t=40, r=20, b=40, l=20),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            title_text=""
        ),
        hoverlabel=dict(
            bgcolor="white", 
            font_size=13, 
            font_family="Roboto",
            bordercolor="#E2E8F0"
        )
    )
    
    # Remove all visible grid lines and ticks for a super minimalistic look
    # Note: For pie charts, these axis updates are safely ignored by Plotly
    fig.update_xaxes(
        showgrid=False, 
        zeroline=False, 
        showline=False, 
        showticklabels=True,
        title_font=dict(size=12, color=COLORS["secondary"]),
        tickfont=dict(size=11, color=COLORS["secondary"])
    )
    fig.update_yaxes(
        showgrid=True, 
        gridwidth=1, 
        gridcolor="#F1F5F9", # Extremely faint gridline
        zeroline=False, 
        showline=False, 
        showticklabels=True,
        title_font=dict(size=12, color=COLORS["secondary"]),
        tickfont=dict(size=11, color=COLORS["secondary"])
    )
    return fig

def create_area_chart(df, x, y, title, color_discrete_sequence=None, color_col=None, color_map=None):
    if color_discrete_sequence is None and color_map is None:
        color_discrete_sequence = [COLORS["tertiary"], COLORS["secondary"], COLORS["primary"]]
        
    fig = px.area(df, x=x, y=y, color=color_col, title=title, 
                  color_discrete_sequence=color_discrete_sequence, color_discrete_map=color_map)
    
    # Removing hardcoded fillcolor so dynamic sequences work. 
    # Plotly will auto-color the fill regions.
    fig.update_traces(line=dict(width=2)) 
    
    return style_fig(fig)

def create_bar_chart(df, x, y, title, color=None, color_discrete_sequence=None, color_discrete_map=None, hover_data=None, barmode="relative"):
    if color_discrete_sequence is None and color_discrete_map is None:
        color_discrete_sequence = [COLORS["primary"], COLORS["tertiary"], COLORS["secondary"]]
        
    fig = px.bar(df, x=x, y=y, color=color, title=title, 
                 color_discrete_sequence=color_discrete_sequence, color_discrete_map=color_discrete_map, 
                 hover_data=hover_data, barmode=barmode)
    fig.update_traces(marker_line_width=0)
    return style_fig(fig)

def create_line_chart(df, x, y, title, color=None, color_discrete_sequence=None):
    if color_discrete_sequence is None:
        color_discrete_sequence = [COLORS["primary"], COLORS["tertiary"], COLORS["secondary"]]
        
    fig = px.line(df, x=x, y=y, color=color, title=title, color_discrete_sequence=color_discrete_sequence)
    # Thicker, smoother spline lines
    fig.update_traces(line=dict(width=3, shape='spline', smoothing=0.8))
    return style_fig(fig)

def create_pie_chart(df, names, values, title, color_discrete_sequence=None, hole=0.6):
    """Factory for donut charts maintaining minimal style constraints."""
    if color_discrete_sequence is None:
        color_discrete_sequence = px.colors.sequential.Teal
        
    fig = px.pie(df, names=names, values=values, title=title, 
                 color_discrete_sequence=color_discrete_sequence, hole=hole)
                 
    # Add a thin white border to each slice for separation, hide text if too small
    fig.update_traces(textposition='inside', textinfo='percent', 
                      marker=dict(line=dict(color='#FFFFFF', width=2)))
    
    styled = style_fig(fig)
    # Override legend for pie to be centered below since horizontal real estate is better
    styled.update_layout(
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.1,
            xanchor="center",
            x=0.5
        )
    )
    return styled
