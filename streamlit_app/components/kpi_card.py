import streamlit as st

def kpi_card_row(cards: list):
    """
    Renders a row of KPI cards using CSS Grid to ensure perfectly even heights
    across the entire block, regardless of individual card content length.
    Expected card dict keys: 'title', 'value', 'subvalue' (optional), 'delta' (optional).
    """
    # Use auto-fit grid to ensure they align properly and are perfectly identical in height.
    grid_html = '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 24px; margin-bottom: 24px;">'
    
    for c in cards:
        title = c.get('title', '')
        value = c.get('value', '')
        subvalue = c.get('subvalue', None)
        delta = c.get('delta', None)
        
        delta_html = f'<span style="color: #10B981; font-family: Roboto, sans-serif; font-weight: 600; font-size: 13px; background: #ECFDF5; padding: 4px 10px; border-radius: 20px; white-space: nowrap;">{delta}</span>' if delta else ''
        subvalue_html = f'<p style="margin: 12px 0 0 0; color: #64748B; font-family: Roboto, sans-serif; font-size: 14px; font-weight: 400; line-height: 1.4;">{subvalue}</p>' if subvalue else '<div style="margin-top: 12px;"></div>'
        
        card_html = (
            '<div style="background-color: #FFFFFF; padding: 24px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0, 0, 0, 0.03); border: 1px solid #F1F5F9; display: flex; flex-direction: column; justify-content: space-between; height: 100%;">'
            '<div>'
            f'<p style="margin: 0; color: #94A3B8; font-family: Roboto, sans-serif; font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 1.2px;">{title}</p>'
            '<div style="display: flex; align-items: baseline; flex-wrap: wrap; gap: 12px; margin-top: 8px;">'
            f'<h2 style="margin: 0; color: #0F172A; font-family: Roboto, sans-serif; font-size: 30px; font-weight: 700; letter-spacing: -1px; word-break: break-word;">{value}</h2>'
            f'{delta_html}'
            '</div>'
            '</div>'
            f'<div style="margin-top: 16px;">{subvalue_html}</div>'
            '</div>'
        )
        grid_html += card_html
        
    grid_html += '</div>'
    
    with st.container():
        st.markdown(grid_html, unsafe_allow_html=True)
