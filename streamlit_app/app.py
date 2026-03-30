import streamlit as st

st.set_page_config(
    page_title="PYUSD Analytics | Paxos",
    page_icon="💸",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern minimalistic styling, importing Roboto
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');

    /* Global Typography */
    html, body, [class*="css"]  {
        font-family: 'Roboto', sans-serif !important;
        background-color: #F8FAFC;
    }

    /* Hide Streamlit components for a cleaner app look */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Clean up the main view background */
    .stApp {
        background-color: #F8FAFC;
    }
    
    /* Sleek Title */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Roboto', sans-serif !important;
        font-weight: 700 !important;
        color: #0F172A !important;
        letter-spacing: -0.5px;
    }
    
    /* General Text */
    p {
        color: #475569;
        font-weight: 400;
    }

    /* Force Streamlit Columns to Equal Height */
    div[data-testid="column"] {
        display: flex;
        flex-direction: column;
    }
    div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"],
    div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"] > div {
        flex: 1;
        display: flex;
        flex-direction: column;
    }
    div[data-testid="column"] > div.element-container {
        flex: 1;
        display: flex;
        flex-direction: column;
    }
    div[data-testid="column"] > div.element-container > div.stMarkdown,
    div[data-testid="column"] > div.element-container > div.stMarkdown > div {
        flex: 1;
        display: flex;
        flex-direction: column;
    }
</style>
""", unsafe_allow_html=True)

# Define the navigation structure, default tab is now the first page 
# and the old "App" landing page content is completely removed.
pg = st.navigation([
    st.Page("pages/1_supply_adoption.py", title="Supply & Adoption", icon="📈", default=True),
    st.Page("pages/2_who_holds_pyusd.py", title="Ecosystem & Wallets", icon="🏦"),
    st.Page("pages/3_health_compliance.py", title="Health & Compliance", icon="🛡️"),
    st.Page("pages/4_ask_ai.py", title="Ask AI", icon="🤖")
])

pg.run()
