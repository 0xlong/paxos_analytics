import streamlit as st
import time

#st.title("Ask AI  about PyUSD data")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! I am your PYUSD Analytical Assistant. You can ask me questions like 'What is the current total supply?' or 'Show me the daily transfer volume trend.'"}
    ]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# React to user input
if prompt := st.chat_input("Ask a question about PYUSD data..."):
    # Display user message in chat message container
    st.chat_message("user").markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        
        # Simulate processing time
        with st.spinner("Analyzing data..."):
            time.sleep(1)
            
        # Mockup responses based on simple heuristics
        lower_prompt = prompt.lower()
        if "supply" in lower_prompt:
            mock_response = "Based on the latest data from the `pyusd_supply_daily` table, the total PYUSD supply is approximately **372.5M**. The supply has grown steadily since the start of the year."
        elif "volume" in lower_prompt:
            mock_response = "The daily transfer volume has been averaging around **$15M - $25M** over the last 30 days. The peak volume recently was observed last Tuesday, touching over **$40M** in a single day."
        elif "velocity" in lower_prompt:
            mock_response = "The current 30-day token velocity is **1.2x**, indicating that the average PYUSD token changes hands roughly 1.2 times a month. This is relatively healthy comparing to stablecoin benchmarks."
        elif "hold" in lower_prompt or "wallet" in lower_prompt:
            mock_response = "The top 3 wallets hold roughly **45%** of the total supply. The remaining supply is distributed across several thousand smaller retail and institutional wallets."
        else:
            mock_response = f"I am a mockup AI assistant. In a real integration, I would execute SQL queries via an LLM agent to answer your question: '{prompt}'. You would see the derived insights right here!"

        # Simulate a stream of response 
        for chunk in mock_response.split():
            full_response += chunk + " "
            time.sleep(0.04)
            # Add a blinking cursor to simulate typing
            message_placeholder.markdown(full_response + "▌")
        message_placeholder.markdown(full_response)
        
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": full_response})
