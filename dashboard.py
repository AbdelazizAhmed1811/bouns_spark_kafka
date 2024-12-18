import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'suspiciousTransactions'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='streamlit_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Streamlit App
st.set_page_config(layout="wide", page_title="Transaction Dashboard", page_icon="📊")
st.title('Transaction Dashboard')

# Modern color theme
PRIMARY_COLOR = "#007BFF"  # Blue
SECONDARY_COLOR = "#6C757D"  # Gray
BACKGROUND_COLOR = "#F8F9FA" # Light Gray

# Custom theme
st.markdown(
    f"""
    <style>
        [data-testid="stAppViewContainer"] {{
            background-color: {BACKGROUND_COLOR};
             color: #000000;
        }}
        h1, h2, h3, h4, h5, h6, .css-1n76uvf {{
            color: {PRIMARY_COLOR};
        }}
        div.css-1r6slb0 {{
            background-color: {BACKGROUND_COLOR};
             color: {PRIMARY_COLOR};
        }}
        .plot-container {{
            background-color: #ffffff;
        }}
        div.css-1d391kg {{
            color: {SECONDARY_COLOR};
            }}
         .metric-card {{
            background-color: #ffffff;
            padding: 15px;
            border-radius: 10px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            margin: 10px; /* Add margin here */
            text-align: center;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .metric-container {{
            display: flex;
            align-items: center;
        }}
        .metric-icon {{
            margin-right: 5px;
            font-size: 1.2em;
        }}
        .metric-label {{
            font-size: 1.2em;
            font-weight: 600;
            margin-left: 5px
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

# Initialize data storage
all_transactions = []
placeholder = st.empty()
key_counter = 0

# Functions to calculate metrics and plots
def calculate_metrics(df):
    total_transactions = len(df)
    suspicious_transactions = df['Is_laundering'].sum()
    average_transaction_amount = df['Amount'].mean() if not df.empty else 0
    suspicious_ratio = (suspicious_transactions / total_transactions) * 100 if total_transactions > 0 else 0
    return total_transactions, suspicious_transactions, average_transaction_amount, suspicious_ratio


def create_top_banks_bar(df, col, title):
    if df.empty:
        return None
    suspicious_df = df[df['Is_laundering'] == 1]
    bank_counts = suspicious_df[col].value_counts().nlargest(5)
    bar_df = pd.DataFrame({'Bank': bank_counts.index, 'Count': bank_counts.values})

    colors = px.colors.qualitative.Set2  # Define a list of colors to use
    fig = go.Figure(data=[go.Bar(x=bar_df['Bank'], y=bar_df['Count'],
                                 marker_color=colors[:len(bar_df)])])  # Use slice to match bar number with colors
    fig.update_layout(title=f'{title} 🏦', xaxis_title='Bank', yaxis_title='Count', template="plotly_white")
    return fig
    
def create_payment_type_pie(df):
    if df.empty:
        return None
    payment_counts = df['Payment_type'].value_counts()
    fig = px.pie(names=payment_counts.index, values=payment_counts.values, title='Payment Types 💳', color_discrete_sequence=px.colors.qualitative.Set2,
                  template="plotly_white")
    return fig

def create_currency_pie(df):
    if df.empty:
        return None
    currency_counts = df['Payment_currency'].value_counts()
    fig = px.pie(names=currency_counts.index, values=currency_counts.values, title='Currencies Distribution 🌍', color_discrete_sequence=px.colors.qualitative.Set2,
                template="plotly_white")
    return fig

def create_transaction_hour_bar(df):
    if df.empty:
        return None
    hour_counts = df['Hour'].value_counts().sort_index()
    
    colors = px.colors.qualitative.Set2 # Define a list of colors to use
    fig = go.Figure(data=[go.Bar(x=hour_counts.index, y=hour_counts.values,
                                 marker_color=colors[:len(hour_counts)])])
    fig.update_layout(title="Transaction Volume by Hour 🕒",
                        xaxis_title='Hour of the Day',
                        yaxis_title='Transaction Count', template="plotly_white")
    return fig
    
def create_same_location_pie(df):
    if df.empty:
        return None
    location_counts = df['Same_Location'].value_counts()
    location_labels = ["Different Location", "Same Location"]

    # Handle cases where value counts might return 0 or 1 value
    if len(location_counts) == 0:
      location_values = [0, 0]  # Set default to both 0
    elif len(location_counts) == 1:
      if 0 in location_counts.index:  # Different location only
            location_values = [location_counts[0], 0]
      else:
           location_values = [0, location_counts[1]] # Same location only
    else:
      location_values = list(location_counts.values)

    fig = px.pie(names=location_labels, values=location_values, title='Transaction Location 📍',color_discrete_sequence=px.colors.qualitative.Set2, template="plotly_white")
    return fig


def create_currency_match_pie(df):
    if df.empty:
        return None
    currency_counts = df['currency_match'].value_counts()
    labels = ["Not Matching", "Matching"]

    if len(currency_counts) == 0:
        currency_values = [0, 0]
    elif len(currency_counts) == 1:
        if 0 in currency_counts.index:
           currency_values = [currency_counts[0], 0]
        else:
           currency_values = [0, currency_counts[1]]
    else:
        currency_values = list(currency_counts.values)


    fig = px.pie(names=labels, values=currency_values, title='Currency Matching 💱',color_discrete_sequence=px.colors.qualitative.Set2,template="plotly_white")
    return fig


# Main loop to consume messages and update the dashboard
try:
    metrics_placeholder = st.empty()
    charts_placeholder = st.empty()
    while True:
         
        for message in consumer:
            transaction = message.value
            all_transactions.append(transaction)
            df = pd.DataFrame(all_transactions)

            with metrics_placeholder.container():
                total_transactions, suspicious_transactions, average_transaction_amount, suspicious_ratio = calculate_metrics(df)

                # Metrics Row
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                   st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-container">
                            <span class="metric-icon">💰</span>
                            <span class="metric-label">Total Transactions</span>
                         </div>
                        <h3 style="color: #000000">{total_transactions}</h3>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
                    <div class="metric-card">
                         <div class="metric-container">
                            <span class="metric-icon">🚩</span>
                             <span class="metric-label">Suspicious Transactions</span>
                         </div>
                          <h3 style="color: #000000">{suspicious_transactions}</h3>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="metric-card">
                         <div class="metric-container">
                            <span class="metric-icon">⚖️</span>
                             <span class="metric-label">Avg Transaction</span>
                         </div>
                        <h3 style="color: #000000">{average_transaction_amount:.2f}</h3>
                    </div>
                    """, unsafe_allow_html=True)
                with col4:
                     st.markdown(f"""
                    <div class="metric-card">
                         <div class="metric-container">
                            <span class="metric-icon">📈</span>
                            <span class="metric-label">Suspicious Ratio</span>
                        </div>
                        <h3 style="color: #000000">{suspicious_ratio:.2f}%</h3>
                    </div>
                    """, unsafe_allow_html=True)
            
            with charts_placeholder.container():
                # Create the columns for the charts
                # Suspicious Transactions Graphs First
                col5, col6, col7 = st.columns(3)

                with col5:
                    top_sender_banks_bar = create_top_banks_bar(df, 'Sender_bank_location', 'Top 5 Banks (Send Suspicious Trans)')
                    if top_sender_banks_bar:
                        st.plotly_chart(top_sender_banks_bar, use_container_width=True, key=f"sender_bank_bar_{key_counter}")

                with col6:
                    top_receiver_banks_bar = create_top_banks_bar(df, 'Receiver_bank_location', 'Top 5 Banks (Receive Suspicious Trans)')
                    if top_receiver_banks_bar:
                        st.plotly_chart(top_receiver_banks_bar, use_container_width=True, key=f"receiver_bank_bar_{key_counter}")
                
                with col7:
                    same_location_pie = create_same_location_pie(df)
                    if same_location_pie:
                        st.plotly_chart(same_location_pie, use_container_width=True, key=f"same_location_pie_{key_counter}")
                
                # Other Graphs
                col8, col9, col10 = st.columns(3)

                with col8:
                    payment_type_pie = create_payment_type_pie(df)
                    if payment_type_pie:
                        st.plotly_chart(payment_type_pie, use_container_width=True, key=f"payment_pie_{key_counter}")

                with col9:
                    currency_pie = create_currency_pie(df)
                    if currency_pie:
                         st.plotly_chart(currency_pie, use_container_width=True, key=f"currency_pie_{key_counter}")
                
                with col10:
                    transaction_hour_bar = create_transaction_hour_bar(df)
                    if transaction_hour_bar:
                        st.plotly_chart(transaction_hour_bar, use_container_width=True, key=f"transaction_hour_bar_{key_counter}")
                
                col11, _, _ = st.columns(3)

                with col11:
                    currency_match_pie = create_currency_match_pie(df)
                    if currency_match_pie:
                        st.plotly_chart(currency_match_pie, use_container_width=True, key=f"currency_match_pie_{key_counter}")

                key_counter += 1

            break
        
            
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    consumer.close()