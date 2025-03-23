import streamlit as st
import requests
import pandas as pd
import pydeck as pdk
from datetime import datetime

# Configuration
BACKEND_URL = "http://localhost:8000"
MAP_KEY = st.secrets["mapbox"]["token"]

st.set_page_config(
    page_title="European Flight Monitor",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Style
st.markdown("""
<style>
    .main {background-color: #f0f2f6;}
    .stAlert {padding: 20px;}
    .metric-box {padding: 20px; background: white; border-radius: 10px; margin: 10px;}
</style>
""", unsafe_allow_html=True)

def fetch_data(endpoint):
    try:
        response = requests.get(f"{BACKEND_URL}/{endpoint}")
        return response.json() if response.status_code == 200 else []
    except:
        return []

def main():
    st.title("✈️ European Flight Monitoring System")
    
    # Control Panel
    with st.sidebar:
        st.header("Controls")
        if st.button("🔄 Refresh All Data"):
            with st.spinner("Updating data..."):
                requests.get(f"{BACKEND_URL}/update-data")
            st.success("Data updated!")
        
        st.markdown("---")
        st.markdown("**Filter Options**")
        airport_filter = st.selectbox("Select Airport", options=["All"] + [
            f"{a['iata_code']} - {a['name']}" 
            for a in fetch_data("airports")
        ])
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Airports", len(fetch_data("airports")))
    with col2:
        st.metric("Active Flights", len(fetch_data("flights")))
    with col3:
        delays = fetch_data("delays")
        st.metric("Current Delays", len(delays))
    with col4:
        avg_delay = pd.DataFrame(delays).get("delay", pd.Series([0])).mean()
        st.metric("Average Delay (min)", f"{avg_delay:.1f}" if avg_delay else "N/A")

    # Map View
    st.subheader("Airport Locations")
    airports_df = pd.DataFrame(fetch_data("airports"))
    if not airports_df.empty:
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=airports_df,
            get_position=["longitude", "latitude"],
            get_radius=10000,
            get_fill_color=[255, 0, 0, 160],
            pickable=True
        )
        
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(
                latitude=51.1657,
                longitude=10.4515,
                zoom=4.5,
                pitch=50,
            ),
            layers=[layer],
            tooltip={"text": "{name}\nIATA: {iata_code}\nCity: {city}"}
        ))
    
    # Flight Data
# Update the flight data processing section in the frontend/app.py

# Flight Data

    st.subheader("Flight Information")
    flights = fetch_data("flights")
    if flights:
        df = pd.DataFrame(flights)
        
        # Handle potential missing data
        df["scheduled_departure"] = df["scheduled_departure"].fillna("")
        df["actual_departure"] = df["actual_departure"].fillna("")
        
        # Convert datetime columns
        try:
            df["scheduled_departure"] = pd.to_datetime(
                df["scheduled_departure"].str.replace(r'(\+00:00|Z)$', '', regex=True),
                format='%Y-%m-%dT%H:%M:%S',
                errors='coerce'
            )
            df["actual_departure"] = pd.to_datetime(
                df["actual_departure"].str.replace(r'(\+00:00|Z)$', '', regex=True),
                format='%Y-%m-%dT%H:%M:%S',
                errors='coerce'
            )
        except Exception as e:
            st.error(f"Date conversion error: {str(e)}")
        
        # Calculate delays safely
        df["delay"] = (df["actual_departure"] - df["scheduled_departure"]).dt.total_seconds() / 60
        df["delay"] = df["delay"].fillna(0).astype(int)
        
        # Filtering
        if airport_filter != "All":
            selected_airport = airport_filter.split(" - ")[0]
            df = df[df["departure_airport"] == selected_airport]
        
        # Display data with proper column names
        st.dataframe(
            df[[
                "flight_number", 
                "airline", 
                "departure_airport", 
                "arrival_airport", 
                "status", 
                "delay"
            ]].rename(columns={
                "departure_airport": "Departure",
                "arrival_airport": "Arrival"
            }),
            height=400,
            use_container_width=True
        )
    else:
        st.warning("No flight data available")


if __name__ == "__main__":
    main()
