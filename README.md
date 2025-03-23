# European Flight Monitoring System

A comprehensive system for monitoring flights across Europe, providing real-time updates and delay tracking.

## Overview

This project integrates MongoDB for data storage, FastAPI for backend API management, and Streamlit for a user-friendly frontend interface. It fetches real-time flight data using the AviationStack API, allowing users to track flights, identify delays, and analyze flight information.

## Features

- **Real-time Flight Updates**: Fetches and updates flight data in real-time using the AviationStack API.
- **Delay Tracking**: Identifies flights delayed by more than 2 hours.
- **Interactive Dashboard**: Streamlit frontend provides an interactive map view of airport locations and real-time metrics on flights and delays.
- **Data Visualization**: Displays flight information in a tabular format with filtering capabilities.
- **API Endpoints**: FastAPI backend offers endpoints for updating data, retrieving airport and flight information, and querying delayed flights.