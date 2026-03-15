"""
smart_meter_simulator.py
========================
Hybrid Secure Data Aggregation for Smart Grids
-----------------------------------------------
Simulates multiple smart meters using the UCI Individual Household Electric
Power Consumption dataset.  Designed to integrate seamlessly with a downstream
differential-privacy module and a homomorphic-encryption module before sending
readings to an aggregation server.

Pipeline:
    meter_data  →  privacy_module  →  encryption_module  →  aggregation_server

Dataset source:
    https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption
"""
