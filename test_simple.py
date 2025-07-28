#!/usr/bin/env python3
print("Starting test script")

import sys
print("sys imported")

import argparse
print("argparse imported")

try:
    import psycopg2
    print("psycopg2 imported successfully")
except ImportError as e:
    print(f"psycopg2 import failed: {e}")

print("All imports completed")

# Test database connection
try:
    print("Attempting database connection...")
    conn = psycopg2.connect("host=gisdb.manatee.org port=5433 dbname=gis user=smay sslmode=require", connect_timeout=5)
    print("Database connection successful!")
    conn.close()
except Exception as e:
    print(f"Database connection failed: {e}")

print("Test script completed")