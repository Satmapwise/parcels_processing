#!/usr/bin/env python3
"""
Explore Tables Script

This script runs SELECT queries to understand the structure of the three support tables:
- support.zoning_transform
- support.flu_transform  
- support.parcel_shp_fields
"""

import os
import psycopg2
import psycopg2.extras
from pathlib import Path


def load_environment():
    """Load environment variables from .env file if available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        # dotenv not available, try manual loading
        env_path = Path('.env')
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('\'"')

# Load environment on import
load_environment()

# Database connection
PG_CONNECTION = os.getenv("PG_CONNECTION")


def explore_table(table_name: str, conn):
    """Explore a table structure and sample data."""
    print(f"\n{'='*60}")
    print(f"EXPLORING TABLE: {table_name}")
    print(f"{'='*60}")
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Get table structure
            print(f"\n1. TABLE STRUCTURE:")
            cur.execute(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = 'support' AND table_name = '{table_name.split('.')[-1]}'
                ORDER BY ordinal_position
            """)
            
            columns = cur.fetchall()
            for col in columns:
                print(f"  {col['column_name']:<20} {col['data_type']:<15} {'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'}")
            
            # Get row count
            print(f"\n2. ROW COUNT:")
            cur.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            count = cur.fetchone()
            print(f"  Total rows: {count['count']}")
            
            # Get sample data (first 5 rows)
            print(f"\n3. SAMPLE DATA (first 5 rows):")
            cur.execute(f"SELECT * FROM {table_name} LIMIT 5")
            sample_rows = cur.fetchall()
            
            if sample_rows:
                # Print column headers
                headers = list(sample_rows[0].keys())
                header_str = " | ".join(f"{h:<15}" for h in headers)
                print(f"  {header_str}")
                print(f"  {'-' * len(header_str)}")
                
                # Print sample data
                for row in sample_rows:
                    row_str = " | ".join(f"{str(v):<15}" for v in row.values())
                    print(f"  {row_str}")
            else:
                print("  No data found")
                
    except Exception as e:
        print(f"  ERROR exploring {table_name}: {e}")


def main():
    """Main execution function."""
    if not PG_CONNECTION:
        print("[ERROR] PG_CONNECTION not found in environment. Please set it in .env file.")
        return 1
    
    try:
        print("[INFO] Connecting to database...")
        conn = psycopg2.connect(PG_CONNECTION, connect_timeout=10)
        print("[INFO] Database connection successful")
        
        # Explore each table
        tables = [
            "support.zoning_transform",
            "support.flu_transform", 
            "support.parcel_shp_fields"
        ]
        
        for table in tables:
            explore_table(table, conn)
            
        conn.close()
        print(f"\n[INFO] Table exploration completed!")
        
    except Exception as e:
        print(f"[ERROR] Script execution failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 