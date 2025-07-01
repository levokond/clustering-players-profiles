#!/usr/bin/env python3
"""
Setup script for FBref → Supabase Pipeline
Helps with initial configuration and testing
"""

import os
import sys
import subprocess
from pathlib import Path

def check_requirements():
    """Check if all required packages are installed."""
    print("Checking requirements...")
    try:
        import pandas
        import requests
        import cloudscraper
        import sqlalchemy
        import psycopg2
        from bs4 import BeautifulSoup
        print("✓ All required packages are installed")
        return True
    except ImportError as e:
        print(f"✗ Missing package: {e}")
        print("Run: pip install -r requirements.txt")
        return False

def setup_environment():
    """Setup environment configuration."""
    print("\nSetting up environment...")
    
    env_file = Path('.env')
    env_example = Path('.env.example')
    
    if env_file.exists():
        print("✓ .env file already exists")
        return True
    
    if env_example.exists():
        print("Copying .env.example to .env...")
        env_file.write_text(env_example.read_text())
        print("✓ Created .env file")
        print("⚠️  Please edit .env with your Supabase credentials")
        return True
    else:
        print("✗ .env.example not found")
        return False

def test_database_connection():
    """Test database connectivity."""
    print("\nTesting database connection...")
    
    try:
        from db import SupabaseDB
        db = SupabaseDB()
        stats = db.get_table_stats()
        db.close()
        print("✓ Database connection successful!")
        print(f"✓ Tables found: {list(stats.keys())}")
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        print("Please check your SUPABASE_DB_URL in .env file")
        return False

def test_scraper():
    """Test scraper functionality."""
    print("\nTesting scraper (this may take a moment)...")
    
    try:
        from scraper import get_all_recent_fixtures
        fixtures = get_all_recent_fixtures(days_back=1)  # Test with 1 day to be quick
        print(f"✓ Scraper working! Found {len(fixtures)} recent fixtures")
        return True
    except Exception as e:
        print(f"✗ Scraper test failed: {e}")
        return False

def make_scripts_executable():
    """Make the main scripts executable."""
    print("\nMaking scripts executable...")
    
    scripts = ['fbref_update.py', 'db_maintenance.py']
    
    for script in scripts:
        script_path = Path(script)
        if script_path.exists():
            os.chmod(script_path, 0o755)
            print(f"✓ Made {script} executable")
        else:
            print(f"✗ {script} not found")

def show_cron_setup():
    """Show cron job setup instructions."""
    print("\n" + "="*50)
    print("PRODUCTION SETUP")
    print("="*50)
    
    current_dir = Path.cwd()
    
    print("\n1. Add these cron jobs (run 'crontab -e'):")
    print(f"""
# FBref Pipeline - Weekly Update (Monday 03:15)
15 3 * * MON /usr/bin/python3 {current_dir}/fbref_update.py >> /var/log/fbref_update.log 2>&1

# FBref Pipeline - Daily Maintenance (08:00)
0  8 * * *   /usr/bin/python3 {current_dir}/db_maintenance.py >> /var/log/db_maint.log 2>&1
""")
    
    print("2. Create log directories:")
    print("sudo mkdir -p /var/log")
    print("sudo touch /var/log/fbref_update.log /var/log/db_maint.log")
    print("sudo chown $USER:$USER /var/log/fbref_update.log /var/log/db_maint.log")
    
    print("\n3. Test manually:")
    print(f"python3 {current_dir}/fbref_update.py")
    print(f"python3 {current_dir}/db_maintenance.py")

def main():
    """Main setup routine."""
    print("FBref → Supabase Pipeline Setup")
    print("="*40)
    
    success = True
    
    # Check requirements
    if not check_requirements():
        success = False
    
    # Setup environment
    if not setup_environment():
        success = False
    
    # Make scripts executable
    make_scripts_executable()
    
    # Test database (only if env setup was successful)
    if success:
        if not test_database_connection():
            success = False
            print("\n⚠️  Database test failed - please configure .env first")
        
        # Test scraper (only if database works)
        if success:
            test_scraper()
    
    print("\n" + "="*40)
    if success:
        print("✓ Setup completed successfully!")
        show_cron_setup()
    else:
        print("✗ Setup incomplete - please fix the issues above")
        sys.exit(1)

if __name__ == "__main__":
    main() 