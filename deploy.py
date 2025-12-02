#!/usr/bin/env python3
"""
Deployment script for setting up the system
"""

import os
import sys
import subprocess

def check_dependencies():
    """Check if required dependencies are installed"""
    required = ['python3', 'pip']
    missing = []
    
    for dep in required:
        try:
            subprocess.run([dep, '--version'], 
                          capture_output=True, 
                          check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            missing.append(dep)
    
    return missing

def setup_directories():
    """Create necessary directories"""
    directories = [
        'data/raw',
        'data/processed',
        'data/backups',
        'outputs/reports',
        'outputs/alerts',
        'models/saved_models',
        'config',
        'templates'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")
    
    return True

def install_requirements():
    """Install Python packages"""
    if not os.path.exists('requirements.txt'):
        print("Error: requirements.txt not found!")
        return False
    
    try:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'],
                      check=True)
        print("Requirements installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error installing requirements: {e}")
        return False

def create_config_files():
    """Create default configuration files if they don't exist"""
    config_content = """app:
  name: "Livestock Outbreak Detection MVP"
  version: "1.0.0"
  offline_mode: true

database:
  path: "./data/livestock.db"
  type: "sqlite"

anomaly_detection:
  method: "statistical"
  threshold: 3.0
  window_size: 7
  min_anomalies_for_alert: 3

livestock_metrics:
  normal_ranges:
    temperature:
      cattle: [38.0, 39.0]
      sheep: [38.5, 40.0]
      goat: [38.5, 40.5]
    heart_rate:
      cattle: [48, 84]
      sheep: [70, 80]
    activity_level: [0.5, 1.5]
"""
    
    config_path = 'config/settings.yaml'
    if not os.path.exists(config_path):
        with open(config_path, 'w') as f:
            f.write(config_content)
        print(f"Created configuration file: {config_path}")
    
    return True

def main():
    print("=" * 60)
    print("Livestock Outbreak Detection System - Setup")
    print("=" * 60)
    
    # Step 1: Check dependencies
    print("\n1. Checking dependencies...")
    missing = check_dependencies()
    if missing:
        print(f"   Missing dependencies: {', '.join(missing)}")
        print("   Please install them manually.")
        return False
    
    # Step 2: Setup directories
    print("\n2. Setting up directories...")
    setup_directories()
    
    # Step 3: Create config files
    print("\n3. Creating configuration files...")
    create_config_files()
    
    # Step 4: Install requirements
    print("\n4. Installing Python packages...")
    if not install_requirements():
        return False
    
    print("\n" + "=" * 60)
    print("Setup completed successfully!")
    print("\nNext steps:")
    print("1. Run the simulation: python run_pipeline.py")
    print("2. Start the dashboard: python dashboard.py")
    print("3. View dashboard at: http://localhost:5000")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)