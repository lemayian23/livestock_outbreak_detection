#!/usr/bin/env python3
"""
Script to setup email configuration for alerts
"""

import yaml
import os
import getpass

def setup_email_config():
    """Interactive setup for email configuration"""
    
    config_path = 'config/settings.yaml'
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        return
    
    # Load current config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    print("=" * 60)
    print("Email Alert Configuration Setup")
    print("=" * 60)
    
    # Enable email alerts
    enable = input("\nEnable email alerts? (y/n): ").lower().strip()
    if enable == 'y':
        config['email_alerts']['enabled'] = True
        
        # Get email configuration
        print("\n--- Email Server Configuration ---")
        
        smtp_server = input(f"SMTP Server [{config['email_alerts'].get('smtp_server', 'smtp.gmail.com')}]: ").strip()
        if smtp_server:
            config['email_alerts']['smtp_server'] = smtp_server
        
        smtp_port = input(f"SMTP Port [{config['email_alerts'].get('smtp_port', 587)}]: ").strip()
        if smtp_port:
            config['email_alerts']['smtp_port'] = int(smtp_port)
        
        use_tls = input(f"Use TLS? (y/n) [{config['email_alerts'].get('use_tls', True)}]: ").strip().lower()
        if use_tls == 'y':
            config['email_alerts']['use_tls'] = True
        elif use_tls == 'n':
            config['email_alerts']['use_tls'] = False
        
        # Sender email
        sender_email = input(f"\nSender Email [{config['email_alerts'].get('sender_email', '')}]: ").strip()
        if sender_email:
            config['email_alerts']['sender_email'] = sender_email
        
        # Sender password (masked input)
        print("\nSender Password (input will be hidden):")
        sender_password = getpass.getpass("Password: ")
        if sender_password:
            config['email_alerts']['sender_password'] = sender_password
        
        # Recipients
        print("\n--- Recipients ---")
        print("Current recipients:", config['email_alerts'].get('recipients', []))
        
        modify_recipients = input("\nModify recipients? (y/n): ").lower().strip()
        if modify_recipients == 'y':
            recipients = []
            print("Enter recipient emails (one per line, empty line to finish):")
            while True:
                recipient = input("> ").strip()
                if not recipient:
                    break
                if '@' in recipient:  # Simple email validation
                    recipients.append(recipient)
                else:
                    print("Invalid email format. Skipping.")
            
            if recipients:
                config['email_alerts']['recipients'] = recipients
        
        # Alert threshold
        print("\n--- Alert Settings ---")
        threshold = input(f"Send emails for alerts with severity: (low/medium/high/critical) [{config['email_alerts'].get('alert_threshold', 'high')}]: ").strip().lower()
        if threshold in ['low', 'medium', 'high', 'critical']:
            config['email_alerts']['alert_threshold'] = threshold
        
        include_attachments = input(f"Include report attachments? (y/n) [{config['email_alerts'].get('include_attachments', True)}]: ").strip().lower()
        if include_attachments == 'y':
            config['email_alerts']['include_attachments'] = True
        elif include_attachments == 'n':
            config['email_alerts']['include_attachments'] = False
        
        print("\nEmail configuration updated!")
        
    else:
        config['email_alerts']['enabled'] = False
        print("\nEmail alerts disabled.")
    
    # Save updated config
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print(f"\nConfiguration saved to: {config_path}")
    
    # Test email configuration
    if config['email_alerts'].get('enabled', False):
        test_email = input("\nTest email configuration? (y/n): ").lower().strip()
        if test_email == 'y':
            test_email_config(config['email_alerts'])

def test_email_config(email_config):
    """Test email configuration"""
    try:
        from src.notification.email_sender import EmailAlertSender
        
        print("\nTesting email configuration...")
        
        # Initialize email sender
        sender = EmailAlertSender(email_config)
        
        # Create test alert
        test_alert = {
            'severity': 'high',
            'farm_id': 'TEST_FARM',
            'affected_animals': 5,
            'description': 'Test alert to verify email configuration',
            'created_at': '2024-01-15 10:00:00',
            'animal_types': ['cattle', 'sheep'],
            'avg_anomaly_score': 6.5
        }
        
        # Send test email
        success = sender.send_alert(
            test_alert,
            custom_subject="[TEST] Livestock System Email Test",
            custom_body="This is a test email to verify your email configuration is working correctly.\n\nIf you receive this email, your email alerts are properly configured."
        )
        
        if success:
            print("✓ Test email sent successfully!")
            print("Check the inbox of the recipient emails.")
        else:
            print("✗ Failed to send test email.")
            print("Please check your email configuration and credentials.")
    
    except Exception as e:
        print(f"✗ Error testing email configuration: {str(e)}")
        print("Please verify your settings and try again.")

if __name__ == "__main__":
    setup_email_config()