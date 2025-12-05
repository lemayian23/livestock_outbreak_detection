"""
Notification manager for handling different alert channels
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
from .email_sender import EmailAlertSender

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NotificationManager:
    """Manages all notification channels"""
    
    def __init__(self, config: Dict):
        """
        Initialize notification manager
        
        Args:
            config: Notification configuration
        """
        self.config = config
        self.email_sender = None
        self.alert_history = []
        
        # Initialize email sender if configured
        if 'email_alerts' in config and config['email_alerts'].get('enabled', False):
            self.email_sender = EmailAlertSender(config['email_alerts'])
            logger.info("Email alerts enabled")
        else:
            logger.info("Email alerts disabled")
        
        # Initialize other notification channels here
        # e.g., SMS, Push notifications, etc.
    
    def send_outbreak_alert(self, 
                           alert_data: Dict,
                           attachments: List[str] = None) -> Dict:
        """
        Send outbreak alert through all available channels
        
        Args:
            alert_data: Alert data
            attachments: Files to attach
            
        Returns:
            Dictionary with sending results
        """
        results = {
            'timestamp': datetime.now(),
            'alert_id': alert_data.get('id'),
            'severity': alert_data.get('severity'),
            'channels': {}
        }
        
        # Check if we should send based on severity threshold
        if not self._should_send_alert(alert_data):
            results['skipped'] = True
            results['reason'] = 'Below severity threshold'
            return results
        
        # Send via email if enabled
        if self.email_sender:
            try:
                email_success = self.email_sender.send_alert(
                    alert_data, 
                    attachments
                )
                results['channels']['email'] = {
                    'success': email_success,
                    'recipients': self.email_sender.config.get('recipients', [])
                }
                
                if email_success:
                    logger.info(f"Email alert sent for {alert_data.get('farm_id')}")
                else:
                    logger.error(f"Failed to send email alert for {alert_data.get('farm_id')}")
                    
            except Exception as e:
                results['channels']['email'] = {
                    'success': False,
                    'error': str(e)
                }
                logger.error(f"Error sending email alert: {str(e)}")
        
        # Add other notification channels here
        # e.g., SMS, Push notifications
        
        # Store in history
        self.alert_history.append({
            'timestamp': datetime.now(),
            'alert_data': alert_data,
            'results': results
        })
        
        # Keep only last 100 alerts in history
        if len(self.alert_history) > 100:
            self.alert_history = self.alert_history[-100:]
        
        return results
    
    def send_daily_report(self, 
                         report_data: Dict,
                         report_file: str = None) -> Dict:
        """
        Send daily report
        
        Args:
            report_data: Report data
            report_file: Path to report file
            
        Returns:
            Sending results
        """
        results = {
            'timestamp': datetime.now(),
            'type': 'daily_report',
            'channels': {}
        }
        
        # Send via email if enabled
        if self.email_sender:
            try:
                email_success = self.email_sender.send_daily_report(
                    report_data,
                    report_file
                )
                results['channels']['email'] = {
                    'success': email_success
                }
                
                if email_success:
                    logger.info("Daily report email sent")
                else:
                    logger.error("Failed to send daily report email")
                    
            except Exception as e:
                results['channels']['email'] = {
                    'success': False,
                    'error': str(e)
                }
                logger.error(f"Error sending daily report: {str(e)}")
        
        return results
    
    def _should_send_alert(self, alert_data: Dict) -> bool:
        """
        Check if alert should be sent based on severity threshold
        
        Args:
            alert_data: Alert data
            
        Returns:
            True if alert should be sent
        """
        if not self.email_sender:
            return False
        
        # Get severity threshold from config
        severity_threshold = self.email_sender.config.get('alert_threshold', 'high')
        
        # Define severity levels
        severity_levels = {
            'critical': 4,
            'high': 3,
            'medium': 2,
            'low': 1
        }
        
        # Get alert severity
        alert_severity = alert_data.get('severity', 'low').lower()
        
        # Check if alert severity meets threshold
        alert_level = severity_levels.get(alert_severity, 0)
        threshold_level = severity_levels.get(severity_threshold.lower(), 0)
        
        return alert_level >= threshold_level
    
    def get_alert_history(self, 
                         limit: int = 20,
                         severity: Optional[str] = None) -> List[Dict]:
        """
        Get alert history
        
        Args:
            limit: Maximum number of alerts to return
            severity: Filter by severity
            
        Returns:
            List of alert history entries
        """
        history = self.alert_history.copy()
        
        # Filter by severity if specified
        if severity:
            history = [
                entry for entry in history
                if entry['alert_data'].get('severity', '').lower() == severity.lower()
            ]
        
        # Sort by timestamp (newest first)
        history.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return history[:limit]
    
    def clear_alert_history(self):
        """Clear alert history"""
        self.alert_history = []
        logger.info("Alert history cleared")