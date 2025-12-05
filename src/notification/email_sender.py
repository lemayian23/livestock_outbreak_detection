"""
Email notification system for outbreak alerts
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
from datetime import datetime
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmailAlertSender:
    """Send email alerts for outbreak detection"""
    
    def __init__(self, config: Dict):
        """
        Initialize email sender
        
        Args:
            config: Email configuration dictionary
        """
        self.config = config
        self.enabled = config.get('enabled', False)
        
        if self.enabled:
            self._validate_config()
    
    def _validate_config(self):
        """Validate email configuration"""
        required_fields = ['smtp_server', 'smtp_port', 'sender_email', 'recipients']
        
        for field in required_fields:
            if field not in self.config or not self.config[field]:
                self.enabled = False
                logger.warning(f"Email alerts disabled: Missing {field}")
                return
        
        # Check if we have password for authentication
        if not self.config.get('sender_password'):
            logger.warning("Email password not set. Alerts may fail to send.")
    
    def send_alert(self, 
                  alert_data: Dict,
                  attachments: List[str] = None,
                  custom_subject: str = None,
                  custom_body: str = None) -> bool:
        """
        Send email alert
        
        Args:
            alert_data: Alert data dictionary
            attachments: List of file paths to attach
            custom_subject: Custom subject line (optional)
            custom_body: Custom body text (optional)
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.info("Email alerts are disabled")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart()
            
            # Set subject
            if custom_subject:
                subject = custom_subject
            else:
                subject = self._generate_subject(alert_data)
            msg['Subject'] = subject
            
            # Set sender and recipients
            msg['From'] = self.config['sender_email']
            msg['To'] = ', '.join(self.config['recipients'])
            
            # Create email body
            if custom_body:
                body = custom_body
            else:
                body = self._generate_body(alert_data)
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Add attachments if any
            if attachments and self.config.get('include_attachments', True):
                for attachment_path in attachments:
                    if os.path.exists(attachment_path):
                        self._add_attachment(msg, attachment_path)
                    else:
                        logger.warning(f"Attachment not found: {attachment_path}")
            
            # Send email
            success = self._send_email(msg)
            
            if success:
                logger.info(f"Alert email sent: {subject}")
            else:
                logger.error("Failed to send alert email")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending email alert: {str(e)}")
            return False
    
    def _generate_subject(self, alert_data: Dict) -> str:
        """
        Generate email subject line
        
        Args:
            alert_data: Alert data
            
        Returns:
            Subject line
        """
        severity = alert_data.get('severity', 'ALERT').upper()
        farm_id = alert_data.get('farm_id', 'Unknown Farm')
        affected = alert_data.get('affected_animals', 0)
        
        return f"[{severity}] Livestock Outbreak Alert - Farm {farm_id} - {affected} Animals Affected"
    
    def _generate_body(self, alert_data: Dict) -> str:
        """
        Generate email body text
        
        Args:
            alert_data: Alert data
            
        Returns:
            Email body
        """
        body = "LIVESTOCK DISEASE OUTBREAK ALERT\n"
        body += "=" * 50 + "\n\n"
        
        # Alert details
        body += f"ALERT SEVERITY: {alert_data.get('severity', 'UNKNOWN').upper()}\n"
        body += f"FARM ID: {alert_data.get('farm_id', 'Unknown')}\n"
        body += f"DETECTION TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        body += f"ALERT GENERATED: {alert_data.get('created_at', datetime.now()).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # Outbreak details
        body += "OUTBREAK DETAILS:\n"
        body += "-" * 20 + "\n"
        body += f"Affected Animals: {alert_data.get('affected_animals', 0)}\n"
        
        if 'start_date' in alert_data:
            body += f"Start Date: {alert_data['start_date'].strftime('%Y-%m-%d')}\n"
        
        if 'end_date' in alert_data:
            body += f"End Date: {alert_data['end_date'].strftime('%Y-%m-%d')}\n"
        
        if 'animal_types' in alert_data and alert_data['animal_types']:
            body += f"Animal Types: {', '.join(alert_data['animal_types'])}\n"
        
        if 'detection_methods' in alert_data:
            body += f"Detection Methods: {', '.join(alert_data['detection_methods'])}\n"
        
        if 'avg_anomaly_score' in alert_data:
            body += f"Average Anomaly Score: {alert_data['avg_anomaly_score']:.2f}\n"
        
        if 'ensemble_score' in alert_data:
            body += f"Ensemble Confidence Score: {alert_data['ensemble_score']:.2f}/10.0\n"
        
        # Metrics affected
        if 'features_contributing' in alert_data and alert_data['features_contributing']:
            body += f"Key Metrics Affected: {', '.join(alert_data['features_contributing'])}\n"
        
        # Description
        if 'description' in alert_data:
            body += f"\nDescription: {alert_data['description']}\n"
        
        # Recommendations
        body += "\nRECOMMENDED ACTIONS:\n"
        body += "-" * 20 + "\n"
        
        severity = alert_data.get('severity', 'medium').lower()
        
        if severity == 'critical':
            body += "1. IMMEDIATELY isolate affected animals\n"
            body += "2. Contact veterinary authorities\n"
            body += "3. Implement biosecurity measures\n"
            body += "4. Monitor all animals closely\n"
            body += "5. Consider temporary movement restrictions\n"
        elif severity == 'high':
            body += "1. Isolate symptomatic animals\n"
            body += "2. Increase monitoring frequency\n"
            body += "3. Review feeding and water sources\n"
            body += "4. Contact veterinary services\n"
        elif severity == 'medium':
            body += "1. Monitor affected animals closely\n"
            body += "2. Check environmental conditions\n"
            body += "3. Review recent changes\n"
            body += "4. Prepare isolation areas if needed\n"
        else:  # low
            body += "1. Continue monitoring\n"
            body += "2. Document observations\n"
            body += "3. Review normal ranges\n"
        
        # Footer
        body += "\n" + "=" * 50 + "\n"
        body += "This alert was automatically generated by the Livestock Outbreak Detection System.\n"
        body += "Please verify the information and take appropriate actions.\n"
        
        return body
    
    def _add_attachment(self, msg: MIMEMultipart, filepath: str):
        """
        Add file attachment to email
        
        Args:
            msg: Email message
            filepath: Path to file
        """
        filename = os.path.basename(filepath)
        
        with open(filepath, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={filename}",
        )
        
        msg.attach(part)
    
    def _send_email(self, msg: MIMEMultipart) ->