#!/usr/bin/env python3
"""
Configuration management CLI tool
"""
import argparse
import logging
import sys
import os
import json
import yaml
from pathlib import Path
from getpass import getpass

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from config_manager.manager import get_config_manager, reset_config_manager
from config_manager.secrets import get_secrets_manager, reset_secrets_manager
from config_manager.environments import get_environment_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConfigCLI:
    """Command-line interface for configuration management"""
    
    def __init__(self, config_dir: str = "config", env: str = None):
        self.config_dir = Path(config_dir)
        self.env = env
        self.config_manager = get_config_manager(config_dir, env)
        self.secrets_manager = get_secrets_manager()
        self.env_manager = get_environment_manager()
    
    def show_config(self, key: str = None, format: str = "yaml") -> None:
        """Show configuration"""
        if key:
            value = self.config_manager.get(key)
            if value is None:
                print(f"Configuration key not found: {key}")
                return
            
            if format == "json":
                print(json.dumps(value, indent=2, default=str))
            else:
                print(yaml.dump(value, default_flow_style=False))
        else:
            config = self.config_manager.export(format)
            if format == "json":
                print(json.dumps(config, indent=2, default=str))
            else:
                print(yaml.dump(config, default_flow_style=False))
    
    def show_env(self) -> None:
        """Show environment information"""
        env_summary = self.env_manager.get_environment_summary()
        
        print("\n" + "=" * 60)
        print("🌍 ENVIRONMENT INFORMATION")
        print("=" * 60)
        
        for key, value in env_summary.items():
            if key == "metadata" and value:
                print(f"\nMetadata:")
                for meta_key, meta_value in value.items():
                    if meta_value:
                        print(f"  {meta_key}: {meta_value}")
            else:
                print(f"{key.replace('_', ' ').title():20}: {value}")
        
        # Show current environment configuration
        print(f"\n📋 Environment Config ({self.env_manager.current_env.value}):")
        env_config = self.env_manager.get_config()
        
        for section, values in env_config.items():
            print(f"\n  {section.upper()}:")
            if isinstance(values, dict):
                for k, v in values.items():
                    print(f"    {k}: {v}")
            else:
                print(f"    {values}")
        
        print("\n" + "=" * 60)
    
    def list_sections(self) -> None:
        """List configuration sections"""
        sections = self.config_manager.list_sections()
        
        print("\n📁 Configuration Sections:")
        print("=" * 40)
        
        for section in sections:
            config_section = self.config_manager.get_section(section)
            if config_section:
                print(f"\n{section}:")
                print(f"  Source: {config_section.source.value}")
                print(f"  Description: {config_section.description}")
                print(f"  Timestamp: {config_section.timestamp}")
    
    def validate_config(self) -> None:
        """Validate configuration"""
        print("Validating configuration...")
        
        try:
            self.config_manager.validate()
            print("✅ Configuration validation passed")
        except Exception as e:
            print(f"❌ Configuration validation failed: {str(e)}")
            sys.exit(1)
        
        # Validate environment
        if self.env_manager.validate_environment():
            print("✅ Environment validation passed")
        else:
            print("❌ Environment validation failed")
            sys.exit(1)
    
    def show_secrets(self, include_values: bool = False) -> None:
        """Show secrets"""
        secrets = self.secrets_manager.list(include_values)
        
        if not secrets:
            print("No secrets found")
            return
        
        print("\n🔒 Secrets:")
        print("=" * 60)
        
        for name, info in secrets.items():
            print(f"\n{name}:")
            print(f"  Source: {info['source']}")
            print(f"  Encrypted: {info['encrypted']}")
            if include_values:
                if info['encrypted']:
                    print(f"  Value: [ENCRYPTED] {self.secrets_manager.mask_value(info['value'])}")
                else:
                    print(f"  Value: {self.secrets_manager.mask_value(info['value'])}")
            if info['description']:
                print(f"  Description: {info['description']}")
    
    def set_secret(self, name: str, value: str = None, 
                  encrypt: bool = True, description: str = "") -> None:
        """Set a secret"""
        if value is None:
            value = getpass(f"Enter value for secret '{name}': ")
            confirm = getpass("Confirm value: ")
            if value != confirm:
                print("Values do not match!")
                return
        
        self.secrets_manager.set(name, value, encrypt, description)
        print(f"✅ Secret '{name}' set")
    
    def get_secret(self, name: str) -> None:
        """Get a secret value"""
        value = self.secrets_manager.get(name)
        if value:
            print(f"{name}: {self.secrets_manager.mask_value(value)}")
        else:
            print(f"Secret '{name}' not found")
    
    def save_secrets(self) -> None:
        """Save secrets to file"""
        self.secrets_manager.save_to_file()
        print("✅ Secrets saved to file")
    
    def create_template(self, template_type: str, output_file: str = None) -> None:
        """Create configuration template"""
        if template_type == "config":
            if not output_file:
                output_file = self.config_dir / "settings.template.yaml"
            
            self.config_manager.create_template(output_file)
            print(f"✅ Configuration template created at {output_file}")
        
        elif template_type == "secrets":
            if not output_file:
                output_file = self.config_dir / "secrets.template.yaml"
            
            self.secrets_manager.create_template(Path(output_file))
            print(f"✅ Secrets template created at {output_file}")
        
        else:
            print(f"Unknown template type: {template_type}")
    
    def check_updates(self) -> None:
        """Check for configuration updates"""
        if self.config_manager.check_for_updates():
            print("🔄 Configuration files have changed and were reloaded")
        else:
            print("✅ Configuration files are up to date")
    
    def switch_env(self, env_name: str) -> None:
        """Switch environment"""
        from config_manager.environments import Environment
        
        env_map = {
            "dev": Environment.DEVELOPMENT,
            "development": Environment.DEVELOPMENT,
            "test": Environment.TESTING,
            "testing": Environment.TESTING,
            "staging": Environment.STAGING,
            "prod": Environment.PRODUCTION,
            "production": Environment.PRODUCTION,
            "demo": Environment.DEMO
        }
        
        if env_name.lower() not in env_map:
            print(f"Unknown environment: {env_name}")
            print(f"Available: {', '.join(env_map.keys())}")
            return
        
        new_env = env_map[env_name.lower()]
        self.env_manager.switch_environment(new_env)
        
        # Reset config manager for new environment
        reset_config_manager()
        self.config_manager = get_config_manager(str(self.config_dir), env_name.lower())
        
        print(f"✅ Switched to {env_name} environment")
    
    def export_config(self, output_file: str, format: str = "yaml") -> None:
        """Export configuration to file"""
        config = self.config_manager.export(format)
        
        with open(output_file, 'w') as f:
            if format == "json":
                json.dump(config, f, indent=2, default=str)
            else:
                yaml.dump(config, f, default_flow_style=False)
        
        print(f"✅ Configuration exported to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Livestock Outbreak Detection - Configuration Management Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s show                           Show all configuration
  %(prog)s show app.name                  Show specific configuration value
  %(prog)s env                            Show environment information
  %(prog)s validate                       Validate configuration
  %(prog)s secrets list                   List all secrets
  %(prog)s secrets set API_KEY            Set a secret (prompt for value)
  %(prog)s template config                Create configuration template
  %(prog)s switch prod                    Switch to production environment
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Show command
    show_parser = subparsers.add_parser('show', help='Show configuration')
    show_parser.add_argument('key', nargs='?', help='Configuration key (dot notation)')
    show_parser.add_argument('--format', choices=['yaml', 'json'], default='yaml',
                           help='Output format')
    
    # Environment command
    subparsers.add_parser('env', help='Show environment information')
    
    # List command
    subparsers.add_parser('list', help='List configuration sections')
    
    # Validate command
    subparsers.add_parser('validate', help='Validate configuration')
    
    # Secrets command
    secrets_parser = subparsers.add_parser('secrets', help='Manage secrets')
    secrets_subparsers = secrets_parser.add_subparsers(dest='secrets_command')
    secrets_subparsers.add_parser('list', help='List secrets')
    secrets_show_parser = secrets_subparsers.add_parser('show', help='Show secret value')
    secrets_show_parser.add_argument('name', help='Secret name')
    secrets_set_parser = secrets_subparsers.add_parser('set', help='Set secret')
    secrets_set_parser.add_argument('name', help='Secret name')
    secrets_set_parser.add_argument('value', nargs='?', help='Secret value (prompt if not provided)')
    secrets_set_parser.add_argument('--no-encrypt', action='store_true',
                                   help='Do not encrypt the secret')
    secrets_set_parser.add_argument('--description', help='Secret description')
    secrets_subparsers.add_parser('save', help='Save secrets to file')
    
    # Template command
    template_parser = subparsers.add_parser('template', help='Create templates')
    template_parser.add_argument('type', choices=['config', 'secrets'],
                                help='Template type')
    template_parser.add_argument('--output', help='Output file')
    
    # Check command
    subparsers.add_parser('check', help='Check for configuration updates')
    
    # Switch command
    switch_parser = subparsers.add_parser('switch', help='Switch environment')
    switch_parser.add_argument('environment', help='Environment name')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export configuration')
    export_parser.add_argument('output_file', help='Output file')
    export_parser.add_argument('--format', choices=['yaml', 'json'], default='yaml',
                              help='Export format')
    
    # Global arguments
    parser.add_argument('--config-dir', default='config',
                       help='Configuration directory')
    parser.add_argument('--env', help='Environment name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialize CLI
    cli = ConfigCLI(args.config_dir, args.env)
    
    # Execute command
    if args.command == 'show':
        cli.show_config(args.key, args.format)
        
    elif args.command == 'env':
        cli.show_env()
        
    elif args.command == 'list':
        cli.list_sections()
        
    elif args.command == 'validate':
        cli.validate_config()
        
    elif args.command == 'secrets':
        if args.secrets_command == 'list':
            cli.show_secrets(include_values=False)
        elif args.secrets_command == 'show':
            cli.get_secret(args.name)
        elif args.secrets_command == 'set':
            cli.set_secret(
                args.name, 
                args.value, 
                not args.no_encrypt,
                args.description or ""
            )
        elif args.secrets_command == 'save':
            cli.save_secrets()
        
    elif args.command == 'template':
        cli.create_template(args.type, args.output)
        
    elif args.command == 'check':
        cli.check_updates()
        
    elif args.command == 'switch':
        cli.switch_env(args.environment)
        
    elif args.command == 'export':
        cli.export_config(args.output_file, args.format)


if __name__ == "__main__":
    main()