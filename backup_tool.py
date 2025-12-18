#!/usr/bin/env python3
"""
Backup and restore management CLI tool
"""
import argparse
import sys
import os
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from backup.manager import (
    BackupManager, BackupConfig, BackupItem, BackupType, BackupStrategy,
    get_backup_manager, reset_backup_manager
)


class BackupCLI:
    """Command-line interface for backup management"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = None
        
        if config_file and Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    config_data = json.load(f)
                self.config = BackupConfig(**config_data)
            except Exception as e:
                print(f"Failed to load config from {config_file}: {e}")
        
        self.manager = get_backup_manager(self.config)
    
    def create(self, 
              backup_type: str = "full",
              comment: str = "",
              start_scheduler: bool = False) -> None:
        """Create a new backup"""
        try:
            backup_type_enum = BackupType(backup_type.lower())
        except ValueError:
            print(f"Invalid backup type: {backup_type}")
            print(f"Available types: {[t.value for t in BackupType]}")
            return
        
        print(f"Creating {backup_type} backup...")
        
        record = self.manager.create_backup(backup_type_enum, comment)
        
        if record:
            print(f"✅ Backup created successfully!")
            print(f"   ID: {record.id}")
            print(f"   Timestamp: {record.timestamp}")
            print(f"   Size: {self._format_bytes(record.total_size_bytes)}")
            print(f"   Items: {len(record.items)}")
            
            if start_scheduler:
                self.manager.start_scheduler()
                print(f"✅ Backup scheduler started")
        else:
            print("❌ Backup creation failed")
    
    def list_backups(self, show_all: bool = False, json_output: bool = False) -> None:
        """List all backups"""
        backups = self.manager.list_backups()
        
        if not backups:
            print("No backups found")
            return
        
        completed = [b for b in backups if b.status == "completed"]
        failed = [b for b in backups if b.status == "failed"]
        
        if json_output:
            output = []
            for backup in backups:
                output.append({
                    "id": backup.id,
                    "timestamp": backup.timestamp.isoformat(),
                    "type": backup.backup_type.value,
                    "status": backup.status,
                    "size_bytes": backup.total_size_bytes,
                    "size_human": self._format_bytes(backup.total_size_bytes),
                    "items": len(backup.items),
                    "error": backup.error_message
                })
            print(json.dumps(output, indent=2, default=str))
            return
        
        print("\n" + "=" * 100)
        print("📦 BACKUP LIST")
        print("=" * 100)
        
        print(f"\nTotal Backups: {len(backups)}")
        print(f"  ✅ Completed: {len(completed)}")
        print(f"  ❌ Failed: {len(failed)}")
        
        if show_all or len(backups) <= 20:
            backups_to_show = backups
        else:
            backups_to_show = backups[:20]
            print(f"\nShowing 20 most recent backups (use --all to see all):")
        
        print(f"\n{'ID':40} {'Timestamp':20} {'Type':10} {'Status':10} {'Size':12} {'Items':6}")
        print("-" * 100)
        
        for backup in backups_to_show:
            status_icon = "✅" if backup.status == "completed" else "❌"
            print(f"{backup.id[:40]:40} {backup.timestamp.strftime('%Y-%m-%d %H:%M:%S'):20} "
                  f"{backup.backup_type.value:10} {status_icon:4}{backup.status:6} "
                  f"{self._format_bytes(backup.total_size_bytes):12} {len(backup.items):6}")
        
        if failed:
            print(f"\n⚠️  Failed Backups:")
            for backup in failed[:5]:
                print(f"  • {backup.id}: {backup.error_message}")
    
    def restore(self, 
               backup_id: str,
               target_dir: Optional[str] = None,
               items: Optional[List[str]] = None,
               verify: bool = True) -> None:
        """Restore a backup"""
        print(f"Restoring backup: {backup_id}")
        
        target_path = Path(target_dir) if target_dir else None
        
        success = self.manager.restore_backup(
            backup_id=backup_id,
            target_dir=target_path,
            items=items,
            verify_checksum=verify
        )
        
        if success:
            print(f"✅ Backup restored successfully!")
        else:
            print(f"❌ Backup restore failed")
    
    def delete(self, backup_id: str, force: bool = False) -> None:
        """Delete a backup"""
        if not force:
            confirm = input(f"Are you sure you want to delete backup '{backup_id}'? (y/N): ")
            if confirm.lower() != 'y':
                print("Deletion cancelled")
                return
        
        # Find backup directory
        backup_dirs = list(self.manager.backup_dir.glob(f"{backup_id}_*"))
        if not backup_dirs:
            print(f"Backup not found: {backup_id}")
            return
        
        backup_dir = backup_dirs[0]
        
        try:
            import shutil
            shutil.rmtree(backup_dir)
            
            # Remove from history
            self.manager.backup_history = [
                b for b in self.manager.backup_history 
                if b.id != backup_id
            ]
            self.manager._save_history()
            
            print(f"✅ Backup deleted: {backup_id}")
            print(f"   Removed: {backup_dir}")
            
        except Exception as e:
            print(f"❌ Failed to delete backup: {str(e)}")
    
    def verify(self, backup_id: str) -> None:
        """Verify backup integrity"""
        print(f"Verifying backup: {backup_id}")
        
        success = self.manager.verify_backup(backup_id)
        
        if success:
            print(f"✅ Backup verification passed!")
        else:
            print(f"❌ Backup verification failed")
    
    def stats(self, json_output: bool = False) -> None:
        """Show backup statistics"""
        stats = self.manager.get_stats()
        
        if json_output:
            print(json.dumps(stats, indent=2, default=str))
            return
        
        print("\n" + "=" * 100)
        print("📊 BACKUP STATISTICS")
        print("=" * 100)
        
        print(f"\n📦 Backup Overview:")
        print(f"  Total Backups:      {stats['total_backups']}")
        print(f"  Successful:         {stats['completed']}")
        print(f"  Failed:             {stats['failed']}")
        print(f" 