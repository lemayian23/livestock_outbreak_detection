"""
Data backup and restore manager with compression and scheduling
"""
import os
import shutil
import json
import zipfile
import tarfile
import hashlib
import logging
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
import tempfile
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class BackupType(Enum):
    """Types of backups"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


class BackupStrategy(Enum):
    """Backup strategies"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ON_DEMAND = "on_demand"


@dataclass
class BackupItem:
    """Single item to backup"""
    source: Path
    destination_name: str
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    compress: bool = True
    critical: bool = False
    description: str = ""


@dataclass
class BackupConfig:
    """Backup configuration"""
    name: str = "livestock_backup"
    version: str = "1.0"
    backup_dir: Path = field(default_factory=lambda: Path("backups"))
    retention_days: int = 30
    max_backups: int = 10
    compression_level: int = 6
    backup_type: BackupType = BackupType.FULL
    strategy: BackupStrategy = BackupStrategy.DAILY
    items: List[BackupItem] = field(default_factory=list)
    schedule_enabled: bool = True
    schedule_hour: int = 2  # 2 AM
    schedule_minute: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if isinstance(self.backup_dir, str):
            self.backup_dir = Path(self.backup_dir)


@dataclass
class BackupRecord:
    """Record of a completed backup"""
    id: str
    timestamp: datetime
    config_name: str
    backup_type: BackupType
    strategy: BackupStrategy
    items: List[str]
    total_size_bytes: int
    compressed_size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    status: str = "completed"
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class BackupManager:
    """
    Manages automated backups with compression, scheduling, and restore capabilities
    """
    
    def __init__(self, config: Optional[BackupConfig] = None):
        self.config = config or self._default_config()
        self.backup_dir = self.config.backup_dir
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        self.scheduler_thread: Optional[threading.Thread] = None
        self.scheduler_running = False
        self.backup_history: List[BackupRecord] = []
        
        self._load_history()
        self._setup_default_items()
        
        logger.info(f"BackupManager initialized for '{self.config.name}'")
        logger.info(f"Backup directory: {self.backup_dir.absolute()}")
    
    def _default_config(self) -> BackupConfig:
        """Create default configuration"""
        return BackupConfig(
            name="livestock_outbreak_detection",
            backup_dir=Path("backups"),
            retention_days=30,
            max_backups=10,
            compression_level=6,
            backup_type=BackupType.FULL,
            strategy=BackupStrategy.DAILY,
            schedule_enabled=True,
            schedule_hour=2,
            schedule_minute=0
        )
    
    def _setup_default_items(self) -> None:
        """Setup default backup items if none provided"""
        if not self.config.items:
            base_dir = Path(".")
            
            # Data directories
            self.config.items.extend([
                BackupItem(
                    source=base_dir / "data",
                    destination_name="data",
                    include_patterns=["*.csv", "*.json", "*.parquet"],
                    exclude_patterns=["*.tmp", "temp_*"],
                    compress=True,
                    critical=True,
                    description="Raw and processed data"
                ),
                BackupItem(
                    source=base_dir / "outputs",
                    destination_name="outputs",
                    include_patterns=["*.csv", "*.json", "*.html", "*.pdf"],
                    compress=True,
                    critical=False,
                    description="Generated reports and outputs"
                ),
                BackupItem(
                    source=base_dir / "models" / "saved_models",
                    destination_name="models",
                    include_patterns=["*.pkl", "*.joblib", "*.h5", "*.pt"],
                    compress=True,
                    critical=True,
                    description="Trained ML models"
                ),
                BackupItem(
                    source=base_dir / "config",
                    destination_name="config",
                    include_patterns=["*.yaml", "*.yml", "*.json"],
                    compress=True,
                    critical=True,
                    description="Configuration files"
                ),
                BackupItem(
                    source=base_dir,
                    destination_name="scripts",
                    include_patterns=["*.py", "requirements*.txt", "README.md"],
                    exclude_patterns=["__pycache__", "*.pyc", "venv", ".git"],
                    compress=True,
                    critical=False,
                    description="Source code and scripts"
                )
            ])
            
            logger.info(f"Added {len(self.config.items)} default backup items")
    
    def create_backup(self, 
                     backup_type: Optional[BackupType] = None,
                     comment: str = "") -> Optional[BackupRecord]:
        """
        Create a new backup
        
        Args:
            backup_type: Type of backup (full/incremental/differential)
            comment: Optional comment for the backup
            
        Returns:
            BackupRecord if successful, None otherwise
        """
        backup_type = backup_type or self.config.backup_type
        backup_id = self._generate_backup_id(backup_type)
        
        logger.info(f"Starting backup '{backup_id}' ({backup_type.value})")
        
        try:
            # Create backup directory
            timestamp = datetime.now()
            backup_name = f"{backup_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
            backup_path = self.backup_dir / backup_name
            
            if backup_path.exists():
                logger.warning(f"Backup path already exists: {backup_path}")
                backup_path = self.backup_dir / f"{backup_name}_{int(time.time())}"
            
            backup_path.mkdir(parents=True, exist_ok=True)
            
            # Save backup metadata
            metadata = {
                "id": backup_id,
                "timestamp": timestamp.isoformat(),
                "type": backup_type.value,
                "config_name": self.config.name,
                "comment": comment,
                "items": []
            }
            
            # Backup each item
            total_size = 0
            compressed_size = 0
            backed_up_items = []
            
            for item in self.config.items:
                if not item.source.exists():
                    if item.critical:
                        logger.warning(f"Critical backup item not found: {item.source}")
                    else:
                        logger.debug(f"Backup item not found (non-critical): {item.source}")
                    continue
                
                logger.debug(f"Backing up: {item.source} -> {item.destination_name}")
                
                item_backup_path = backup_path / item.destination_name
                item_backup_path.mkdir(parents=True, exist_ok=True)
                
                # Copy files
                item_size = self._backup_item(item, item_backup_path)
                total_size += item_size
                
                # Compress if requested
                if item.compress and item_size > 0:
                    compressed_item_size = self._compress_directory(
                        item_backup_path, 
                        self.config.compression_level
                    )
                    compressed_size += compressed_item_size
                
                backed_up_items.append(item.destination_name)
                
                item_metadata = {
                    "name": item.destination_name,
                    "source": str(item.source),
                    "size_bytes": item_size,
                    "compressed": item.compress,
                    "critical": item.critical
                }
                metadata["items"].append(item_metadata)
            
            # Save metadata to file
            metadata_file = backup_path / "backup_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            # Calculate checksum
            checksum = self._calculate_checksum(backup_path)
            
            # Create backup record
            record = BackupRecord(
                id=backup_id,
                timestamp=timestamp,
                config_name=self.config.name,
                backup_type=backup_type,
                strategy=self.config.strategy,
                items=backed_up_items,
                total_size_bytes=total_size,
                compressed_size_bytes=compressed_size if compressed_size > 0 else None,
                checksum=checksum,
                status="completed",
                metadata={"comment": comment}
            )
            
            # Save record
            self._save_record(record)
            self.backup_history.append(record)
            
            # Clean up old backups
            self._cleanup_old_backups()
            
            logger.info(f"Backup '{backup_id}' completed successfully")
            logger.info(f"  Items: {len(backed_up_items)}")
            logger.info(f"  Size: {self._format_bytes(total_size)}")
            if compressed_size:
                logger.info(f"  Compressed: {self._format_bytes(compressed_size)}")
            logger.info(f"  Location: {backup_path}")
            
            return record
            
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            
            # Create failure record
            record = BackupRecord(
                id=backup_id,
                timestamp=datetime.now(),
                config_name=self.config.name,
                backup_type=backup_type,
                strategy=self.config.strategy,
                items=[],
                total_size_bytes=0,
                status="failed",
                error_message=str(e),
                metadata={"comment": comment}
            )
            
            self._save_record(record)
            self.backup_history.append(record)
            
            return None
    
    def _backup_item(self, item: BackupItem, destination: Path) -> int:
        """
        Backup a single item
        
        Returns:
            Total size in bytes
        """
        total_size = 0
        
        if item.source.is_file():
            # Single file
            shutil.copy2(item.source, destination)
            total_size = item.source.stat().st_size
            
        elif item.source.is_dir():
            # Directory
            for root, dirs, files in os.walk(item.source):
                # Apply exclude patterns
                dirs[:] = [d for d in dirs if not self._matches_patterns(d, item.exclude_patterns)]
                
                for file in files:
                    file_path = Path(root) / file
                    
                    # Check patterns
                    if item.include_patterns and not self._matches_patterns(file, item.include_patterns):
                        continue
                    if self._matches_patterns(file, item.exclude_patterns):
                        continue
                    
                    # Calculate relative path
                    rel_path = file_path.relative_to(item.source)
                    dest_path = destination / rel_path
                    
                    # Create parent directories
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Copy file
                    shutil.copy2(file_path, dest_path)
                    total_size += file_path.stat().st_size
        
        return total_size
    
    def _matches_patterns(self, filename: str, patterns: List[str]) -> bool:
        """Check if filename matches any of the patterns"""
        import fnmatch
        
        for pattern in patterns:
            if fnmatch.fnmatch(filename, pattern):
                return True
        return False
    
    def _compress_directory(self, directory: Path, compression_level: int = 6) -> int:
        """
        Compress a directory using zip format
        
        Returns:
            Compressed size in bytes
        """
        if not directory.exists() or not any(directory.iterdir()):
            return 0
        
        zip_path = directory.with_suffix('.zip')
        
        with zipfile.ZipFile(
            zip_path, 
            'w', 
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=compression_level
        ) as zipf:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = Path(root) / file
                    arcname = file_path.relative_to(directory.parent)
                    zipf.write(file_path, arcname)
        
        # Remove original directory
        shutil.rmtree(directory)
        
        return zip_path.stat().st_size
    
    def _generate_backup_id(self, backup_type: BackupType) -> str:
        """Generate a unique backup ID"""
        timestamp = datetime.now().strftime('%Y%m%d')
        type_code = backup_type.value[0].upper()
        random_suffix = hashlib.md5(str(time.time()).encode()).hexdigest()[:6]
        
        return f"{self.config.name}_{timestamp}_{type_code}_{random_suffix}"
    
    def _calculate_checksum(self, directory: Path) -> str:
        """Calculate MD5 checksum of backup directory"""
        hash_md5 = hashlib.md5()
        
        for root, dirs, files in os.walk(directory):
            for file in sorted(files):  # Sort for consistent hashing
                file_path = Path(root) / file
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b''):
                        hash_md5.update(chunk)
        
        return hash_md5.hexdigest()
    
    def _save_record(self, record: BackupRecord) -> None:
        """Save backup record to history file"""
        history_file = self.backup_dir / "backup_history.json"
        
        records_data = []
        if history_file.exists():
            with open(history_file, 'r') as f:
                records_data = json.load(f)
        
        record_dict = {
            "id": record.id,
            "timestamp": record.timestamp.isoformat(),
            "config_name": record.config_name,
            "backup_type": record.backup_type.value,
            "strategy": record.strategy.value,
            "items": record.items,
            "total_size_bytes": record.total_size_bytes,
            "compressed_size_bytes": record.compressed_size_bytes,
            "checksum": record.checksum,
            "status": record.status,
            "error_message": record.error_message,
            "metadata": record.metadata
        }
        
        records_data.append(record_dict)
        
        # Keep only last 100 records in file
        if len(records_data) > 100:
            records_data = records_data[-100:]
        
        with open(history_file, 'w') as f:
            json.dump(records_data, f, indent=2, default=str)
    
    def _load_history(self) -> None:
        """Load backup history from file"""
        history_file = self.backup_dir / "backup_history.json"
        
        if not history_file.exists():
            return
        
        try:
            with open(history_file, 'r') as f:
                records_data = json.load(f)
            
            for record_data in records_data:
                record = BackupRecord(
                    id=record_data["id"],
                    timestamp=datetime.fromisoformat(record_data["timestamp"]),
                    config_name=record_data["config_name"],
                    backup_type=BackupType(record_data["backup_type"]),
                    strategy=BackupStrategy(record_data["strategy"]),
                    items=record_data["items"],
                    total_size_bytes=record_data["total_size_bytes"],
                    compressed_size_bytes=record_data.get("compressed_size_bytes"),
                    checksum=record_data.get("checksum"),
                    status=record_data["status"],
                    error_message=record_data.get("error_message"),
                    metadata=record_data.get("metadata", {})
                )
                self.backup_history.append(record)
            
            logger.info(f"Loaded {len(self.backup_history)} backup records from history")
            
        except Exception as e:
            logger.error(f"Failed to load backup history: {str(e)}")
    
    def list_backups(self) -> List[BackupRecord]:
        """List all available backups"""
        return self.backup_history.copy()
    
    def get_backup(self, backup_id: str) -> Optional[BackupRecord]:
        """Get backup record by ID"""
        for record in self.backup_history:
            if record.id == backup_id:
                return record
        return None
    
    def restore_backup(self, 
                      backup_id: str,
                      target_dir: Optional[Path] = None,
                      items: Optional[List[str]] = None,
                      verify_checksum: bool = True) -> bool:
        """
        Restore a backup
        
        Args:
            backup_id: ID of backup to restore
            target_dir: Directory to restore to (default: original locations)
            items: Specific items to restore (None for all)
            verify_checksum: Verify backup integrity before restore
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Attempting to restore backup: {backup_id}")
        
        # Find backup record
        record = self.get_backup(backup_id)
        if not record:
            logger.error(f"Backup not found: {backup_id}")
            return False
        
        if record.status != "completed":
            logger.error(f"Cannot restore failed backup: {backup_id}")
            return False
        
        # Find backup directory
        backup_dirs = list(self.backup_dir.glob(f"{backup_id}_*"))
        if not backup_dirs:
            logger.error(f"Backup directory not found for: {backup_id}")
            return False
        
        backup_dir = backup_dirs[0]
        
        # Verify checksum if requested
        if verify_checksum and record.checksum:
            current_checksum = self._calculate_checksum(backup_dir)
            if current_checksum != record.checksum:
                logger.error(f"Backup integrity check failed for: {backup_id}")
                logger.error(f"Expected: {record.checksum}, Got: {current_checksum}")
                return False
        
        try:
            # Load backup metadata
            metadata_file = backup_dir / "backup_metadata.json"
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            restored_items = []
            
            for item_metadata in metadata.get("items", []):
                item_name = item_metadata["name"]
                
                # Skip if not in requested items
                if items and item_name not in items:
                    continue
                
                item_backup_path = backup_dir / item_name
                
                # Handle compressed items
                if item_metadata.get("compressed", False):
                    zip_path = item_backup_path.with_suffix('.zip')
                    if zip_path.exists():
                        # Extract compressed item
                        self._extract_compressed(zip_path, target_dir or Path("."))
                    else:
                        logger.warning(f"Compressed backup not found: {zip_path}")
                        continue
                else:
                    # Copy uncompressed item
                    source_path = item_metadata["source"]
                    restore_path = target_dir or Path(source_path).parent
                    
                    if item_backup_path.is_file():
                        shutil.copy2(item_backup_path, restore_path)
                    elif item_backup_path.is_dir():
                        dest_path = restore_path / item_name
                        if dest_path.exists():
                            logger.warning(f"Destination already exists, skipping: {dest_path}")
                            continue
                        shutil.copytree(item_backup_path, dest_path)
                
                restored_items.append(item_name)
                logger.info(f"Restored: {item_name}")
            
            logger.info(f"Restore completed for backup: {backup_id}")
            logger.info(f"Restored items: {len(restored_items)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {str(e)}")
            return False
    
    def _extract_compressed(self, zip_path: Path, target_dir: Path) -> None:
        """Extract compressed backup"""
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            zipf.extractall(target_dir)
    
    def _cleanup_old_backups(self) -> None:
        """Clean up old backups based on retention policy"""
        if not self.config.retention_days and not self.config.max_backups:
            return
        
        now = datetime.now()
        backups_to_keep = []
        backups_to_delete = []
        
        # Group backups by day for retention policy
        for record in self.backup_history:
            if record.status != "completed":
                continue
            
            # Find backup directory
            backup_dirs = list(self.backup_dir.glob(f"{record.id}_*"))
            if not backup_dirs:
                continue
            
            backup_dir = backup_dirs[0]
            
            # Check age
            age_days = (now - record.timestamp).days
            
            keep = True
            
            # Check retention days
            if self.config.retention_days and age_days > self.config.retention_days:
                keep = False
            
            # Check max backups
            if self.config.max_backups:
                successful_backups = [r for r in self.backup_history if r.status == "completed"]
                if len(successful_backups) > self.config.max_backups:
                    # Keep most recent N backups
                    recent_backups = sorted(
                        successful_backups, 
                        key=lambda x: x.timestamp, 
                        reverse=True
                    )[:self.config.max_backups]
                    if record not in recent_backups:
                        keep = False
            
            if keep:
                backups_to_keep.append(record)
            else:
                backups_to_delete.append((record, backup_dir))
        
        # Delete old backups
        for record, backup_dir in backups_to_delete:
            try:
                if backup_dir.exists():
                    shutil.rmtree(backup_dir)
                    logger.info(f"Deleted old backup: {record.id} ({backup_dir})")
                
                # Remove from history
                self.backup_history = [r for r in self.backup_history if r.id != record.id]
                
            except Exception as e:
                logger.error(f"Failed to delete backup {record.id}: {str(e)}")
        
        # Save updated history
        self._save_history()
    
    def _save_history(self) -> None:
        """Save current history to file"""
        history_file = self.backup_dir / "backup_history.json"
        
        records_data = []
        for record in self.backup_history:
            record_dict = {
                "id": record.id,
                "timestamp": record.timestamp.isoformat(),
                "config_name": record.config_name,
                "backup_type": record.backup_type.value,
                "strategy": record.strategy.value,
                "items": record.items,
                "total_size_bytes": record.total_size_bytes,
                "compressed_size_bytes": record.compressed_size_bytes,
                "checksum": record.checksum,
                "status": record.status,
                "error_message": record.error_message,
                "metadata": record.metadata
            }
            records_data.append(record_dict)
        
        with open(history_file, 'w') as f:
            json.dump(records_data, f, indent=2, default=str)
    
    def start_scheduler(self) -> None:
        """Start backup scheduler"""
        if self.scheduler_running:
            logger.warning("Backup scheduler already running")
            return
        
        if not self.config.schedule_enabled:
            logger.info("Backup scheduler is disabled in config")
            return
        
        self.scheduler_running = True
        
        def scheduler_loop():
            logger.info("Backup scheduler started")
            
            while self.scheduler_running:
                try:
                    now = datetime.now()
                    
                    # Check if it's time for scheduled backup
                    if (now.hour == self.config.schedule_hour and 
                        now.minute == self.config.schedule_minute):
                        
                        logger.info("Time for scheduled backup")
                        self.create_backup(comment="Scheduled backup")
                        
                        # Sleep for a minute to avoid running multiple times
                        time.sleep(61)
                    
                    # Sleep for 30 seconds
                    time.sleep(30)
                    
                except Exception as e:
                    logger.error(f"Scheduler error: {str(e)}")
                    time.sleep(60)
            
            logger.info("Backup scheduler stopped")
        
        self.scheduler_thread = threading.Thread(
            target=scheduler_loop,
            name="BackupScheduler",
            daemon=True
        )
        self.scheduler_thread.start()
        
        logger.info(f"Backup scheduler started (runs at {self.config.schedule_hour:02d}:{self.config.schedule_minute:02d})")
    
    def stop_scheduler(self) -> None:
        """Stop backup scheduler"""
        self.scheduler_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        logger.info("Backup scheduler stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get backup statistics"""
        completed = [r for r in self.backup_history if r.status == "completed"]
        failed = [r for r in self.backup_history if r.status == "failed"]
        
        total_size = sum(r.total_size_bytes for r in completed)
        avg_size = total_size / len(completed) if completed else 0
        
        return {
            "total_backups": len(self.backup_history),
            "completed": len(completed),
            "failed": len(failed),
            "total_size_bytes": total_size,
            "total_size_human": self._format_bytes(total_size),
            "average_size_bytes": avg_size,
            "average_size_human": self._format_bytes(avg_size),
            "oldest_backup": min(r.timestamp for r in completed) if completed else None,
            "newest_backup": max(r.timestamp for r in completed) if completed else None,
            "success_rate": len(completed) / len(self.backup_history) if self.backup_history else 0
        }
    
    def _format_bytes(self, bytes: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes < 1024.0:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.2f} PB"
    
    def add_backup_item(self, item: BackupItem) -> None:
        """Add a new backup item"""
        self.config.items.append(item)
        logger.info(f"Added backup item: {item.source} -> {item.destination_name}")
    
    def remove_backup_item(self, destination_name: str) -> bool:
        """Remove a backup item by destination name"""
        initial_count = len(self.config.items)
        self.config.items = [item for item in self.config.items 
                           if item.destination_name != destination_name]
        
        removed = len(self.config.items) < initial_count
        if removed:
            logger.info(f"Removed backup item: {destination_name}")
        
        return removed
    
    def verify_backup(self, backup_id: str) -> bool:
        """Verify backup integrity"""
        record = self.get_backup(backup_id)
        if not record:
            logger.error(f"Backup not found: {backup_id}")
            return False
        
        if record.status != "completed":
            logger.error(f"Cannot verify failed backup: {backup_id}")
            return False
        
        # Find backup directory
        backup_dirs = list(self.backup_dir.glob(f"{backup_id}_*"))
        if not backup_dirs:
            logger.error(f"Backup directory not found for: {backup_id}")
            return False
        
        backup_dir = backup_dirs[0]
        
        # Calculate checksum
        current_checksum = self._calculate_checksum(backup_dir)
        
        if current_checksum == record.checksum:
            logger.info(f"Backup verification passed: {backup_id}")
            return True
        else:
            logger.error(f"Backup verification failed: {backup_id}")
            logger.error(f"Expected: {record.checksum}")
            logger.error(f"Got: {current_checksum}")
            return False


# Global backup manager instance
_backup_manager: Optional[BackupManager] = None


def get_backup_manager(config: Optional[BackupConfig] = None) -> BackupManager:
    """Get or create the global backup manager"""
    global _backup_manager
    
    if _backup_manager is None:
        _backup_manager = BackupManager(config)
    
    return _backup_manager


def reset_backup_manager() -> None:
    """Reset the global backup manager (for testing)"""
    global _backup_manager
    _backup_manager = None