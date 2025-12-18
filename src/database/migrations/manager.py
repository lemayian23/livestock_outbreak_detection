"""
Migration manager for handling database schema changes
"""
import os
import logging
import hashlib
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime
import json
from dataclasses import dataclass, field
from enum import Enum

from ..operations import DatabaseOperations

logger = logging.getLogger(__name__)


class MigrationStatus(Enum):
    """Migration status enumeration"""
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class MigrationRecord:
    """Migration execution record"""
    migration_id: str
    name: str
    applied_at: datetime
    checksum: str
    status: MigrationStatus = MigrationStatus.APPLIED
    execution_time_ms: Optional[int] = None
    error_message: Optional[str] = None


class MigrationManager:
    """
    Manages database schema migrations with version control
    """
    
    def __init__(self, db_operations: DatabaseOperations, migrations_dir: str = "migrations"):
        self.db = db_operations
        self.migrations_dir = Path(migrations_dir)
        self.migrations_dir.mkdir(exist_ok=True)
        
        # Ensure migrations table exists
        self._ensure_migrations_table()
        
        logger.info(f"Migration manager initialized with directory: {migrations_dir}")
    
    def _ensure_migrations_table(self) -> None:
        """Create migrations table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            migration_id VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            checksum VARCHAR(64) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'applied',
            execution_time_ms INTEGER,
            error_message TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_migrations_status ON schema_migrations(status);
        CREATE INDEX IF NOT EXISTS idx_migrations_applied_at ON schema_migrations(applied_at);
        """
        
        self.db.execute_query(create_table_sql)
        logger.debug("Migrations table ensured")
    
    def create_migration(self, name: str, description: str = "") -> str:
        """
        Create a new migration template
        
        Args:
            name: Migration name (snake_case)
            description: Optional description
            
        Returns:
            Path to created migration file
        """
        # Generate migration ID
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        migration_id = f"{timestamp}_{name}"
        
        # Create migration file
        migration_file = self.migrations_dir / f"{migration_id}.sql"
        
        template = f"""-- Migration: {name}
-- Created: {datetime.now().isoformat()}
-- Description: {description}

-- UP migration (applying changes)
-- Write your SQL statements here

-- Example:
-- CREATE TABLE IF NOT EXISTS new_table (
--     id SERIAL PRIMARY KEY,
--     name VARCHAR(255) NOT NULL,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- DOWN migration (reverting changes)
-- Write SQL to revert the changes above

-- Example:
-- DROP TABLE IF EXISTS new_table;
"""
        
        migration_file.write_text(template)
        
        logger.info(f"Created migration template: {migration_file}")
        return str(migration_file)
    
    def get_migration_files(self) -> List[Path]:
        """Get all migration files in order"""
        migration_files = sorted(self.migrations_dir.glob("*.sql"))
        return migration_files
    
    def get_applied_migrations(self) -> List[MigrationRecord]:
        """Get all applied migrations from database"""
        query = """
        SELECT migration_id, name, applied_at, checksum, status, 
               execution_time_ms, error_message
        FROM schema_migrations
        WHERE status = 'applied'
        ORDER BY applied_at
        """
        
        results = self.db.fetch_all(query)
        migrations = []
        
        for row in results:
            migration = MigrationRecord(
                migration_id=row['migration_id'],
                name=row['name'],
                applied_at=row['applied_at'],
                checksum=row['checksum'],
                status=MigrationStatus(row['status']),
                execution_time_ms=row['execution_time_ms'],
                error_message=row['error_message']
            )
            migrations.append(migration)
        
        return migrations
    
    def get_pending_migrations(self) -> List[Path]:
        """Get migrations that haven't been applied"""
        applied = {m.migration_id for m in self.get_applied_migrations()}
        all_files = self.get_migration_files()
        
        pending = []
        for file in all_files:
            migration_id = file.stem
            if migration_id not in applied:
                pending.append(file)
        
        return pending
    
    def calculate_checksum(self, filepath: Path) -> str:
        """Calculate SHA256 checksum of a migration file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Remove comments for consistent checksum
        lines = content.split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped.startswith('--'):
                cleaned_lines.append(line)
        
        cleaned_content = '\n'.join(cleaned_lines)
        return hashlib.sha256(cleaned_content.encode()).hexdigest()
    
    def parse_migration(self, filepath: Path) -> Tuple[str, str]:
        """
        Parse migration file into UP and DOWN sections
        
        Returns:
            Tuple of (up_sql, down_sql)
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split by DOWN migration marker
        parts = content.split('-- DOWN migration')
        if len(parts) != 2:
            raise ValueError(f"Invalid migration format in {filepath}. Must contain '-- DOWN migration' section.")
        
        up_section = parts[0]
        down_section = parts[1]
        
        # Extract SQL from UP section (after '-- UP migration')
        up_parts = up_section.split('-- UP migration')
        if len(up_parts) != 2:
            raise ValueError(f"Invalid migration format in {filepath}. Must contain '-- UP migration' section.")
        
        up_sql = up_parts[1].strip()
        down_sql = down_section.strip()
        
        return up_sql, down_sql
    
    def apply_migration(self, filepath: Path, force: bool = False) -> bool:
        """
        Apply a single migration
        
        Args:
            filepath: Path to migration file
            force: Apply even if already applied
            
        Returns:
            True if successful, False otherwise
        """
        migration_id = filepath.stem
        migration_name = '_'.join(migration_id.split('_')[1:])  # Remove timestamp
        
        # Check if already applied
        if not force:
            check_query = "SELECT COUNT(*) as count FROM schema_migrations WHERE migration_id = %s AND status = 'applied'"
            result = self.db.fetch_one(check_query, (migration_id,))
            if result['count'] > 0:
                logger.info(f"Migration {migration_id} already applied, skipping")
                return True
        
        logger.info(f"Applying migration: {migration_id}")
        
        try:
            # Parse migration
            up_sql, down_sql = self.parse_migration(filepath)
            checksum = self.calculate_checksum(filepath)
            
            # Start timing
            start_time = datetime.now()
            
            # Execute migration in transaction
            self.db.execute_query("BEGIN")
            
            # Execute UP SQL
            if up_sql:
                self.db.execute_query(up_sql)
            
            # Record migration
            record_query = """
            INSERT INTO schema_migrations 
            (migration_id, name, checksum, status, execution_time_ms)
            VALUES (%s, %s, %s, 'applied', %s)
            ON CONFLICT (migration_id) DO UPDATE SET
                status = 'applied',
                execution_time_ms = %s,
                applied_at = CURRENT_TIMESTAMP,
                error_message = NULL
            """
            
            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds() * 1000)
            
            self.db.execute_query(record_query, (
                migration_id, migration_name, checksum, 
                execution_time, execution_time
            ))
            
            self.db.execute_query("COMMIT")
            
            logger.info(f"✓ Applied migration {migration_id} in {execution_time}ms")
            return True
            
        except Exception as e:
            self.db.execute_query("ROLLBACK")
            error_msg = str(e)
            
            # Record failure
            try:
                fail_query = """
                INSERT INTO schema_migrations 
                (migration_id, name, checksum, status, error_message)
                VALUES (%s, %s, %s, 'failed', %s)
                ON CONFLICT (migration_id) DO UPDATE SET
                    status = 'failed',
                    error_message = %s,
                    applied_at = CURRENT_TIMESTAMP
                """
                self.db.execute_query(fail_query, (
                    migration_id, migration_name, 
                    self.calculate_checksum(filepath),
                    error_msg, error_msg
                ))
            except Exception as inner_e:
                logger.error(f"Failed to record migration failure: {inner_e}")
            
            logger.error(f"✗ Failed to apply migration {migration_id}: {error_msg}")
            return False
    
    def rollback_migration(self, migration_id: str, force: bool = False) -> bool:
        """
        Rollback a specific migration
        
        Args:
            migration_id: Migration ID to rollback
            force: Rollback even if not applied
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Rolling back migration: {migration_id}")
        
        # Find migration file
        migration_file = self.migrations_dir / f"{migration_id}.sql"
        if not migration_file.exists():
            logger.error(f"Migration file not found: {migration_file}")
            return False
        
        # Check if migration was applied
        check_query = """
        SELECT status, checksum FROM schema_migrations 
        WHERE migration_id = %s
        """
        result = self.db.fetch_one(check_query, (migration_id,))
        
        if not result:
            if not force:
                logger.error(f"Migration {migration_id} not found in database")
                return False
        elif result['status'] != MigrationStatus.APPLIED.value and not force:
            logger.error(f"Migration {migration_id} is not in applied state")
            return False
        
        try:
            # Parse migration to get DOWN SQL
            _, down_sql = self.parse_migration(migration_file)
            
            if not down_sql:
                logger.warning(f"No DOWN SQL found for migration {migration_id}")
            
            # Start timing
            start_time = datetime.now()
            
            # Execute rollback in transaction
            self.db.execute_query("BEGIN")
            
            # Execute DOWN SQL
            if down_sql:
                self.db.execute_query(down_sql)
            
            # Update migration status
            update_query = """
            UPDATE schema_migrations 
            SET status = 'rolled_back', 
                execution_time_ms = %s,
                error_message = NULL
            WHERE migration_id = %s
            """
            
            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds() * 1000)
            
            self.db.execute_query(update_query, (execution_time, migration_id))
            
            self.db.execute_query("COMMIT")
            
            logger.info(f"✓ Rolled back migration {migration_id} in {execution_time}ms")
            return True
            
        except Exception as e:
            self.db.execute_query("ROLLBACK")
            error_msg = str(e)
            
            # Update with error
            try:
                error_query = """
                UPDATE schema_migrations 
                SET error_message = %s
                WHERE migration_id = %s
                """
                self.db.execute_query(error_query, (error_msg, migration_id))
            except Exception as inner_e:
                logger.error(f"Failed to record rollback error: {inner_e}")
            
            logger.error(f"✗ Failed to rollback migration {migration_id}: {error_msg}")
            return False
    
    def apply_all_pending(self, force: bool = False) -> Dict[str, bool]:
        """
        Apply all pending migrations
        
        Returns:
            Dictionary of migration_id -> success status
        """
        pending = self.get_pending_migrations()
        
        if not pending:
            logger.info("No pending migrations to apply")
            return {}
        
        logger.info(f"Applying {len(pending)} pending migrations...")
        
        results = {}
        for migration_file in pending:
            migration_id = migration_file.stem
            success = self.apply_migration(migration_file, force)
            results[migration_id] = success
            
            # Stop on first failure unless forced
            if not success and not force:
                logger.error("Stopping migration due to failure")
                break
        
        return results
    
    def rollback_last(self, count: int = 1, force: bool = False) -> Dict[str, bool]:
        """
        Rollback last N applied migrations
        
        Args:
            count: Number of migrations to rollback
            force: Force rollback even if errors
            
        Returns:
            Dictionary of migration_id -> success status
        """
        applied = self.get_applied_migrations()
        
        if not applied:
            logger.info("No applied migrations to rollback")
            return {}
        
        # Get last N migrations
        to_rollback = applied[-count:]
        
        logger.info(f"Rolling back {len(to_rollback)} migrations...")
        
        results = {}
        for migration in reversed(to_rollback):  # Rollback in reverse order
            success = self.rollback_migration(migration.migration_id, force)
            results[migration.migration_id] = success
            
            # Stop on first failure unless forced
            if not success and not force:
                logger.error("Stopping rollback due to failure")
                break
        
        return results
    
    def validate_migrations(self) -> Tuple[bool, List[str]]:
        """
        Validate all migrations
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check migrations directory exists
        if not self.migrations_dir.exists():
            errors.append(f"Migrations directory not found: {self.migrations_dir}")
            return False, errors
        
        # Check each migration file
        migration_files = self.get_migration_files()
        
        for file in migration_files:
            try:
                # Parse to validate format
                self.parse_migration(file)
                
                # Verify checksum matches if applied
                migration_id = file.stem
                check_query = """
                SELECT checksum FROM schema_migrations 
                WHERE migration_id = %s AND status = 'applied'
                """
                result = self.db.fetch_one(check_query, (migration_id,))
                
                if result:
                    db_checksum = result['checksum']
                    file_checksum = self.calculate_checksum(file)
                    
                    if db_checksum != file_checksum:
                        errors.append(
                            f"Checksum mismatch for {migration_id}. "
                            f"DB: {db_checksum[:8]}, File: {file_checksum[:8]}"
                        )
                
            except Exception as e:
                errors.append(f"Invalid migration {file.name}: {str(e)}")
        
        return len(errors) == 0, errors
    
    def get_migration_history(self, limit: int = 20) -> List[Dict]:
        """Get migration history with details"""
        query = """
        SELECT migration_id, name, applied_at, status, 
               execution_time_ms, error_message,
               CASE 
                   WHEN status = 'applied' THEN '✓'
                   WHEN status = 'failed' THEN '✗'
                   WHEN status = 'rolled_back' THEN '↩'
                   ELSE '?'
               END as status_icon
        FROM schema_migrations
        ORDER BY applied_at DESC
        LIMIT %s
        """
        
        results = self.db.fetch_all(query, (limit,))
        return results
    
    def get_status(self) -> Dict:
        """Get migration system status"""
        # Count by status
        count_query = """
        SELECT status, COUNT(*) as count
        FROM schema_migrations
        GROUP BY status
        """
        
        counts = self.db.fetch_all(count_query)
        status_counts = {row['status']: row['count'] for row in counts}
        
        # Get pending migrations
        pending = self.get_pending_migrations()
        
        # Validate migrations
        is_valid, errors = self.validate_migrations()
        
        return {
            'total_applied': status_counts.get('applied', 0),
            'total_failed': status_counts.get('failed', 0),
            'total_rolled_back': status_counts.get('rolled_back', 0),
            'pending_count': len(pending),
            'is_valid': is_valid,
            'validation_errors': errors,
            'migrations_dir': str(self.migrations_dir.absolute()),
            'database_ready': self.db.test_connection()
        }
    
    def create_initial_migrations(self) -> None:
        """Create initial migrations for livestock outbreak detection system"""
        initial_migrations = [
            {
                'name': 'create_livestock_health_table',
                'description': 'Create main livestock health metrics table',
                'up_sql': """
                CREATE TABLE IF NOT EXISTS livestock_health_metrics (
                    id SERIAL PRIMARY KEY,
                    farm_id VARCHAR(50) NOT NULL,
                    date DATE NOT NULL,
                    animal_type VARCHAR(50) NOT NULL,
                    total_animals INTEGER NOT NULL CHECK (total_animals >= 0),
                    sick_animals INTEGER NOT NULL CHECK (sick_animals >= 0 AND sick_animals <= total_animals),
                    deceased_animals INTEGER NOT NULL CHECK (deceased_animals >= 0 AND deceased_animals <= sick_animals),
                    avg_temperature DECIMAL(4,1) CHECK (avg_temperature BETWEEN 35 AND 45),
                    feed_intake_percent DECIMAL(5,2) CHECK (feed_intake_percent BETWEEN 0 AND 100),
                    water_intake_percent DECIMAL(5,2) CHECK (water_intake_percent BETWEEN 0 AND 100),
                    activity_level DECIMAL(3,1) CHECK (activity_level BETWEEN 0 AND 10),
                    location_lat DECIMAL(9,6),
                    location_lon DECIMAL(9,6),
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(farm_id, date, animal_type)
                );
                
                CREATE INDEX IF NOT EXISTS idx_health_farm_date ON livestock_health_metrics(farm_id, date);
                CREATE INDEX IF NOT EXISTS idx_health_animal_type ON livestock_health_metrics(animal_type);
                """,
                'down_sql': """
                DROP TABLE IF EXISTS livestock_health_metrics;
                """
            },
            {
                'name': 'create_outbreak_alerts_table',
                'description': 'Create outbreak alerts table',
                'up_sql': """
                CREATE TABLE IF NOT EXISTS outbreak_alerts (
                    id SERIAL PRIMARY KEY,
                    alert_id VARCHAR(100) UNIQUE NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    farm_id VARCHAR(50) NOT NULL,
                    severity VARCHAR(20) CHECK (severity IN ('low', 'medium', 'high', 'critical')),
                    anomaly_score DECIMAL(4,3) CHECK (anomaly_score BETWEEN 0 AND 1),
                    sick_count INTEGER NOT NULL CHECK (sick_count >= 0),
                    description TEXT,
                    status VARCHAR(20) DEFAULT 'new' CHECK (status IN ('new', 'investigating', 'confirmed', 'false_positive', 'resolved')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_alerts_farm ON outbreak_alerts(farm_id);
                CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON outbreak_alerts(timestamp);
                CREATE INDEX IF NOT EXISTS idx_alerts_severity ON outbreak_alerts(severity);
                CREATE INDEX IF NOT EXISTS idx_alerts_status ON outbreak_alerts(status);
                
                -- Update trigger for updated_at
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ language 'plpgsql';
                
                CREATE TRIGGER update_outbreak_alerts_updated_at 
                    BEFORE UPDATE ON outbreak_alerts
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                """,
                'down_sql': """
                DROP TRIGGER IF EXISTS update_outbreak_alerts_updated_at ON outbreak_alerts;
                DROP FUNCTION IF EXISTS update_updated_at_column;
                DROP TABLE IF EXISTS outbreak_alerts;
                """
            },
            {
                'name': 'create_environmental_data_table',
                'description': 'Create environmental conditions table',
                'up_sql': """
                CREATE TABLE IF NOT EXISTS environmental_data (
                    id SERIAL PRIMARY KEY,
                    location_id VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    temperature DECIMAL(4,1) CHECK (temperature BETWEEN -30 AND 50),
                    humidity DECIMAL(5,2) CHECK (humidity BETWEEN 0 AND 100),
                    air_quality_index INTEGER CHECK (air_quality_index BETWEEN 0 AND 500),
                    precipitation_mm DECIMAL(6,2) CHECK (precipitation_mm >= 0),
                    wind_speed DECIMAL(6,2) CHECK (wind_speed BETWEEN 0 AND 200),
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(location_id, timestamp)
                );
                
                CREATE INDEX IF NOT EXISTS idx_env_location ON environmental_data(location_id);
                CREATE INDEX IF NOT EXISTS idx_env_timestamp ON environmental_data(timestamp);
                """,
                'down_sql': """
                DROP TABLE IF EXISTS environmental_data;
                """
            },
            {
                'name': 'create_detection_results_table',
                'description': 'Create anomaly detection results table',
                'up_sql': """
                CREATE TABLE IF NOT EXISTS detection_results (
                    id SERIAL PRIMARY KEY,
                    detection_id VARCHAR(100) UNIQUE NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    farm_id VARCHAR(50) NOT NULL,
                    algorithm VARCHAR(50) NOT NULL,
                    anomaly_score DECIMAL(4,3) NOT NULL CHECK (anomaly_score BETWEEN 0 AND 1),
                    confidence DECIMAL(4,3) CHECK (confidence BETWEEN 0 AND 1),
                    features JSONB,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_detection_farm ON detection_results(farm_id);
                CREATE INDEX IF NOT EXISTS idx_detection_timestamp ON detection_results(timestamp);
                CREATE INDEX IF NOT EXISTS idx_detection_algorithm ON detection_results(algorithm);
                CREATE INDEX IF NOT EXISTS idx_detection_score ON detection_results(anomaly_score);
                """,
                'down_sql': """
                DROP TABLE IF EXISTS detection_results;
                """
            }
        ]
        
        for migration in initial_migrations:
            # Create migration file
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            migration_id = f"{timestamp}_{migration['name']}"
            migration_file = self.migrations_dir / f"{migration_id}.sql"
            
            content = f"""-- Migration: {migration['name']}
-- Created: {datetime.now().isoformat()}
-- Description: {migration['description']}

-- UP migration (applying changes)
{migration['up_sql']}

-- DOWN migration (reverting changes)
{migration['down_sql']}
"""
            
            migration_file.write_text(content)
            logger.info(f"Created initial migration: {migration_file.name}")


# Global migration manager instance
_migration_manager: Optional[MigrationManager] = None


def get_migration_manager(db_operations: Optional[DatabaseOperations] = None, 
                         migrations_dir: str = "migrations") -> MigrationManager:
    """Get or create global migration manager"""
    global _migration_manager
    
    if _migration_manager is None:
        if db_operations is None:
            from ..operations import DatabaseOperations
            db_operations = DatabaseOperations()
        
        _migration_manager = MigrationManager(db_operations, migrations_dir)
    
    return _migration_manager