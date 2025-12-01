from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class Livestock(Base):
    """Livestock animal record"""
    __tablename__ = 'livestock'
    
    id = Column(Integer, primary_key=True)
    tag_id = Column(String, unique=True, nullable=False)
    animal_type = Column(String, nullable=False)  # cattle, sheep, goat
    age_months = Column(Integer)
    farm_id = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    is_active = Column(Boolean, default=True)

class HealthMetric(Base):
    """Daily health metrics for each animal"""
    __tablename__ = 'health_metrics'
    
    id = Column(Integer, primary_key=True)
    tag_id = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    
    # Health metrics
    temperature = Column(Float)  # Celsius
    heart_rate = Column(Float)   # BPM
    activity_level = Column(Float)  # Relative to baseline
    feed_intake = Column(Float)  # kg/day
    water_intake = Column(Float)  # liters/day
    
    # Calculated fields
    is_anomaly = Column(Boolean, default=False)
    anomaly_score = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.now)
    
    # Index for faster queries
    __table_args__ = (
        {'sqlite_autoincrement': True},
    )

class OutbreakAlert(Base):
    """Generated outbreak alerts"""
    __tablename__ = 'outbreak_alerts'
    
    id = Column(Integer, primary_key=True)
    farm_id = Column(String)
    alert_type = Column(String)  # individual, cluster, outbreak
    severity = Column(String)  # low, medium, high, critical
    description = Column(String)
    affected_animals = Column(Integer, default=1)
    start_date = Column(DateTime)
    end_date = Column(DateTime, nullable=True)
    is_resolved = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)

class DatabaseManager:
    def __init__(self, db_path: str = './data/livestock.db'):
        self.engine = create_engine(f'sqlite:///{db_path}')
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
    
    def get_session(self):
        return self.Session()