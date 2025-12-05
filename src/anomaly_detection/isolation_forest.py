"""
Isolation Forest anomaly detection for livestock health monitoring
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import Dict, List, Tuple, Optional
import joblib
import os


class IsolationForestDetector:
    """Isolation Forest based anomaly detection"""
    
    def __init__(self, 
                 contamination: float = 0.1,
                 n_estimators: int = 100,
                 max_samples: str = "auto",
                 random_state: int = 42):
        """
        Initialize Isolation Forest detector
        
        Args:
            contamination: Expected proportion of outliers
            n_estimators: Number of base estimators
            max_samples: Number of samples to draw for each base estimator
            random_state: Random seed for reproducibility
        """
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.random_state = random_state
        
        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            max_samples=max_samples,
            random_state=random_state,
            n_jobs=-1  # Use all available cores
        )
        
        self.scaler = StandardScaler()
        self.feature_cols = None
        self.is_fitted = False
    
    def prepare_features(self, df: pd.DataFrame, 
                        feature_cols: List[str]) -> np.ndarray:
        """
        Prepare features for isolation forest
        
        Args:
            df: Input dataframe
            feature_cols: Columns to use as features
            
        Returns:
            Scaled feature matrix
        """
        # Keep only feature columns that exist in dataframe
        existing_cols = [col for col in feature_cols if col in df.columns]
        
        if not existing_cols:
            raise ValueError("No valid feature columns found")
        
        # Extract features
        features = df[existing_cols].values
        
        # Handle missing values
        if np.isnan(features).any():
            # Impute missing values with column means
            col_means = np.nanmean(features, axis=0)
            inds = np.where(np.isnan(features))
            features[inds] = np.take(col_means, inds[1])
        
        # Store feature columns for later use
        self.feature_cols = existing_cols
        
        return features
    
    def fit(self, df: pd.DataFrame, feature_cols: List[str]):
        """
        Fit the isolation forest model
        
        Args:
            df: Training data
            feature_cols: Columns to use as features
        """
        # Prepare features
        features = self.prepare_features(df, feature_cols)
        
        # Scale features
        features_scaled = self.scaler.fit_transform(features)
        
        # Fit model
        self.model.fit(features_scaled)
        self.is_fitted = True
    
    def predict(self, df: pd.DataFrame, 
                feature_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Predict anomalies using isolation forest
        
        Args:
            df: Data to predict
            feature_cols: Feature columns (uses stored if None)
            
        Returns:
            DataFrame with anomaly predictions
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        if feature_cols is None:
            if self.feature_cols is None:
                raise ValueError("Feature columns not specified")
            feature_cols = self.feature_cols
        
        # Prepare features
        features = self.prepare_features(df, feature_cols)
        
        # Scale features
        features_scaled = self.scaler.transform(features)
        
        # Predict anomalies (1 = normal, -1 = anomaly)
        predictions = self.model.predict(features_scaled)
        
        # Get anomaly scores (more negative = more anomalous)
        scores = self.model.decision_function(features_scaled)
        
        # Convert to boolean (True = anomaly)
        anomalies = predictions == -1
        
        # Normalize scores to 0-10 range for consistency
        # Isolation Forest scores: lower = more anomalous
        score_min = scores.min()
        score_max = scores.max()
        
        if score_max > score_min:
            normalized_scores = 10 * (1 - (scores - score_min) / (score_max - score_min))
        else:
            normalized_scores = np.zeros_like(scores)
        
        # Create result dataframe
        result_df = df.copy()
        result_df['if_anomaly'] = anomalies
        result_df['if_anomaly_score'] = normalized_scores
        
        # Add individual feature contributions
        self._add_feature_contributions(result_df, features_scaled)
        
        return result_df
    
    def _add_feature_contributions(self, df: pd.DataFrame, features_scaled: np.ndarray):
        """
        Estimate feature contributions to anomalies
        
        Args:
            df: Result dataframe to update
            features_scaled: Scaled feature matrix
        """
        if self.feature_cols is None:
            return
        
        # Simple heuristic: features far from zero (in standardized space) 
        # contribute more to anomalies
        for i, col in enumerate(self.feature_cols):
            # Absolute deviation from zero in standardized space
            deviation = np.abs(features_scaled[:, i])
            
            # Normalize to 0-1 range
            if deviation.max() > 0:
                contribution = deviation / deviation.max()
            else:
                contribution = np.zeros_like(deviation)
            
            df[f'{col}_if_contribution'] = contribution
    
    def detect_anomalies(self, df: pd.DataFrame, 
                        feature_cols: List[str]) -> pd.DataFrame:
        """
        Detect anomalies (fit and predict in one step for single dataset)
        
        Args:
            df: Data to analyze
            feature_cols: Feature columns
            
        Returns:
            DataFrame with anomaly detections
        """
        # Fit on the data itself (unsupervised)
        self.fit(df, feature_cols)
        return self.predict(df, feature_cols)
    
    def detect_clusters(self, df: pd.DataFrame,
                       time_window: str = '7D',
                       min_cluster_size: int = 3) -> List[Dict]:
        """
        Detect clusters of isolation forest anomalies
        
        Args:
            df: DataFrame with anomaly predictions
            time_window: Time window for clustering
            min_cluster_size: Minimum cluster size
            
        Returns:
            List of cluster dictionaries
        """
        if 'if_anomaly' not in df.columns:
            raise ValueError("DataFrame must contain 'if_anomaly' column")
        
        anomalies = df[df['if_anomaly']].copy()
        
        if len(anomalies) < min_cluster_size:
            return []
        
        # Ensure date column is datetime
        if 'date' in anomalies.columns:
            anomalies['date'] = pd.to_datetime(animals['date'])
        
        # Group by farm and time window
        clusters = []
        
        if 'farm_id' in anomalies.columns:
            anomalies['time_window'] = anomalies['date'].dt.floor(time_window)
            
            for (farm_id, window), group in anomalies.groupby(['farm_id', 'time_window']):
                if len(group) >= min_cluster_size:
                    cluster = {
                        'farm_id': farm_id,
                        'start_date': window,
                        'end_date': window + pd.Timedelta(time_window),
                        'affected_animals': len(group['tag_id'].unique()),
                        'avg_anomaly_score': group['if_anomaly_score'].mean(),
                        'animal_types': group['animal_type'].unique().tolist() 
                                      if 'animal_type' in group.columns else [],
                        'detection_method': 'isolation_forest',
                        'features_contributing': self._get_top_contributing_features(group)
                    }
                    clusters.append(cluster)
        
        return clusters
    
    def _get_top_contributing_features(self, anomaly_group: pd.DataFrame, 
                                      top_n: int = 3) -> List[str]:
        """
        Get top contributing features for anomaly group
        
        Args:
            anomaly_group: Group of anomalies
            top_n: Number of top features to return
            
        Returns:
            List of top contributing feature names
        """
        if self.feature_cols is None:
            return []
        
        # Calculate average contribution for each feature
        feature_contributions = {}
        
        for feature in self.feature_cols:
            contribution_col = f'{feature}_if_contribution'
            if contribution_col in anomaly_group.columns:
                avg_contribution = anomaly_group[contribution_col].mean()
                feature_contributions[feature] = avg_contribution
        
        # Sort by contribution (descending)
        sorted_features = sorted(feature_contributions.items(), 
                                key=lambda x: x[1], 
                                reverse=True)
        
        # Return top N features
        top_features = [feat for feat, _ in sorted_features[:top_n]]
        
        return top_features
    
    def save_model(self, filepath: str):
        """
        Save trained model to disk
        
        Args:
            filepath: Path to save model
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        save_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_cols': self.feature_cols,
            'contamination': self.contamination,
            'n_estimators': self.n_estimators,
            'is_fitted': self.is_fitted
        }
        
        joblib.dump(save_data, filepath)
    
    def load_model(self, filepath: str):
        """
        Load trained model from disk
        
        Args:
            filepath: Path to saved model
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Model file not found: {filepath}")
        
        load_data = joblib.load(filepath)
        
        self.model = load_data['model']
        self.scaler = load_data['scaler']
        self.feature_cols = load_data['feature_cols']
        self.contamination = load_data['contamination']
        self.n_estimators = load_data['n_estimators']
        self.is_fitted = load_data['is_fitted']
    
    def get_feature_importance(self) -> Dict[str, float]:
        """
        Get feature importance from isolation forest
        
        Returns:
            Dictionary of feature importances
        """
        if not self.is_fitted or self.feature_cols is None:
            return {}
        
        # Isolation Forest doesn't have direct feature importance
        # Use average depth as proxy
        try:
            # Get average path lengths
            importances = {}
            
            # This is a simplified approach - in practice you'd need
            # to calculate feature importance from the ensemble
            for i, feature in enumerate(self.feature_cols):
                # Placeholder: equal importance for all features
                importances[feature] = 1.0 / len(self.feature_cols)
            
            return importances
        except:
            # Fallback to equal importance
            return {feature: 1.0/len(self.feature_cols) for feature in self.feature_cols}