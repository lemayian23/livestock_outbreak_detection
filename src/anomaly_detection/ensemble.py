"""
Ensemble anomaly detection combining multiple methods
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from .statistical import StatisticalDetector
from .isolation_forest import IsolationForestDetector


class EnsembleDetector:
    """
    Ensemble detector combining statistical and isolation forest methods
    """
    
    def __init__(self, 
                 statistical_config: Dict = None,
                 isolation_forest_config: Dict = None,
                 weights: Dict = None):
        """
        Initialize ensemble detector
        
        Args:
            statistical_config: Configuration for statistical detector
            isolation_forest_config: Configuration for isolation forest detector
            weights: Weights for each method {'statistical': 0.4, 'isolation_forest': 0.6}
        """
        # Default configurations
        if statistical_config is None:
            statistical_config = {
                'window_size': 7,
                'threshold': 3.0
            }
        
        if isolation_forest_config is None:
            isolation_forest_config = {
                'contamination': 0.1,
                'n_estimators': 100,
                'random_state': 42
            }
        
        if weights is None:
            weights = {'statistical': 0.4, 'isolation_forest': 0.6}
        
        # Initialize detectors
        self.statistical_detector = StatisticalDetector(**statistical_config)
        self.isolation_forest_detector = IsolationForestDetector(**isolation_forest_config)
        
        self.weights = weights
        self.feature_cols = None
    
    def detect_anomalies(self, df: pd.DataFrame, 
                        metric_cols: List[str]) -> pd.DataFrame:
        """
        Detect anomalies using ensemble method
        
        Args:
            df: Input dataframe
            metric_cols: Metric columns to monitor
            
        Returns:
            DataFrame with ensemble anomaly predictions
        """
        result_df = df.copy()
        
        # 1. Run statistical detection
        statistical_result = self.statistical_detector.detect_anomalies(df, metric_cols)
        
        # 2. Run isolation forest detection
        # Prepare features for isolation forest
        self.feature_cols = [col for col in metric_cols if col in df.columns]
        if_ready_df = self._prepare_for_isolation_forest(df)
        isolation_result = self.isolation_forest_detector.detect_anomalies(
            if_ready_df, 
            self.feature_cols
        )
        
        # 3. Combine results
        # Get statistical anomaly scores (max of z-scores)
        if 'anomaly_score' in statistical_result.columns:
            stat_scores = statistical_result['anomaly_score']
        else:
            # Calculate from individual z-scores
            zscore_cols = [f'{col}_zscore' for col in metric_cols 
                          if f'{col}_zscore' in statistical_result.columns]
            if zscore_cols:
                stat_scores = statistical_result[zscore_cols].abs().max(axis=1)
            else:
                stat_scores = pd.Series(0, index=statistical_result.index)
        
        # Get isolation forest scores (already normalized to 0-10)
        if 'if_anomaly_score' in isolation_result.columns:
            if_scores = isolation_result['if_anomaly_score']
        else:
            if_scores = pd.Series(0, index=isolation_result.index)
        
        # Normalize statistical scores to 0-10 range for consistency
        if stat_scores.max() > 0:
            stat_scores_norm = 10 * stat_scores / stat_scores.max()
        else:
            stat_scores_norm = pd.Series(0, index=stat_scores.index)
        
        # 4. Calculate ensemble score
        ensemble_scores = (
            self.weights['statistical'] * stat_scores_norm +
            self.weights['isolation_forest'] * if_scores
        )
        
        # 5. Determine anomalies based on combined score
        # Threshold at 5.0 (midpoint of 0-10 range)
        anomaly_threshold = 5.0
        ensemble_anomalies = ensemble_scores >= anomaly_threshold
        
        # 6. Add all results to dataframe
        # Statistical results
        for col in ['is_anomaly', 'anomaly_score']:
            if col in statistical_result.columns:
                result_df[f'stat_{col}'] = statistical_result[col]
        
        # Isolation forest results
        for col in ['if_anomaly', 'if_anomaly_score']:
            if col in isolation_result.columns:
                result_df[col] = isolation_result[col]
        
        # Add individual method z-scores and contributions
        for col in metric_cols:
            zscore_col = f'{col}_zscore'
            if zscore_col in statistical_result.columns:
                result_df[zscore_col] = statistical_result[zscore_col]
            
            contrib_col = f'{col}_if_contribution'
            if contrib_col in isolation_result.columns:
                result_df[contrib_col] = isolation_result[contrib_col]
        
        # Ensemble results
        result_df['ensemble_anomaly_score'] = ensemble_scores
        result_df['ensemble_anomaly'] = ensemble_anomalies
        
        # Combined anomaly flag (any method detects anomaly)
        anomaly_cols = ['stat_is_anomaly', 'if_anomaly', 'ensemble_anomaly']
        existing_anomaly_cols = [col for col in anomaly_cols if col in result_df.columns]
        
        if existing_anomaly_cols:
            result_df['is_anomaly'] = result_df[existing_anomaly_cols].any(axis=1)
            result_df['anomaly_score'] = result_df[existing_anomaly_cols + ['ensemble_anomaly_score']].max(axis=1)
        else:
            result_df['is_anomaly'] = ensemble_anomalies
            result_df['anomaly_score'] = ensemble_scores
        
        # Add detection method summary
        result_df['detection_method'] = self._get_detection_method_summary(result_df)
        
        return result_df
    
    def _prepare_for_isolation_forest(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare dataframe for isolation forest
        
        Args:
            df: Input dataframe
            
        Returns:
            Prepared dataframe
        """
        prepared_df = df.copy()
        
        # Ensure numeric columns for isolation forest
        if self.feature_cols:
            for col in self.feature_cols:
                if col in prepared_df.columns:
                    prepared_df[col] = pd.to_numeric(prepared_df[col], errors='coerce')
        
        # Fill NaN values (isolation forest doesn't handle NaN)
        if self.feature_cols:
            for col in self.feature_cols:
                if col in prepared_df.columns:
                    prepared_df[col] = prepared_df[col].fillna(prepared_df[col].mean())
        
        return prepared_df
    
    def _get_detection_method_summary(self, df: pd.DataFrame) -> pd.Series:
        """
        Create summary of which methods detected each anomaly
        
        Args:
            df: DataFrame with anomaly results
            
        Returns:
            Series with method summaries
        """
        methods = []
        
        for idx, row in df.iterrows():
            detected_by = []
            
            if row.get('stat_is_anomaly', False):
                detected_by.append('statistical')
            
            if row.get('if_anomaly', False):
                detected_by.append('isolation_forest')
            
            if row.get('ensemble_anomaly', False):
                detected_by.append('ensemble')
            
            if detected_by:
                methods.append('+'.join(detected_by))
            else:
                methods.append('none')
        
        return pd.Series(methods, index=df.index)
    
    def detect_clusters(self, df: pd.DataFrame,
                       time_window: str = '7D',
                       min_cluster_size: int = 3) -> List[Dict]:
        """
        Detect clusters using ensemble approach
        
        Args:
            df: DataFrame with anomaly predictions
            time_window: Time window for clustering
            min_cluster_size: Minimum cluster size
            
        Returns:
            List of cluster dictionaries
        """
        clusters = []
        
        # Get clusters from both methods
        stat_clusters = self.statistical_detector.detect_clusters(
            df, time_window, min_cluster_size
        )
        
        if_clusters = self.isolation_forest_detector.detect_clusters(
            df, time_window, min_cluster_size
        )
        
        # Combine and deduplicate clusters
        all_clusters = stat_clusters + if_clusters
        
        if not all_clusters:
            return []
        
        # Group clusters by farm and time window
        cluster_groups = {}
        
        for cluster in all_clusters:
            key = (cluster['farm_id'], 
                   cluster['start_date'].strftime('%Y-%m-%d'),
                   cluster['end_date'].strftime('%Y-%m-%d'))
            
            if key not in cluster_groups:
                cluster_groups[key] = {
                    'farm_id': cluster['farm_id'],
                    'start_date': cluster['start_date'],
                    'end_date': cluster['end_date'],
                    'affected_animals': set(),
                    'animal_types': set(),
                    'detection_methods': set(),
                    'scores': [],
                    'features_contributing': []
                }
            
            # Add affected animals
            if 'affected_animals' in cluster:
                # Convert to set of animal IDs if available
                # For now, just track count
                cluster_groups[key]['affected_animals'].add(cluster['affected_animals'])
            
            # Add animal types
            if 'animal_types' in cluster:
                cluster_groups[key]['animal_types'].update(cluster['animal_types'])
            
            # Add detection methods
            if 'detection_method' in cluster:
                cluster_groups[key]['detection_methods'].add(cluster['detection_method'])
            else:
                cluster_groups[key]['detection_methods'].add('statistical')
            
            # Add scores
            if 'avg_anomaly_score' in cluster:
                cluster_groups[key]['scores'].append(cluster['avg_anomaly_score'])
            
            # Add contributing features
            if 'features_contributing' in cluster:
                cluster_groups[key]['features_contributing'].extend(
                    cluster['features_contributing']
                )
        
        # Create final clusters
        for key, group_data in cluster_groups.items():
            # Calculate combined affected animals (max of methods)
            affected_animals = max(group_data['affected_animals']) if group_data['affected_animals'] else 0
            
            # Only include if meets minimum size
            if affected_animals >= min_cluster_size:
                cluster = {
                    'farm_id': group_data['farm_id'],
                    'start_date': group_data['start_date'],
                    'end_date': group_data['end_date'],
                    'affected_animals': affected_animals,
                    'animal_types': list(group_data['animal_types']),
                    'detection_methods': list(group_data['detection_methods']),
                    'avg_anomaly_score': np.mean(group_data['scores']) if group_data['scores'] else 0,
                    'ensemble_score': self._calculate_ensemble_cluster_score(group_data),
                    'severity': self._determine_cluster_severity(affected_animals, 
                                                               group_data['scores'])
                }
                
                # Add contributing features (unique)
                if group_data['features_contributing']:
                    unique_features = list(set(group_data['features_contributing']))
                    cluster['features_contributing'] = unique_features[:5]  # Top 5
                
                clusters.append(cluster)
        
        return clusters
    
    def _calculate_ensemble_cluster_score(self, group_data: Dict) -> float:
        """
        Calculate ensemble score for cluster
        
        Args:
            group_data: Group data dictionary
            
        Returns:
            Ensemble score
        """
        if not group_data['scores']:
            return 0.0
        
        scores = np.array(group_data['scores'])
        
        # Weight by number of detection methods that found it
        method_weight = len(group_data['detection_methods']) / 2.0  # Max 2 methods
        
        # Average score weighted by method weight
        ensemble_score = scores.mean() * method_weight
        
        return min(10.0, ensemble_score)  # Cap at 10
    
    def _determine_cluster_severity(self, affected_animals: int, 
                                   scores: List[float]) -> str:
        """
        Determine severity level for cluster
        
        Args:
            affected_animals: Number of affected animals
            scores: List of anomaly scores
            
        Returns:
            Severity level (low, medium, high, critical)
        """
        avg_score = np.mean(scores) if scores else 0
        
        if affected_animals >= 10 or avg_score >= 7.0:
            return 'critical'
        elif affected_animals >= 5 or avg_score >= 5.0:
            return 'high'
        elif affected_animals >= 3 or avg_score >= 3.0:
            return 'medium'
        else:
            return 'low'
    
    def save_models(self, base_path: str):
        """
        Save all models to disk
        
        Args:
            base_path: Base path for saving models
        """
        import os
        os.makedirs(base_path, exist_ok=True)
        
        # Save statistical detector state (if needed)
        # Statistical detector is stateless, so we just save config
        
        # Save isolation forest model
        isolation_path = os.path.join(base_path, 'isolation_forest_model.joblib')
        self.isolation_forest_detector.save_model(isolation_path)
    
    def load_models(self, base_path: str):
        """
        Load models from disk
        
        Args:
            base_path: Base path containing saved models
        """
        # Load isolation forest model
        isolation_path = os.path.join(base_path, 'isolation_forest_model.joblib')
        if os.path.exists(isolation_path):
            self.isolation_forest_detector.load_model(isolation_path)