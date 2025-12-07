"""
Data quality analyzer for livestock health metrics
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import json
import os


class DataQualityAnalyzer:
    """Analyzes data quality of livestock health metrics"""
    
    def __init__(self):
        """Initialize data quality analyzer"""
        self.required_columns = ['tag_id', 'date', 'animal_type']
        self.numeric_columns = ['temperature', 'heart_rate', 'activity_level']
    
    def analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive data quality analysis
        
        Args:
            df: Input dataframe
            
        Returns:
            Dictionary with quality metrics
        """
        if df.empty:
            return self._empty_analysis_result()
        
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'basic_stats': self._get_basic_stats(df),
            'completeness': self._analyze_completeness(df),
            'validity': self._analyze_validity(df),
            'consistency': self._analyze_consistency(df),
            'timeliness': self._analyze_timeliness(df),
            'quality_score': 0.0,
            'issues': [],
            'recommendations': []
        }
        
        # Calculate overall quality score
        analysis['quality_score'] = self._calculate_quality_score(analysis)
        
        # Generate issues and recommendations
        analysis['issues'] = self._identify_issues(analysis)
        analysis['recommendations'] = self._generate_recommendations(analysis)
        
        return analysis
    
    def _empty_analysis_result(self) -> Dict[str, Any]:
        """Return empty analysis result"""
        return {
            'timestamp': datetime.now().isoformat(),
            'basic_stats': {'total_records': 0, 'unique_animals': 0},
            'completeness': {'overall': 0.0, 'by_column': {}},
            'validity': {'overall': 0.0, 'by_column': {}},
            'consistency': {'overall': 0.0},
            'timeliness': {'overall': 0.0},
            'quality_score': 0.0,
            'issues': ['No data available for analysis'],
            'recommendations': ['Collect more data']
        }
    
    def _get_basic_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic statistics about the data"""
        stats = {
            'total_records': len(df),
            'unique_animals': df['tag_id'].nunique() if 'tag_id' in df.columns else 0,
            'date_range': {},
            'animal_distribution': {}
        }
        
        # Date range
        if 'date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['date'])
                stats['date_range'] = {
                    'start': df['date'].min().strftime('%Y-%m-%d'),
                    'end': df['date'].max().strftime('%Y-%m-%d'),
                    'days': (df['date'].max() - df['date'].min()).days + 1
                }
            except:
                stats['date_range'] = {'error': 'Invalid date format'}
        
        # Animal type distribution
        if 'animal_type' in df.columns:
            stats['animal_distribution'] = df['animal_type'].value_counts().to_dict()
        
        # Record frequency
        if 'date' in df.columns and 'tag_id' in df.columns:
            try:
                records_per_animal = df.groupby('tag_id').size()
                stats['records_per_animal'] = {
                    'min': int(records_per_animal.min()),
                    'max': int(records_per_animal.max()),
                    'mean': float(records_per_animal.mean()),
                    'median': float(records_per_animal.median())
                }
            except:
                pass
        
        return stats
    
    def _analyze_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data completeness (missing values)"""
        completeness = {
            'overall': 0.0,
            'by_column': {},
            'missing_counts': {},
            'missing_percentages': {}
        }
        
        if df.empty:
            return completeness
        
        total_cells = len(df) * len(df.columns)
        non_missing_cells = df.notna().sum().sum()
        
        completeness['overall'] = (non_missing_cells / total_cells * 100) if total_cells > 0 else 0
        
        # Analyze by column
        for column in df.columns:
            non_missing = df[column].notna().sum()
            missing = len(df) - non_missing
            percent_missing = (missing / len(df)) * 100
            
            completeness['by_column'][column] = 100 - percent_missing
            completeness['missing_counts'][column] = int(missing)
            completeness['missing_percentages'][column] = float(percent_missing)
            
            # Critical columns check
            if column in self.required_columns:
                completeness[f'critical_{column}_missing'] = percent_missing
        
        return completeness
    
    def _analyze_validity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data validity (values within expected ranges)"""
        validity = {
            'overall': 0.0,
            'by_column': {},
            'invalid_counts': {},
            'invalid_percentages': {}
        }
        
        if df.empty:
            return validity
        
        # Define valid ranges for numeric columns
        valid_ranges = {
            'temperature': (35.0, 42.0),  # Celsius
            'heart_rate': (40, 120),      # BPM
            'activity_level': (0.1, 2.0)  # Relative
        }
        
        valid_cells = 0
        total_cells = 0
        
        for column in df.columns:
            if column in valid_ranges and column in df.columns:
                min_val, max_val = valid_ranges[column]
                
                # Count valid values (non-null and within range)
                valid_mask = df[column].notna() & (df[column] >= min_val) & (df[column] <= max_val)
                valid_count = valid_mask.sum()
                invalid_count = len(df) - valid_count
                
                valid_cells += valid_count
                total_cells += len(df)
                
                percent_valid = (valid_count / len(df)) * 100 if len(df) > 0 else 0
                
                validity['by_column'][column] = percent_valid
                validity['invalid_counts'][column] = int(invalid_count)
                validity['invalid_percentages'][column] = 100 - percent_valid
            else:
                # For non-numeric or columns without defined ranges, consider all non-null values valid
                valid_count = df[column].notna().sum()
                valid_cells += valid_count
                total_cells += len(df)
                
                percent_valid = (valid_count / len(df)) * 100 if len(df) > 0 else 0
                validity['by_column'][column] = percent_valid
        
        validity['overall'] = (valid_cells / total_cells * 100) if total_cells > 0 else 0
        
        return validity
    
    def _analyze_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data consistency (duplicates, logical consistency)"""
        consistency = {
            'overall': 0.0,
            'duplicates': {},
            'inconsistencies': []
        }
        
        if df.empty:
            return consistency
        
        scores = []
        
        # Check for duplicate records
        if 'tag_id' in df.columns and 'date' in df.columns:
            duplicate_mask = df.duplicated(subset=['tag_id', 'date'], keep=False)
            duplicate_count = duplicate_mask.sum()
            duplicate_percent = (duplicate_count / len(df)) * 100
            
            consistency['duplicates'] = {
                'count': int(duplicate_count),
                'percent': float(duplicate_percent)
            }
            
            # Score for duplicates (lower duplicates = higher score)
            duplicate_score = max(0, 100 - duplicate_percent * 2)  # Penalize duplicates heavily
            scores.append(duplicate_score)
        
        # Check for logical inconsistencies
        inconsistencies = []
        
        # Animal type consistency (same animal should have same type)
        if 'tag_id' in df.columns and 'animal_type' in df.columns:
            animal_type_consistency = df.groupby('tag_id')['animal_type'].nunique()
            inconsistent_animals = animal_type_consistency[animal_type_consistency > 1]
            
            if len(inconsistent_animals) > 0:
                inconsistency = {
                    'type': 'animal_type_inconsistency',
                    'message': f'{len(inconsistent_animals)} animals have multiple types assigned',
                    'count': int(len(inconsistent_animals))
                }
                inconsistencies.append(inconsistency)
        
        # Date consistency (dates should be in chronological order per animal)
        if 'tag_id' in df.columns and 'date' in df.columns:
            try:
                df_sorted = df.copy()
                df_sorted['date'] = pd.to_datetime(df_sorted['date'])
                
                date_issues = 0
                for animal_id, group in df_sorted.groupby('tag_id'):
                    if len(group) > 1:
                        # Check if dates are sorted
                        if not group['date'].is_monotonic_increasing:
                            date_issues += 1
                
                if date_issues > 0:
                    inconsistency = {
                        'type': 'date_ordering',
                        'message': f'{date_issues} animals have non-chronological dates',
                        'count': date_issues
                    }
                    inconsistencies.append(inconsistency)
            except:
                pass
        
        consistency['inconsistencies'] = inconsistencies
        
        # Calculate overall consistency score
        if scores:
            consistency['overall'] = np.mean(scores)
        else:
            consistency['overall'] = 100.0  # Perfect if no checks applicable
        
        return consistency
    
    def _analyze_timeliness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data timeliness (how recent is the data)"""
        timeliness = {
            'overall': 0.0,
            'data_age': {},
            'update_frequency': {}
        }
        
        if df.empty or 'date' not in df.columns:
            return timeliness
        
        try:
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate data age
            latest_date = df['date'].max()
            today = pd.Timestamp.now()
            days_old = (today - latest_date).days
            
            timeliness['data_age'] = {
                'latest_date': latest_date.strftime('%Y-%m-%d'),
                'days_old': int(days_old),
                'is_recent': days_old <= 7  # Consider data recent if within 7 days
            }
            
            # Calculate update frequency
            if 'tag_id' in df.columns:
                update_stats = []
                for animal_id, group in df.groupby('tag_id'):
                    if len(group) > 1:
                        group = group.sort_values('date')
                        time_diffs = group['date'].diff().dt.days.dropna()
                        if len(time_diffs) > 0:
                            avg_frequency = time_diffs.mean()
                            update_stats.append(avg_frequency)
                
                if update_stats:
                    timeliness['update_frequency'] = {
                        'mean_days_between': float(np.mean(update_stats)),
                        'median_days_between': float(np.median(update_stats)),
                        'consistent': np.mean(update_stats) <= 3  # Consistent if updates <= 3 days apart
                    }
            
            # Calculate timeliness score
            # Recent data (<= 7 days old) gets 100%, older data gets lower score
            if days_old <= 7:
                recency_score = 100.0
            elif days_old <= 30:
                recency_score = 70.0
            elif days_old <= 90:
                recency_score = 40.0
            else:
                recency_score = 10.0
            
            # Frequency score
            if 'update_frequency' in timeliness and timeliness['update_frequency']:
                mean_freq = timeliness['update_frequency']['mean_days_between']
                if mean_freq <= 1:
                    frequency_score = 100.0
                elif mean_freq <= 3:
                    frequency_score = 80.0
                elif mean_freq <= 7:
                    frequency_score = 60.0
                else:
                    frequency_score = 30.0
            else:
                frequency_score = 50.0  # Default if can't calculate
            
            timeliness['overall'] = (recency_score * 0.7) + (frequency_score * 0.3)
            
        except Exception as e:
            timeliness['error'] = str(e)
            timeliness['overall'] = 50.0  # Default score if analysis fails
        
        return timeliness
    
    def _calculate_quality_score(self, analysis: Dict[str, Any]) -> float:
        """Calculate overall data quality score"""
        weights = {
            'completeness': 0.30,
            'validity': 0.30,
            'consistency': 0.20,
            'timeliness': 0.20
        }
        
        score = 0.0
        
        for dimension, weight in weights.items():
            if dimension in analysis:
                dimension_score = analysis[dimension].get('overall', 0.0)
                score += dimension_score * weight
        
        return min(100.0, max(0.0, score))  # Clamp between 0-100
    
    def _identify_issues(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify data quality issues"""
        issues = []
        
        # Check completeness issues
        completeness = analysis.get('completeness', {})
        if completeness.get('overall', 100) < 90:
            issues.append({
                'type': 'completeness',
                'severity': 'high' if completeness['overall'] < 70 else 'medium',
                'message': f'Data completeness is {completeness["overall"]:.1f}%',
                'details': completeness
            })
        
        # Check for missing critical columns
        for column in self.required_columns:
            missing_key = f'critical_{column}_missing'
            if missing_key in completeness and completeness[missing_key] > 10:
                issues.append({
                    'type': 'missing_critical_column',
                    'severity': 'high',
                    'message': f'Critical column "{column}" has {completeness[missing_key]:.1f}% missing values',
                    'column': column
                })
        
        # Check validity issues
        validity = analysis.get('validity', {})
        if validity.get('overall', 100) < 90:
            issues.append({
                'type': 'validity',
                'severity': 'high' if validity['overall'] < 70 else 'medium',
                'message': f'Data validity is {validity["overall"]:.1f}%',
                'details': validity
            })
        
        # Check consistency issues
        consistency = analysis.get('consistency', {})
        duplicates = consistency.get('duplicates', {})
        if duplicates.get('percent', 0) > 5:
            issues.append({
                'type': 'duplicates',
                'severity': 'medium' if duplicates['percent'] < 20 else 'high',
                'message': f'{duplicates["percent"]:.1f}% of records are duplicates',
                'count': duplicates['count']
            })
        
        # Check timeliness issues
        timeliness = analysis.get('timeliness', {})
        data_age = timeliness.get('data_age', {})
        if data_age.get('days_old', 0) > 30:
            issues.append({
                'type': 'stale_data',
                'severity': 'high' if data_age['days_old'] > 90 else 'medium',
                'message': f'Data is {data_age["days_old"]} days old',
                'latest_date': data_age.get('latest_date', 'unknown')
            })
        
        return issues
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations based on quality issues"""
        recommendations = []
        issues = analysis.get('issues', [])
        
        # Map issue types to recommendations
        recommendation_map = {
            'completeness': 'Add missing values or collect more complete data',
            'missing_critical_column': 'Ensure all required columns are properly populated',
            'validity': 'Review data collection procedures and validate sensor readings',
            'duplicates': 'Remove or merge duplicate records',
            'stale_data': 'Collect more recent data for accurate analysis'
        }
        
        for issue in issues:
            issue_type = issue['type']
            if issue_type in recommendation_map:
                recommendations.append({
                    'issue': issue_type,
                    'priority': issue.get('severity', 'medium'),
                    'recommendation': recommendation_map[issue_type],
                    'details': issue.get('message', '')
                })
        
        # Add general recommendations based on quality score
        quality_score = analysis.get('quality_score', 0)
        
        if quality_score >= 90:
            recommendations.append({
                'issue': 'general',
                'priority': 'low',
                'recommendation': 'Data quality is excellent. Continue current practices.',
                'details': f'Quality score: {quality_score:.1f}%'
            })
        elif quality_score >= 70:
            recommendations.append({
                'issue': 'general',
                'priority': 'medium',
                'recommendation': 'Data quality is good but could be improved.',
                'details': f'Quality score: {quality_score:.1f}%'
            })
        else:
            recommendations.append({
                'issue': 'general',
                'priority': 'high',
                'recommendation': 'Data quality needs significant improvement before analysis.',
                'details': f'Quality score: {quality_score:.1f}%'
            })
        
        # Deduplicate recommendations
        unique_recs = []
        seen = set()
        for rec in recommendations:
            key = (rec['issue'], rec['recommendation'])
            if key not in seen:
                unique_recs.append(rec)
                seen.add(key)
        
        return unique_recs
    
    def generate_quality_report(self, analysis: Dict[str, Any], 
                               output_format: str = 'text') -> str:
        """
        Generate human-readable quality report
        
        Args:
            analysis: Quality analysis results
            output_format: 'text' or 'html'
            
        Returns:
            Formatted report string
        """
        if output_format == 'text':
            return self._generate_text_report(analysis)
        elif output_format == 'html':
            return self._generate_html_report(analysis)
        else:
            raise ValueError(f"Unsupported format: {output_format}")
    
    def _generate_text_report(self, analysis: Dict[str, Any]) -> str:
        """Generate text quality report"""
        lines = []
        
        lines.append("=" * 60)
        lines.append("DATA QUALITY ANALYSIS REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # Basic stats
        basic_stats = analysis.get('basic_stats', {})
        lines.append("BASIC STATISTICS")
        lines.append("-" * 40)
        lines.append(f"Total Records: {basic_stats.get('total_records', 0)}")
        lines.append(f"Unique Animals: {basic_stats.get('unique_animals', 0)}")
        
        date_range = basic_stats.get('date_range', {})
        if 'start' in date_range:
            lines.append(f"Date Range: {date_range['start']} to {date_range['end']}")
        
        lines.append("")
        
        # Quality score
        quality_score = analysis.get('quality_score', 0)
        lines.append("OVERALL QUALITY SCORE")
        lines.append("-" * 40)
        
        # Visual indicator
        if quality_score >= 90:
            indicator = "★★★★★ EXCELLENT"
        elif quality_score >= 70:
            indicator = "★★★★☆ GOOD"
        elif quality_score >= 50:
            indicator = "★★★☆☆ FAIR"
        elif quality_score >= 30:
            indicator = "★★☆☆☆ POOR"
        else:
            indicator = "★☆☆☆☆ VERY POOR"
        
        lines.append(f"{indicator}")
        lines.append(f"Score: {quality_score:.1f}%")
        lines.append("")
        
        # Dimension scores
        lines.append("DIMENSION SCORES")
        lines.append("-" * 40)
        
        dimensions = ['completeness', 'validity', 'consistency', 'timeliness']
        for dim in dimensions:
            if dim in analysis:
                score = analysis[dim].get('overall', 0)
                lines.append(f"{dim.title():12s}: {score:6.1f}%")
        
        lines.append("")
        
        # Issues
        issues = analysis.get('issues', [])
        if issues:
            lines.append("ISSUES DETECTED")
            lines.append("-" * 40)
            
            for i, issue in enumerate(issues, 1):
                severity = issue.get('severity', 'unknown').upper()
                lines.append(f"{i}. [{severity}] {issue.get('message', '')}")
        
        lines.append("")
        
        # Recommendations
        recommendations = analysis.get('recommendations', [])
        if recommendations:
            lines.append("RECOMMENDATIONS")
            lines.append("-" * 40)
            
            # Sort by priority
            priority_order = {'high': 0, 'medium': 1, 'low': 2}
            sorted_recs = sorted(recommendations, 
                               key=lambda x: priority_order.get(x.get('priority', 'low'), 2))
            
            for i, rec in enumerate(sorted_recs, 1):
                priority = rec.get('priority', 'medium').upper()
                lines.append(f"{i}. [{priority}] {rec.get('recommendation', '')}")
        
        lines.append("")
        lines.append("=" * 60)
        lines.append("End of Report")
        
        return "\n".join(lines)
    
    def _generate_html_report(self, analysis: Dict[str, Any]) -> str:
        """Generate HTML quality report"""
        quality_score = analysis.get('quality_score', 0)
        
        # Determine quality color
        if quality_score >= 90:
            quality_color = "#28a745"
            quality_text = "Excellent"
        elif quality_score >= 70:
            quality_color = "#ffc107"
            quality_text = "Good"
        elif quality_score >= 50:
            quality_color = "#fd7e14"
            quality_text = "Fair"
        elif quality_score >= 30:
            quality_color = "#dc3545"
            quality_text = "Poor"
        else:
            quality_color = "#6c757d"
            quality_text = "Very Poor"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; }}
                .score-card {{ 
                    text-align: center; 
                    padding: 20px; 
                    margin: 20px 0;
                    border-radius: 10px;
                    background-color: {quality_color}20;
                    border-left: 5px solid {quality_color};
                }}
                .score-number {{ 
                    font-size: 48px; 
                    font-weight: bold;
                    color: {quality_color};
                }}
                .dimension-card {{ 
                    border: 1px solid #dee2e6; 
                    border-radius: 5px; 
                    padding: 15px; 
                    margin: 10px 0;
                }}
                .issue-high {{ border-left: 5px solid #dc3545; }}
                .issue-medium {{ border-left: 5px solid #ffc107; }}
                .issue-low {{ border-left: 5px solid #28a745; }}
                .progress {{ 
                    height: 20px; 
                    background-color: #e9ecef; 
                    border-radius: 10px;
                    margin: 10px 0;
                }}
                .progress-bar {{ 
                    height: 100%; 
                    border-radius: 10px;
                    background-color: {quality_color};
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Data Quality Analysis Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="score-card">
                <h2>Overall Data Quality Score</h2>
                <div class="score-number">{quality_score:.1f}%</div>
                <p><strong>{quality_text}</strong></p>
            </div>
            
            <h2>📈 Dimension Scores</h2>
        """
        
        # Add dimension scores
        dimensions = ['completeness', 'validity', 'consistency', 'timeliness']
        for dim in dimensions:
            if dim in analysis:
                score = analysis[dim].get('overall', 0)
                html += f"""
                <div class="dimension-card">
                    <h3>{dim.title()}</h3>
                    <p>{score:.1f}%</p>
                    <div class="progress">
                        <div class="progress-bar" style="width: {score}%"></div>
                    </div>
                </div>
                """
        
        # Add issues
        issues = analysis.get('issues', [])
        if issues:
            html += """
            <h2>⚠️ Issues Detected</h2>
            """
            
            for issue in issues:
                severity = issue.get('severity', 'medium')
                severity_class = f'issue-{severity}'
                html += f"""
                <div class="dimension-card {severity_class}">
                    <strong>{issue.get('type', 'Unknown').replace('_', ' ').title()}</strong>
                    <p>{issue.get('message', '')}</p>
                </div>
                """
        
        # Add recommendations
        recommendations = analysis.get('recommendations', [])
        if recommendations:
            html += """
            <h2>💡 Recommendations</h2>
            """
            
            for rec in recommendations:
                priority = rec.get('priority', 'medium')
                priority_class = f'issue-{priority}'
                html += f"""
                <div class="dimension-card {priority_class}">
                    <strong>{rec.get('recommendation', '')}</strong>
                    <p>Priority: {priority.title()}</p>
                </div>
                """
        
        # Add basic stats
        basic_stats = analysis.get('basic_stats', {})
        html += f"""
        <h2>📊 Basic Statistics</h2>
        <div class="dimension-card">
            <p><strong>Total Records:</strong> {basic_stats.get('total_records', 0)}</p>
            <p><strong>Unique Animals:</strong> {basic_stats.get('unique_animals', 0)}</p>
        """
        
        if 'date_range' in basic_stats and 'start' in basic_stats['date_range']:
            html += f"""
            <p><strong>Date Range:</strong> {basic_stats['date_range']['start']} to {basic_stats['date_range']['end']}</p>
            """
        
        html += """
        </div>
        
        <div style="margin-top: 30px; padding: 15px; background-color: #f8f9fa; border-radius: 5px;">
            <p><em>Generated by Livestock Outbreak Detection System - Data Quality Module</em></p>
        </div>
        
        </body>
        </html>
        """
        
        return html
    
    def save_analysis(self, analysis: Dict[str, Any], filepath: str):
        """
        Save analysis results to file
        
        Args:
            analysis: Analysis results
            filepath: Path to save file
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, default=str)
    
    def load_analysis(self, filepath: str) -> Dict[str, Any]:
        """
        Load analysis results from file
        
        Args:
            filepath: Path to analysis file
            
        Returns:
            Analysis results
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)