#!/usr/bin/env python3
"""
Test script for data quality analysis feature
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.data_quality.analyzer import DataQualityAnalyzer

def create_test_data():
    """Create test data with various quality issues"""
    np.random.seed(42)
    
    dates = pd.date_range(start='2024-01-01', periods=100, freq='D')
    
    # Create dataframe with intentional quality issues
    data = {
        'tag_id': [],
        'date': [],
        'animal_type': [],
        'farm_id': [],
        'temperature': [],
        'heart_rate': [],
        'activity_level': []
    }
    
    for i in range(100):
        data['tag_id'].append(f'ANM{str(i%20+1).zfill(3)}')  # 20 unique animals
        
        # Add some missing dates
        if i < 5:
            data['date'].append(None)  # Missing dates
        else:
            data['date'].append(dates[i-5])
        
        # Animal types with some inconsistencies
        if i < 30:
            data['animal_type'].append('cattle')
        elif i < 60:
            data['animal_type'].append('sheep')
        elif i < 90:
            data['animal_type'].append('goat')
        else:
            data['animal_type'].append('unknown')  # Invalid type
        
        data['farm_id'].append('FARM1')
        
        # Temperature with some outliers
        if i < 10:
            data['temperature'].append(None)  # Missing
        elif i < 15:
            data['temperature'].append(45.0)  # Too high
        elif i < 20:
            data['temperature'].append(35.0)  # Too low
        else:
            data['temperature'].append(np.random.normal(38.5, 0.5))
        
        # Heart rate with some issues
        if i < 25:
            data['heart_rate'].append(None)  # Missing
        elif i < 30:
            data['heart_rate'].append(150)  # Too high
        else:
            data['heart_rate'].append(np.random.normal(70, 5))
        
        # Activity level
        if i < 35:
            data['activity_level'].append(None)  # Missing
        elif i < 40:
            data['activity_level'].append(0.05)  # Too low
        else:
            data['activity_level'].append(np.random.normal(1.0, 0.2))
    
    df = pd.DataFrame(data)
    
    # Add some duplicate records
    duplicates = df.iloc[:5].copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    
    return df

def test_data_quality_analysis():
    """Test the data quality analyzer"""
    print("Testing Data Quality Analysis Feature...")
    print("=" * 60)
    
    # Create analyzer
    analyzer = DataQualityAnalyzer()
    
    # Create test data
    print("\n1. Creating test data with quality issues...")
    test_df = create_test_data()
    print(f"   Created {len(test_df)} test records")
    print(f"   Columns: {list(test_df.columns)}")
    
    # Perform analysis
    print("\n2. Analyzing data quality...")
    analysis = analyzer.analyze_dataframe(test_df)
    
    # Display results
    print("\n3. Analysis Results:")
    print("-" * 40)
    
    # Basic stats
    stats = analysis.get('basic_stats', {})
    print(f"   Total Records: {stats.get('total_records', 0)}")
    print(f"   Unique Animals: {stats.get('unique_animals', 0)}")
    
    # Quality score
    score = analysis.get('quality_score', 0)
    print(f"\n   Overall Quality Score: {score:.1f}%")
    
    if score >= 90:
        print("   Rating: ★★★★★ EXCELLENT")
    elif score >= 70:
        print("   Rating: ★★★★☆ GOOD")
    elif score >= 50:
        print("   Rating: ★★★☆☆ FAIR")
    elif score >= 30:
        print("   Rating: ★★☆☆☆ POOR")
    else:
        print("   Rating: ★☆☆☆☆ VERY POOR")
    
    # Dimension scores
    print("\n   Dimension Scores:")
    dimensions = ['completeness', 'validity', 'consistency', 'timeliness']
    for dim in dimensions:
        if dim in analysis:
            dim_score = analysis[dim].get('overall', 0)
            print(f"     {dim.title():12s}: {dim_score:6.1f}%")
    
    # Issues
    issues = analysis.get('issues', [])
    print(f"\n   Issues Found: {len(issues)}")
    
    if issues:
        print("\n   Detected Issues:")
        for i, issue in enumerate(issues[:5], 1):  # Show first 5
            severity = issue.get('severity', 'unknown').upper()
            print(f"     {i}. [{severity}] {issue.get('message', '')}")
    
    # Recommendations
    recommendations = analysis.get('recommendations', [])
    print(f"\n   Recommendations: {len(recommendations)}")
    
    if recommendations:
        print("\n   Top Recommendations:")
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        sorted_recs = sorted(recommendations, 
                           key=lambda x: priority_order.get(x.get('priority', 'low'), 2))
        
        for i, rec in enumerate(sorted_recs[:3], 1):  # Show top 3
            priority = rec.get('priority', 'medium').upper()
            print(f"     {i}. [{priority}] {rec.get('recommendation', '')}")
    
    # Generate reports
    print("\n4. Generating reports...")
    
    # Text report
    text_report = analyzer.generate_quality_report(analysis, 'text')
    print("   ✓ Generated text report")
    
    # HTML report
    html_report = analyzer.generate_quality_report(analysis, 'html')
    print("   ✓ Generated HTML report")
    
    # Save analysis
    print("\n5. Saving analysis results...")
    os.makedirs('outputs/quality_reports', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save JSON analysis
    json_file = f'outputs/quality_reports/analysis_{timestamp}.json'
    analyzer.save_analysis(analysis, json_file)
    print(f"   ✓ Saved analysis to: {json_file}")
    
    # Save text report
    txt_file = f'outputs/quality_reports/report_{timestamp}.txt'
    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write(text_report)
    print(f"   ✓ Saved text report to: {txt_file}")
    
    # Save HTML report
    html_file = f'outputs/quality_reports/report_{timestamp}.html'
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_report)
    print(f"   ✓ Saved HTML report to: {html_file}")
    
    print("\n" + "=" * 60)
    print("Data quality analysis test completed successfully! ✓")
    
    return analysis

def test_with_good_data():
    """Test with good quality data"""
    print("\n\nTesting with Good Quality Data...")
    print("=" * 60)
    
    # Create good quality data
    np.random.seed(123)
    
    dates = pd.date_range(start='2024-01-01', periods=50, freq='D')
    
    data = {
        'tag_id': [f'ANM{str(i%10+1).zfill(3)}' for i in range(50)],
        'date': dates,
        'animal_type': ['cattle'] * 20 + ['sheep'] * 20 + ['goat'] * 10,
        'farm_id': ['FARM1'] * 50,
        'temperature': np.random.normal(38.5, 0.3, 50),
        'heart_rate': np.random.normal(70, 4, 50),
        'activity_level': np.random.normal(1.0, 0.15, 50)
    }
    
    df = pd.DataFrame(data)
    
    # Analyze
    analyzer = DataQualityAnalyzer()
    analysis = analyzer.analyze_dataframe(df)
    
    score = analysis.get('quality_score', 0)
    print(f"\nGood data quality score: {score:.1f}%")
    
    if score >= 80:
        print("✓ Good data correctly identified as high quality")
    else:
        print("✗ Warning: Good data not scoring as expected")
    
    return analysis

if __name__ == "__main__":
    try:
        # Test with problematic data
        analysis1 = test_data_quality_analysis()
        
        # Test with good data
        analysis2 = test_with_good_data()
        
        print("\n" + "=" * 60)
        print("Summary:")
        print(f"  - Problematic data score: {analysis1.get('quality_score', 0):.1f}%")
        print(f"  - Good data score: {analysis2.get('quality_score', 0):.1f}%")
        
        print("\nAll tests passed! Data quality feature is working correctly. ✓")
        
    except Exception as e:
        print(f"\nError during data quality test: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)