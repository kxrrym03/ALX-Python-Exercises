import pandas as pd
import numpy as np
from scipy import stats

def run_ttest(Column_A, Column_B):
    """
    Runs an independent T-test between two columns.
    The nan_policy='omit' part is critical because it tells Python
    to ignore empty values, which prevents the test from returning 'NaN'.
    """
    t_statistic, p_value = stats.ttest_ind(Column_A, Column_B, nan_policy='omit')
    return t_statistic, p_value

def print_ttest_results(station_id, measurement, p_val, alpha):
    """
    Interprets the p-value against the significance level (alpha).
    """
    if p_val < alpha:
        print(f" {station_id} ({measurement}):")
        print(f"  P-value: {p_val:.5f} - Significant difference (REJECT Null Hypothesis)\n")
    else:
        print(f" {station_id} ({measurement}):")
        print(f"  P-value: {p_val:.5f} - No significant difference (FAIL TO REJECT Null Hypothesis)\n")