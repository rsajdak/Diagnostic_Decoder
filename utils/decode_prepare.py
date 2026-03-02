from pyspark.sql.functions import col, length, when, lit
import pyspark.sql.functions as F

def filter_valid_ddt_lengths(df, alrt_len, alerts_to_check=None):
    """
    Filter rows based on valid DDT string lengths defined in alrt_len dictionary.
    
    Args:
        df: Input DataFrame with BCM_A001, BCM_A007 columns and vehicle info
        alrt_len: Dictionary containing valid string lengths by family/year/alert
        alerts_to_check: List of alert types to check (default: ['A001', 'A007'])
    
    Returns:
        DataFrame with only rows having valid DDT string lengths
    """
    
    if alerts_to_check is None:
        alerts_to_check = ['A001', 'A007']
    
    # Start with original dataframe
    result_df = df
    
    # Add columns to indicate valid lengths for each alert type
    for alert in alerts_to_check:
        col_name = f"BCM_{alert}"
        valid_length_col = f"valid_length_{alert}"
        has_valid_length_col = f"has_valid_length_{alert}"
        
        if col_name in df.columns:
            # Create a complex when condition based on family and year
            # Initialize with a default case
            length_condition = when(lit(False), lit(-1))  # Start with impossible condition
            
            # Build conditions for each family/year combination
            for family, years in alrt_len.items():
                for year, alerts in years.items():
                    if alert in alerts:
                        expected_length = alerts[alert]
                        # Add condition for this family/year combination
                        length_condition = length_condition.when(
                            (col("C_FAM") == family) & (col("MODELYEAR") == year),
                            lit(expected_length)
                        )
            
            # Add the valid length column
            result_df = result_df.withColumn(valid_length_col, length_condition.otherwise(lit(None)))
            
            # Add boolean column indicating if the actual length matches expected
            result_df = result_df.withColumn(
                has_valid_length_col,
                when(
                    col(valid_length_col).isNotNull() & 
                    (length(col(col_name)) == col(valid_length_col)),
                    lit(True)
                ).otherwise(lit(False))
            )
    
    # Create overall validity column
    validity_conditions = [col(f"has_valid_length_{alert}") for alert in alerts_to_check 
                          if f"BCM_{alert}" in df.columns]
    
    if validity_conditions:
        # All alerts must have valid lengths
        result_df = result_df.withColumn(
            "all_ddt_lengths_valid",
            F.expr(" AND ".join([f"has_valid_length_{alert}" for alert in alerts_to_check 
                                if f"BCM_{alert}" in df.columns]))
        )
    
    return result_df

def get_valid_rows_only(df, alrt_len, alerts_to_check=None):
    """
    Filter and return only rows with all valid DDT string lengths.
    
    Args:
        df: Input DataFrame
        alrt_len: Dictionary containing valid string lengths
        alerts_to_check: List of alert types to check
    
    Returns:
        DataFrame containing only rows with valid DDT lengths
    """
    # Add validity columns
    df_with_validity = filter_valid_ddt_lengths(df, alrt_len, alerts_to_check)
    
    # Filter for valid rows only
    valid_df = df_with_validity.filter(col("all_ddt_lengths_valid") == True)
    
    return valid_df

def add_ddt_length_stats(df, alrt_len, alerts_to_check=None):
    """
    Add length validation and create summary statistics.
    
    Args:
        df: Input DataFrame
        alrt_len: Dictionary containing valid string lengths
        alerts_to_check: List of alert types to check
    
    Returns:
        DataFrame with length validation stats
    """
    if alerts_to_check is None:
        alerts_to_check = ['A001', 'A007']
    
    # Add validity columns
    df_with_validity = filter_valid_ddt_lengths(df, alrt_len, alerts_to_check)
    
    print("DDT String Length Validation Summary")
    print("=" * 50)
    
    # Show summary by family and year
    summary_df = df_with_validity.groupBy("C_FAM", "MODELYEAR").agg(
        F.count("*").alias("total_rows"),
        *[F.sum(F.when(col(f"has_valid_length_{alert}"), 1).otherwise(0)).alias(f"valid_{alert}_count") 
          for alert in alerts_to_check if f"BCM_{alert}" in df.columns],
        F.sum(F.when(col("all_ddt_lengths_valid"), 1).otherwise(0)).alias("all_valid_count")
    ).orderBy("C_FAM", "MODELYEAR")
    
    summary_df.show()
    
    # Show actual vs expected lengths for a sample of invalid rows
    # print("\nSample of rows with invalid lengths:")
    # for alert in alerts_to_check:
    #     col_name = f"BCM_{alert}"
    #     if col_name in df.columns:
    #         print(f"\n{alert} Invalid Lengths:")
    #         invalid_sample = df_with_validity.filter(
    #             col(f"has_valid_length_{alert}") == False
    #         ).select(
    #             "vin", "C_FAM", "MODELYEAR", 
    #             col_name,
    #             length(col(col_name)).alias(f"actual_length_{alert}"),
    #             col(f"valid_length_{alert}").alias(f"expected_length_{alert}")
    #         ).limit(5)
    #         invalid_sample.show(truncate=False)
    
    return df_with_validity

# Example usage function
def apply_length_filtering(df, alrt_len, ddt_alerts, filter_invalid=True):
    """
    Apply length filtering to a DataFrame based on the alert_string_lengths configuration.
    
    Args:
        df: Input DataFrame with DDT data
        alrt_len: Dictionary from alert_string_lengths.py
        filter_invalid: If True, remove invalid rows; if False, just add validation columns
    
    Returns:
        Processed DataFrame
    """
    # Add validation columns and show stats
    df_validated = add_ddt_length_stats(df, alrt_len, ddt_alerts)
    
    if filter_invalid:
        # Return only valid rows
        print(f"\nFiltering to keep only rows with valid DDT lengths...")
        df_valid = df_validated.filter(col("all_ddt_lengths_valid") == True)
        
        print(f"Rows before filtering: {df.count()}")
        print(f"Rows after filtering: {df_valid.count()}")
        
        # Drop the validation helper columns if desired
        cols_to_drop = [col for col in df_valid.columns if col.startswith("valid_length_") 
                       or col.startswith("has_valid_length_") or col == "all_ddt_lengths_valid"]
        df_valid = df_valid.drop(*cols_to_drop)
        
        return df_valid
    else:
        # Return all rows with validation columns
        return df_validated

# Specific function for your use case
def prepare_df_for_decoding(df, fam_var, my_var, ddt_alerts, alrt_len):
    """
    Prepare DataFrame for DDT decoding by filtering valid string lengths.
    
    Args:
        df: Input DataFrame
        fam_var: Family variable (e.g., "DT")
        my_var: Model year (e.g., "2025")
        ddt_alerts: List of DDT alerts to process
        alrt_len: Alert length configuration
    
    Returns:
        DataFrame ready for decoding with only valid DDT strings
    """
    # Filter for specific family if not already done
    if "C_FAM" in df.columns:
        df_family = df.filter(col("C_FAM") == fam_var)
    else:
        df_family = df
    
    # Check if the family/year combination exists in alrt_len
    if fam_var not in alrt_len:
        print(f"Warning: Family '{fam_var}' not found in alert length configuration")
        return df_family
    
    if my_var not in alrt_len[fam_var]:
        print(f"Warning: Year '{my_var}' not found for family '{fam_var}' in alert length configuration")
        return df_family
    
    # Apply length filtering
    df_valid = apply_length_filtering(df_family, alrt_len, ddt_alerts, filter_invalid=True)
    
    return df_valid