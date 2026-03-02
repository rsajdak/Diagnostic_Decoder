# utils/ddt_utils.py
"""
Utility functions for DDT (Diagnostic Data Table) decoding pipeline.
Contains helper functions for data loading, processing, and analysis.
"""

import json
import pandas as pd
import os
import time
from pathlib import Path
from pyspark.sql.functions import udf, col, to_timestamp, length, date_format, to_date
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def resolve_config_paths(config, base_dir=None):
    """
    Resolve relative paths in config to absolute paths.

    Args:
        config: Configuration dictionary
        base_dir: Base directory for relative paths (defaults to script directory)

    Returns:
        Updated config with absolute paths
    """
    if base_dir is None:
        # Use the directory containing this utils file as base
        base_dir = Path(__file__).parent.parent.absolute()
    else:
        base_dir = Path(base_dir).absolute()

    # Create a copy to avoid modifying the original
    config_copy = config.copy()

    if "paths" in config_copy:
        paths_copy = config_copy["paths"].copy()
        for key, path in paths_copy.items():
            if not os.path.isabs(path):
                # Convert to absolute path and normalize
                abs_path = (base_dir / path).resolve()
                paths_copy[key] = str(abs_path)
                print(f"🔧 Resolved {key}: {path} -> {abs_path}")
            else:
                print(f"✅ Already absolute {key}: {path}")
        config_copy["paths"] = paths_copy

    return config_copy


def load_data(spark, config):
    """Load data from the specified table"""
    try:
        df = spark.read.table(config["input_data"]["ada_datastream"])
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def join_vehicle_data(df, spark, config):
    """Join vehicle data with diagnostic data"""
    try:
        df_vehicle = spark.read.table(config["input_data"]["vehicle_data"]).select(
            "VIN",
            "MODEL_YEAR",
            F.expr("substring(MODEL_CODE, 1, 2)").alias("MODEL_CODE"),
            "MODEL_NAME",
        )

        # Join vehicle data with diagnostic data
        df_joined = df.join(df_vehicle, df["vin"] == df_vehicle["VIN"], "inner").select(
            df["vin"],
            df_vehicle["MODEL_YEAR"],
            df_vehicle["MODEL_CODE"],
            df_vehicle["MODEL_NAME"],
            "app_timestamp",
            "shift2park",
            "tbm_keyon",
            "tbm_keyoff",
            col("ibs_tcsm_22a001").alias("DIAG_BCM_A001"),
            col("ibs_tcsm_22a001").alias("DIAG_BCM_A007"),
        )
        # convert to date format
        df_joined = df_joined.withColumn(
            "app_timestamp", to_timestamp(col("app_timestamp"), "yyyy-MM-dd HH:mm:ss")
        )

        # create the dt column
        df_joined = df_joined.withColumn("dt", to_date(col("app_timestamp")))

        return df_joined
    except Exception as e:
        print(f"Error joining vehicle data: {e}")
        return None


def findAlerts(df):
    """Find alerts in the diagnostic data
    Create a list of DDT alerts in this dataset"""
    ddt_alerts = [
        col.split("_", 2)[2]
        for col in df.columns
        if col.startswith("DIAG_") and len(col.split("_")) > 2
    ]
    return ddt_alerts


def analyze_families(df, config):
    """Analyze the dataset to identify all available families"""
    try:
        # Get unique families and their counts from the joined data
        # Analyze the dataset to identify all available families
        print("Analyzing dataset to identify all available vehicle families...")

        # Get unique families and their counts from the joined data
        families_summary = (
            df.groupBy("MODEL_CODE", "MODEL_YEAR")
            .count()
            .orderBy("MODEL_CODE", "MODEL_YEAR")
        )
        print("Families in dataset:")
        families_summary.show()

        # Also check what conversion files are available
        base_conv_path = config["paths"]["conversions"]
        available_families = []
        if os.path.exists(base_conv_path):
            for family in os.listdir(base_conv_path):
                if os.path.isdir(os.path.join(base_conv_path, family)):
                    available_families.append(family)

        print(f"Available conversion files for families: {sorted(available_families)}")

        # Check which families have both data and conversion files
        data_families = [
            row["MODEL_CODE"]
            for row in families_summary.select("MODEL_CODE").distinct().collect()
        ]
        print(
            "data_families ", data_families, "available_families ", available_families
        )
        families_with_conversions = list(set(data_families) & set(available_families))
        print(
            f"Families with both data and conversion files: {sorted(families_with_conversions)}"
        )

    except Exception as e:
        print(f"Error analyzing families: {e}")
        return None


def load_ddt_structure(family, year, ddt_type, config):
    """Load DDT structure file for a specific family and year"""
    dir_path = f"{config['paths']['conversions']}{family}/{year}/"
    ddt_file = f"DDT_BCM_{ddt_type}_{family}_{year}.json"

    try:
        file_path = os.path.join(dir_path, ddt_file)
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r") as file:
            return json.load(file)
    except Exception as e:
        print(f"Error loading DDT structure for {family}/{year}/{ddt_type}: {e}")
        return None


def availFamilies(df, config, go_fast=False):
    """
    Analyze the dataset to identify all available families and their model years.
    Filter for diagnostic messages with correct string lengths.

    Args:
        df: DataFrame containing the diagnostic data
        config: Configuration dictionary
        go_fast: Boolean flag to skip detailed output for faster execution

    Returns:
        tuple: (families_with_conversions, available_families, family_year_combinations, df_filtered)
            - families_with_conversions: List of families that have conversion files
            - available_families: List of all families with conversion files available
            - family_year_combinations: List of tuples (family, year) for families with conversions
            - df_filtered: DataFrame filtered for correct string lengths
    """
    # Import the alert string lengths
    from helpers.alert_string_lengths import alrt_len

    # Analyze the dataset to identify all available families
    print("Analyzing dataset to identify all available vehicle families...")

    # Get unique families and their counts from the joined data
    families_summary = (
        df.groupBy("MODEL_CODE", "MODEL_YEAR")
        .count()
        .orderBy("MODEL_CODE", "MODEL_YEAR")
    )

    if go_fast == False:
        print("Families in dataset:")
        families_summary.show()

    # Also check what conversion files are available
    base_conv_path = config["paths"]["conversions"]
    print(f"\nChecking conversion files in: {base_conv_path}")
    available_families = []
    available_family_years = []  # New: store family-year combinations

    if os.path.exists(base_conv_path):
        for family in os.listdir(base_conv_path):
            family_path = os.path.join(base_conv_path, family)
            if os.path.isdir(family_path):
                available_families.append(family)

                # Check for available years within each family directory
                try:
                    for year in os.listdir(family_path):
                        year_path = os.path.join(family_path, year)
                        if os.path.isdir(year_path):
                            # Verify that required conversion files exist
                            conv_L_file = f"conv_L_BCM_{family}_{year}.csv"
                            conv_T_file = f"conv_T_BCM_{family}_{year}.csv"

                            if os.path.exists(
                                os.path.join(year_path, conv_L_file)
                            ) and os.path.exists(os.path.join(year_path, conv_T_file)):
                                available_family_years.append((family, year))
                except Exception as e:
                    print(f"Error checking years for family {family}: {e}")

    print(f"   Available conversion files for families: {sorted(available_families)}")
    print(f"   Available family-year combinations: {sorted(available_family_years)}")
    print(
        "\n   Checking for family-year combinations that has both data and conversion files..."
    )

    # Check which families have both data and conversion files
    data_families = [
        row["MODEL_CODE"]
        for row in families_summary.select("MODEL_CODE").distinct().collect()
    ]
    families_with_conversions = list(set(data_families) & set(available_families))

    # Get family-year combinations that exist in both data and conversion files
    data_family_years = [
        (row["MODEL_CODE"], str(row["MODEL_YEAR"]))
        for row in families_summary.collect()
    ]
    family_year_combinations_with_conversions = list(
        set(data_family_years) & set(available_family_years)
    )

    # NEW: Filter DataFrame for diagnostic messages with correct string lengths
    print("\n   Filtering for diagnostic messages with correct string lengths...")

    # Start with a base condition that's always false, then OR with valid conditions
    length_filter_condition = F.lit(False)

    for fam_var in families_with_conversions:
        if fam_var in alrt_len:
            # Now iterate through model years within each family
            for model_year in alrt_len[fam_var]:
                # Convert model year to string for consistency with DataFrame
                model_year_str = str(model_year)

                # Iterate through diagnostic alerts for this family-year combination
                for diag_alrt in alrt_len[fam_var][model_year]:
                    # Check if the diagnostic column exists in the DataFrame
                    diag_col_name = f"DIAG_BCM_{diag_alrt}"
                    if diag_col_name in df.columns:
                        # Build the condition for this family, model year, and alert combination
                        condition = (
                            (col("MODEL_CODE") == fam_var)
                            & (col("MODEL_YEAR") == model_year_str)
                            & (
                                length(col(diag_col_name))
                                == alrt_len[fam_var][model_year][diag_alrt]
                            )
                        )
                        # OR this condition with the existing filter
                        length_filter_condition = length_filter_condition | condition

                        if not go_fast:
                            print(
                                f"   Added filter: {fam_var} - {model_year_str} - {diag_alrt} - expected length: {alrt_len[fam_var][model_year][diag_alrt]}"
                            )

    # Apply the length filter to the DataFrame
    df_filtered = df.filter(length_filter_condition)

    # Check the results of filtering
    if not go_fast:
        print("\n   Counts after string length filtering:")
        filtered_counts = (
            df_filtered.groupBy("MODEL_CODE", "MODEL_YEAR")
            .count()
            .orderBy("MODEL_CODE", "MODEL_YEAR")
        )
        filtered_counts.show()

        # Show original vs filtered counts
        original_total = df.count()
        filtered_total = df_filtered.count()
        print(f"   Original total rows: {original_total}")
        print(f"   Filtered total rows: {filtered_total}")
        print(f"   Rows removed: {original_total - filtered_total}")

    if go_fast == False:
        print(
            f"Families with both data and conversion files: {sorted(families_with_conversions)}"
        )
        print(
            f"Family-year combinations with both data and conversion files: {sorted(family_year_combinations_with_conversions)}"
        )

    return (
        families_with_conversions,
        sorted(available_families),
        sorted(family_year_combinations_with_conversions),
        df_filtered,
    )


def create_sample_dataset(df, families_with_conversions, go_fast=False):
    """
    Create a sample dataset with 50 rows for each family in families_with_conversions.

    Args:
        df: DataFrame containing the diagnostic data
        families_with_conversions: List of families to sample
        go_fast: Boolean flag to skip detailed output for faster execution

    Returns:
        Sampled DataFrame with 50 rows per family
    """
    print(f"Creating sample with 50 rows for each family: {families_with_conversions}")

    df = df.filter(col("DIAG_BCM_A001").isNotNull())

    # Create a window specification to number rows within each family
    window = Window.partitionBy("MODEL_CODE").orderBy("app_timestamp")

    # Add row numbers within each family and filter to get 50 rows per family
    df_sample = (
        df.filter(col("MODEL_CODE").isin(families_with_conversions))
        .withColumn("row_num", F.row_number().over(window))
        .filter(col("row_num") <= 50)
        .drop("row_num")
    )

    if go_fast == False:
        # Verify the sampling worked correctly
        sample_counts = df_sample.groupBy("MODEL_CODE").count().orderBy("MODEL_CODE")
        print("Sample counts by family:")
        sample_counts.show()

    return df_sample


def loadConvTables(fam_yr, spark, config):
    """Load conversion tables for a specific family and year"""

    # Get the absolute conversions path
    conversions_path = config["paths"]["conversions"]

    # Ensure we have an absolute path
    if not os.path.isabs(conversions_path):
        # If somehow still relative, make it absolute
        from pathlib import Path

        conversions_path = str(Path(conversions_path).absolute())

    # Construct file paths
    conv_L = f"conv_L_BCM_{fam_yr[0]}_{fam_yr[1]}.csv"
    conv_T = f"conv_T_BCM_{fam_yr[0]}_{fam_yr[1]}.csv"

    # Build full paths
    conv_L_path = os.path.join(conversions_path, fam_yr[0], fam_yr[1], conv_L)
    conv_T_path = os.path.join(conversions_path, fam_yr[0], fam_yr[1], conv_T)

    print(f"🔍 Looking for conversion tables at:")
    print(f"   L table: {conv_L_path}")
    print(f"   T table: {conv_T_path}")
    print(f"   L exists: {os.path.exists(conv_L_path)}")
    print(f"   T exists: {os.path.exists(conv_T_path)}")

    try:
        # Use proper file:// URLs for Spark
        conv_T_url = f"file://{conv_T_path}"
        conv_L_url = f"file://{conv_L_path}"

        print(f"📂 Reading T table from: {conv_T_url}")
        conv_tbl_T = spark.read.csv(conv_T_url, header=True, inferSchema=True)
        # Convert to pandas for easier filtering in the decode function
        conv_tbl_pd_T = conv_tbl_T.toPandas()

        print(f"📂 Reading L table from: {conv_L_url}")
        # Load linear conversion table
        conv_tbl_L = spark.read.csv(conv_L_url, header=True, inferSchema=True)
        # Convert to pandas for easier filtering in the decode function
        conv_tbl_pd_L = conv_tbl_L.toPandas()

        print(f"✅ Successfully loaded conversion tables for {fam_yr[0]}/{fam_yr[1]}")
        print(f"   T table shape: {conv_tbl_pd_T.shape}")
        print(f"   L table shape: {conv_tbl_pd_L.shape}")

        return conv_tbl_pd_L, conv_tbl_pd_T

    except Exception as e:
        print(f"❌ Error loading conversion tables for {fam_yr[0]}/{fam_yr[1]}: {e}")
        print(f"   Error type: {type(e).__name__}")

        # Try alternative approach using local file system
        try:
            print("🔄 Trying pandas direct read as fallback...")
            conv_tbl_pd_T = pd.read_csv(conv_T_path)
            conv_tbl_pd_L = pd.read_csv(conv_L_path)

            print(f"✅ Fallback successful for {fam_yr[0]}/{fam_yr[1]}")
            return conv_tbl_pd_L, conv_tbl_pd_T

        except Exception as e2:
            print(f"❌ Fallback also failed: {e2}")
            return None, None


def decode_ddt_wrapper(
    ddt_string,
    structure_name,
    family=None,
    model_year=None,
    ddt_type="A001",
    config=None,
):
    """Wrapper for the decode_ddt function that works with UDFs and uses vehicle-specific conversions.

    Args:
        ddt_string: String containing the hex DDT code
        structure_name: Name of the structure to use for decoding
        family: Vehicle family (optional)
        model_year: Vehicle model year (optional)
        ddt_type: Type of DDT to decode (A001 or A007)
        config: Configuration dictionary

    Returns:
        JSON string with decoded data
    """
    if not ddt_string:
        return json.dumps({"error": "Empty DDT string"})

    try:
        # Get structure - fix the path to use conversions directory
        file_path = os.path.join(
            f"{config['paths']['conversions']}{family}/{model_year}/",
            DDT_file[ddt_type],
        )
        print(f"IN DECODE_DDT_WRAPPER. Loading structure from {file_path}")

        # Check if file exists
        if not os.path.exists(file_path):
            return json.dumps({"error": f"Structure file not found: {file_path}"})

        with open(file_path, "r") as file:
            json_structure = json.load(file)

        if not json_structure:
            return json.dumps({"error": f"Structure {structure_name} not found"})

        # Get the appropriate conversion tables
        if family and model_year:
            # Use the broadcast conversion tables
            conv_table_L = conv_tbl_pd_L_broadcast.value
            conv_table_T = conv_tbl_pd_T_broadcast.value
        else:
            # Fall back to the default broadcast tables if no family/year specified
            conv_table_L = conv_tbl_pd_L_broadcast.value
            conv_table_T = conv_tbl_pd_T_broadcast.value

        # Use the appropriate conversion tables
        from decode_func import decode_ddt

        result = decode_ddt(ddt_string, json_structure, conv_table_T, conv_table_L)

        # Add vehicle info to the result
        if family and model_year:
            result["vehicle_family"] = family
            result["model_year"] = model_year

        # Convert to JSON string for return
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"error": str(e), "traceback": str(e.__class__.__name__)})


def optimized_flatten_json(df, diag_alrt, fam_yr, json_column_name, column_types=None):
    """
    Flattens a JSON column using Spark's native JSON functions.
    Casts columns to specified data types from a dictionary.

    Args:
        df: DataFrame containing the JSON column
        diag_alrt: Diagnostic alert type
        fam_yr: Tuple of (family, year)
        json_column_name: Name of the column containing the JSON strings
        column_types: Dictionary mapping column names to data types
                     (e.g., {'Engine_Speed': 'int', 'Vehicle_Status': 'string'})
                     Supported types: 'string', 'int', 'float', 'boolean'

    Returns:
        DataFrame with flattened columns cast to specified types
    """
    from helpers.alert_string_lengths import alrt_len

    # Initialize column_types if not provided
    if column_types is None:
        column_types = {}

    # Map string type names to PySpark types
    type_mapping = {
        "string": StringType(),
        "str": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "float": FloatType(),
        "double": FloatType(),
        "bool": BooleanType(),
        "boolean": BooleanType(),
    }

    # # Build the query string dynamically with error handling
    # try:
    #     expected_length = alrt_len[fam_yr[0]][fam_yr[1]][diag_alrt]
    #     query_string = f'(length(col("DIAG_BCM_{diag_alrt}")) == {expected_length})'
    # except KeyError as e:
    #     print(
    #         f"Warning: Missing alert length definition for family={fam_yr[0]}, year={fam_yr[1]}, alert={diag_alrt}"
    #     )
    #     print(f"Available families: {list(alrt_len.keys())}")
    #     if fam_yr[0] in alrt_len:
    #         print(
    #             f"Available years for {fam_yr[0]}: {list(alrt_len[fam_yr[0]].keys())}"
    #         )
    #         if fam_yr[1] in alrt_len[fam_yr[0]]:
    #             print(
    #                 f"Available alerts for {fam_yr[0]}/{fam_yr[1]}: {list(alrt_len[fam_yr[0]][fam_yr[1]].keys())}"
    #             )

    #     # Fallback: create a query that doesn't filter by length
    #     query_string = f'col("DIAG_BCM_{diag_alrt}").isNotNull()'
    #     print(f"Using fallback query: {query_string}")

    df_good_row = df.filter(eval(query_string)).limit(1).collect()

    descriptions = {}

    # Check if we got any rows and if the column exists
    if df_good_row and json_column_name in df_good_row[0].asDict():
        try:
            # Get the JSON string from the first (and only) row
            json_data = df_good_row[0][json_column_name]
            if json_data:  # Check if the JSON data is not null
                data = json.loads(json_data)
                for entry in data.get("decoded_values", []):
                    desc = entry.get("description")
                    if desc and desc != "Reserved":
                        descriptions[desc] = descriptions.get(desc, "")
        except Exception as e:
            print(f"Error parsing JSON: {e}")

    # Create a clean column name function
    def clean_column_name(name):
        import re

        base_name = re.sub(r"[^a-zA-Z0-9_]", "_", name).replace("__", "_")
        return base_name

    # Start with the original dataframe
    result_df = df

    # For each description, create a new column using expressions
    for desc, unit in sorted(descriptions.items()):
        clean_desc = clean_column_name(desc)

        # expression to find the matching description and extract its value
        expr_str = f"""
        TRANSFORM(
            FILTER(
                from_json({json_column_name}, 'struct<decoded_values:array<struct<description:string,decoded_value:string,units:string>>>').decoded_values,
                x -> x.description = '{desc}'
            ),
            x -> x.decoded_value
        )[0]
        """

        # Add the new column to the DataFrame
        result_df = result_df.withColumn(clean_desc, F.expr(expr_str))

        # Cast the column to the specified type if provided
        # Look for exact match or pattern match in column_types dictionary
        column_type = None
        for pattern, dtype in column_types.items():
            if pattern == clean_desc or (
                pattern.endswith("*") and clean_desc.startswith(pattern[:-1])
            ):
                column_type = dtype
                break

        if column_type and column_type.lower() in type_mapping:
            # Create safe casting with null handling
            result_df = result_df.withColumn(
                clean_desc,
                F.when(col(clean_desc).isNull(), None).otherwise(
                    col(clean_desc).cast(type_mapping[column_type.lower()])
                ),
            )

    # Add the goodDiagInput column with error handling
    try:
        result_df = result_df.withColumn(
            f"goodDiagInput_{diag_alrt}", F.when(eval(query_string), "x").otherwise("")
        )
    except Exception as e:
        print(f"Error adding goodDiagInput column: {e}")
        # Fallback: add a simple column
        result_df = result_df.withColumn(f"goodDiagInput_{diag_alrt}", F.lit("unknown"))

    return result_df


def json_flatening(df_decoded, ddt_alerts, fam_yr):
    """Flatten JSON columns for all DDT alerts"""
    # Import the alert string lengths
    try:
        from helpers.column_data_types import column_types_dict
    except ImportError:
        print("Warning: column_data_types module not found, using empty column types")
        column_types_dict = {}

    d1 = None
    # Start with the decoded dataframe
    d1 = df_decoded

    # Loop through all alerts and flatten each one
    for alert in ddt_alerts:
        # Get the column types for this alert, or use an empty dict if not defined
        alert_column_types = column_types_dict.get(alert, {})

        # Flatten the JSON column for this alert
        d1 = optimized_flatten_json(
            d1, alert, fam_yr, f"decoded_BCM_{alert}", column_types=alert_column_types
        )
    return d1


def decode_all_families(
    df, family_year_combinations, spark, config, show_progress=True
):
    """
    Apply DDT decoding to all families and return a combined dataframe with progress tracking.

    Args:
        df: Input DataFrame containing diagnostic data
        family_year_combinations: List of tuples (family, year) to process
        spark: Spark session
        config: Configuration dictionary
        show_progress: Boolean to enable/disable progress tracking

    Returns:
        DataFrame: Combined decoded dataframe with all families, or None if no data processed
    """
    print("\n=== Step 4: Apply DDT Decoding ===")

    # Initialize progress tracking
    total_families = len(family_year_combinations)
    completed_families = 0
    start_time = time.time()

    if show_progress:
        print(f"Processing {total_families} family-year combinations...")
        print("Progress: [" + " " * 50 + "] 0%")

    # Initialize an empty list to store decoded dataframes
    decoded_dataframes = []

    # Loop through and decode each family separately
    for i, fam_yr in enumerate(family_year_combinations):
        family_start_time = time.time()

        if show_progress:
            print(f"\n[{i+1}/{total_families}] Starting {fam_yr[0]}-{fam_yr[1]}...")

        decoded_df = decode_single_family(
            df, fam_yr, spark, config, show_progress=show_progress
        )

        if decoded_df is not None:
            decoded_dataframes.append(decoded_df)
            completed_families += 1

        # Update progress
        if show_progress:
            family_time = time.time() - family_start_time
            elapsed_time = time.time() - start_time
            progress_percent = ((i + 1) / total_families) * 100

            # Create progress bar
            filled_length = int(50 * (i + 1) // total_families)
            bar = "█" * filled_length + "-" * (50 - filled_length)

            # Estimate remaining time
            if i > 0:
                avg_time_per_family = elapsed_time / (i + 1)
                remaining_families = total_families - (i + 1)
                estimated_remaining = avg_time_per_family * remaining_families
                eta_str = f"ETA: {estimated_remaining/60:.1f}m"
            else:
                eta_str = "ETA: calculating..."

            print(
                f"Progress: [{bar}] {progress_percent:.1f}% | "
                f"Family time: {family_time:.1f}s | "
                f"Total elapsed: {elapsed_time/60:.1f}m | {eta_str}"
            )

            if decoded_df is not None:
                print(
                    f"✓ Successfully processed {fam_yr[0]}-{fam_yr[1]} ({decoded_df.count()} rows)"
                )
            else:
                print(f"✗ Failed to process {fam_yr[0]}-{fam_yr[1]}")

    # =================================================
    # Union All Decoded Dataframes
    # =================================================
    print(f"\n=== Step 5: Union All Decoded Dataframes ===")
    print(f"Successfully processed {completed_families}/{total_families} families")

    if decoded_dataframes:
        print("Combining dataframes...")
        union_start_time = time.time()

        # Start with the first dataframe
        final_decoded_df = decoded_dataframes[0]

        # Union with all subsequent dataframes
        for i, df_to_union in enumerate(decoded_dataframes[1:], 1):
            if show_progress:
                union_progress = (i / (len(decoded_dataframes) - 1)) * 100
                print(
                    f"Union progress: {union_progress:.1f}% - Combining dataframe {i+1} of {len(decoded_dataframes)}"
                )

            try:
                # Use unionByName to handle potential schema differences
                final_decoded_df = final_decoded_df.unionByName(
                    df_to_union, allowMissingColumns=True
                )
            except Exception as e:
                print(f"Error during union operation: {e}")
                print("Attempting regular union...")
                try:
                    final_decoded_df = final_decoded_df.union(df_to_union)
                except Exception as e2:
                    print(f"Regular union also failed: {e2}")
                    print("Skipping this dataframe...")
                    continue

        union_time = time.time() - union_start_time
        total_time = time.time() - start_time

        final_row_count = final_decoded_df.count()

        print(f"\n🎉 COMPLETION SUMMARY:")
        print(f"   • Total processing time: {total_time/60:.1f} minutes")
        print(f"   • Union time: {union_time:.1f} seconds")
        print(f"   • Families processed: {completed_families}/{total_families}")
        print(f"   • Final row count: {final_row_count:,}")
        print(f"   • Average time per family: {total_time/total_families:.1f} seconds")

        # Display summary by family
        print("\nSummary by family:")
        final_decoded_df.groupBy("MODEL_CODE", "MODEL_YEAR").count().orderBy(
            "MODEL_CODE", "MODEL_YEAR"
        ).show()

        return final_decoded_df

    else:
        print("❌ No dataframes were successfully decoded!")
        return None


def decode_single_family(df, fam_yr, spark, config, show_progress=True):
    """
    Decode DDT data for a single family-year combination with progress tracking.

    Args:
        df: Input DataFrame containing diagnostic data
        fam_yr: Tuple of (family, year) to process
        spark: Spark session
        config: Configuration dictionary
        show_progress: Boolean to enable/disable progress tracking

    Returns:
        DataFrame: Decoded and flattened dataframe for the family, or None if failed
    """
    if show_progress:
        print(f"  🔄 Loading conversion tables for {fam_yr[0]}-{fam_yr[1]}...")

    # Load conversion tables
    conv_tbl_pd_L, conv_tbl_pd_T = loadConvTables(fam_yr, spark, config)

    if conv_tbl_pd_L is None or conv_tbl_pd_T is None:
        print(f"  ❌ Conversion tables not found for {fam_yr}")
        return None

    if show_progress:
        print(f"  🔄 Filtering data for {fam_yr[0]}-{fam_yr[1]}...")

    # Filter df to only include rows with the current family and model year
    df_filtered = df.filter(
        (col("MODEL_CODE") == fam_yr[0]) & (col("MODEL_YEAR") == fam_yr[1])
    )

    # Check if we have any data for this family-year combination
    row_count = df_filtered.count()
    if row_count == 0:
        print(f"  ❌ No data found for {fam_yr[0]}-{fam_yr[1]}")
        return None

    if show_progress:
        print(f"  ✓ Found {row_count:,} rows for {fam_yr[0]}-{fam_yr[1]}")
        print(f"  🔄 Identifying DDT alerts...")

    # Find available DDT alerts
    ddt_alerts = findAlerts(df_filtered)

    if show_progress:
        print(f"  ✓ Found {len(ddt_alerts)} DDT alerts: {ddt_alerts}")

    # Create DDT file mapping
    DDT_files = {}
    for ddt_alert in ddt_alerts:
        DDT_files[ddt_alert] = f"DDT_BCM_{ddt_alert}_{fam_yr[0]}_{fam_yr[1]}.json"

    if show_progress:
        print(f"  🔄 Creating UDFs and applying decoding...")

    # Create UDFs for each alert with proper closures
    df_decoded = df_filtered

    for alert_idx, alert in enumerate(ddt_alerts):
        if show_progress:
            alert_progress = ((alert_idx + 1) / len(ddt_alerts)) * 100
            print(
                f"    Alert {alert_idx + 1}/{len(ddt_alerts)} ({alert_progress:.1f}%): Processing {alert}..."
            )

        # Create a decoder function with all logic self-contained
        def create_decoder(
            alert_type, family, year, conv_L, conv_T, ddt_file_map, config_paths
        ):
            def decode_wrapper(ddt_string):
                if not ddt_string:
                    return json.dumps({"error": "Empty DDT string"})

                try:
                    # Construct the file path for the DDT structure
                    ddt_filename = ddt_file_map.get(alert_type)
                    if not ddt_filename:
                        return json.dumps(
                            {"error": f"DDT file mapping not found for {alert_type}"}
                        )

                    file_path = os.path.join(
                        config_paths["conversions"], family, year, ddt_filename
                    )

                    # Check if file exists
                    if not os.path.exists(file_path):
                        return json.dumps(
                            {"error": f"Structure file not found: {file_path}"}
                        )

                    # Load the JSON structure
                    with open(file_path, "r") as file:
                        json_structure = json.load(file)

                    if not json_structure:
                        return json.dumps(
                            {"error": f"Empty structure for {alert_type}"}
                        )

                    # Import decode_ddt inside the function to ensure it's available
                    from decode_func import decode_ddt

                    # Decode the DDT string
                    result = decode_ddt(ddt_string, json_structure, conv_T, conv_L)

                    # Add vehicle info to the result
                    result["vehicle_family"] = family
                    result["model_year"] = year
                    result["ddt_type"] = alert_type

                    # Convert to JSON string for return
                    return json.dumps(result)

                except Exception as e:
                    return json.dumps(
                        {
                            "error": str(e),
                            "error_type": str(e.__class__.__name__),
                            "ddt_type": alert_type,
                            "family": family,
                            "year": year,
                        }
                    )

            return decode_wrapper

        # Create the UDF with captured variables
        decode_udf = udf(
            create_decoder(
                alert,
                fam_yr[0],
                fam_yr[1],
                conv_tbl_pd_L,
                conv_tbl_pd_T,
                DDT_files,
                config["paths"],
            ),
            StringType(),
        )

        # Apply the UDF
        df_decoded = df_decoded.withColumn(
            f"decoded_BCM_{alert}", decode_udf(col(f"DIAG_BCM_{alert}"))
        )

        if show_progress:
            print(f"    ✓ Completed decoding for {alert}")

    if show_progress:
        print(f"  🔄 Flattening JSON columns...")

    # Flatten the decoded column
    df_decoded_flattened = json_flatening(df_decoded, ddt_alerts, fam_yr)

    if show_progress:
        final_columns = len(df_decoded_flattened.columns)
        print(f"  ✓ Completed {fam_yr[0]}-{fam_yr[1]} with {final_columns} columns")

    return df_decoded_flattened


def decode_all_families_with_row_progress(
    df,
    family_year_combinations,
    spark,
    config,
    checkpoint_interval=1000,
    show_progress=True,
):
    """
    Enhanced version that processes data in chunks and shows row-level progress.
    Useful for very large datasets.

    Args:
        df: Input DataFrame containing diagnostic data
        family_year_combinations: List of tuples (family, year) to process
        spark: Spark session
        config: Configuration dictionary
        checkpoint_interval: Number of rows to process before showing progress
        show_progress: Boolean to enable/disable progress tracking

    Returns:
        DataFrame: Combined decoded dataframe with all families, or None if no data processed
    """
    print("\n=== DDT Decoding with Row-Level Progress Tracking ===")

    # Get total row count upfront
    total_rows = df.count()
    processed_rows = 0
    start_time = time.time()

    if show_progress:
        print(f"Total rows to process: {total_rows:,}")
        print(f"Checkpoint interval: {checkpoint_interval:,} rows")

    decoded_dataframes = []

    for fam_idx, fam_yr in enumerate(family_year_combinations):
        if show_progress:
            print(
                f"\n[Family {fam_idx+1}/{len(family_year_combinations)}] Processing {fam_yr[0]}-{fam_yr[1]}..."
            )

        # Filter for this family
        family_df = df.filter(
            (col("MODEL_CODE") == fam_yr[0]) & (col("MODEL_YEAR") == fam_yr[1])
        )
        family_row_count = family_df.count()

        if family_row_count == 0:
            if show_progress:
                print(f"  No data for {fam_yr[0]}-{fam_yr[1]}, skipping...")
            continue

        if show_progress:
            print(f"  Family has {family_row_count:,} rows")

        # Process this family in chunks if it's large
        if family_row_count > checkpoint_interval:
            # Add row numbers for chunking
            window = Window.orderBy("app_timestamp")
            family_df_numbered = family_df.withColumn(
                "row_num", F.row_number().over(window)
            )

            # Process in chunks
            chunks_processed = 0
            total_chunks = (
                family_row_count + checkpoint_interval - 1
            ) // checkpoint_interval

            family_results = []

            for chunk_start in range(0, family_row_count, checkpoint_interval):
                chunk_end = min(chunk_start + checkpoint_interval, family_row_count)

                # Get chunk
                chunk_df = family_df_numbered.filter(
                    (col("row_num") > chunk_start) & (col("row_num") <= chunk_end)
                ).drop("row_num")

                # Process chunk
                chunk_result = decode_single_family(
                    chunk_df, fam_yr, spark, config, show_progress=False
                )

                if chunk_result is not None:
                    family_results.append(chunk_result)

                chunks_processed += 1
                processed_rows += chunk_end - chunk_start

                if show_progress:
                    chunk_progress = (chunks_processed / total_chunks) * 100
                    overall_progress = (processed_rows / total_rows) * 100
                    elapsed = time.time() - start_time

                    if processed_rows > 0:
                        rate = processed_rows / elapsed
                        eta_seconds = (total_rows - processed_rows) / rate
                        eta_str = f"ETA: {eta_seconds/60:.1f}m"
                    else:
                        eta_str = "ETA: calculating..."

                    print(
                        f"    Chunk {chunks_processed}/{total_chunks} ({chunk_progress:.1f}%) | "
                        f"Overall: {overall_progress:.1f}% ({processed_rows:,}/{total_rows:,}) | "
                        f"Rate: {rate:.1f} rows/sec | {eta_str}"
                    )

            # Combine family chunks
            if family_results:
                family_decoded = family_results[0]
                for chunk_result in family_results[1:]:
                    family_decoded = family_decoded.unionByName(
                        chunk_result, allowMissingColumns=True
                    )
                decoded_dataframes.append(family_decoded)

        else:
            # Process entire family at once
            family_decoded = decode_single_family(
                family_df, fam_yr, spark, config, show_progress=False
            )
            if family_decoded is not None:
                decoded_dataframes.append(family_decoded)
            processed_rows += family_row_count

            if show_progress:
                overall_progress = (processed_rows / total_rows) * 100
                print(
                    f"  Overall progress: {overall_progress:.1f}% ({processed_rows:,}/{total_rows:,})"
                )

    # Union all results
    if decoded_dataframes:
        print(f"\nCombining {len(decoded_dataframes)} family dataframes...")
        final_df = decoded_dataframes[0]
        for df_to_union in decoded_dataframes[1:]:
            final_df = final_df.unionByName(df_to_union, allowMissingColumns=True)

        total_time = time.time() - start_time
        print(f"\n🎉 Processing complete!")
        print(f"   Total time: {total_time/60:.1f} minutes")
        print(f"   Final rows: {final_df.count():,}")
        print(f"   Average rate: {total_rows/(total_time):.1f} rows/second")

        return final_df
    else:
        print("❌ No families were successfully processed!")
        return None


# Keep all other existing functions from the original ddt_utils.py
# (load_data, join_vehicle_data, findAlerts, etc. - copy them from the original file)


def create_progress_tracker(total_items, description="Processing"):
    """
    Create a simple progress tracker for use in loops.

    Args:
        total_items: Total number of items to process
        description: Description of what's being processed

    Returns:
        Function that can be called to update progress
    """
    start_time = time.time()

    def update_progress(current_item, extra_info=""):
        if total_items == 0:
            return

        progress = (current_item / total_items) * 100
        elapsed = time.time() - start_time

        if current_item > 0:
            rate = current_item / elapsed
            eta_seconds = (total_items - current_item) / rate if rate > 0 else 0
            eta_str = f"ETA: {eta_seconds/60:.1f}m"
        else:
            eta_str = "ETA: calculating..."

        # Create progress bar
        bar_length = 30
        filled_length = int(bar_length * current_item // total_items)
        bar = "█" * filled_length + "-" * (bar_length - filled_length)

        print(
            f"{description}: [{bar}] {progress:.1f}% ({current_item}/{total_items}) | "
            f"Elapsed: {elapsed/60:.1f}m | {eta_str} {extra_info}"
        )

    return update_progress


def validate_family_combinations(family_year_combinations, config):
    """
    Validate that all family-year combinations have the required files.

    Args:
        family_year_combinations: List of tuples (family, year)
        config: Configuration dictionary

    Returns:
        tuple: (valid_combinations, invalid_combinations)
    """
    valid_combinations = []
    invalid_combinations = []

    for fam_yr in family_year_combinations:
        family, year = fam_yr

        # Check for conversion table files
        conv_L_file = f"{config['paths']['conversions']}{family}/{year}/conv_L_BCM_{family}_{year}.csv"
        conv_T_file = f"{config['paths']['conversions']}{family}/{year}/conv_T_BCM_{family}_{year}.csv"

        # Check for DDT structure files (common ones)
        ddt_a001_file = f"{config['paths']['conversions']}{family}/{year}/DDT_BCM_A001_{family}_{year}.json"
        ddt_a007_file = f"{config['paths']['conversions']}{family}/{year}/DDT_BCM_A007_{family}_{year}.json"

        missing_files = []
        if not os.path.exists(conv_L_file):
            missing_files.append(conv_L_file)
        if not os.path.exists(conv_T_file):
            missing_files.append(conv_T_file)
        if not os.path.exists(ddt_a001_file):
            missing_files.append(ddt_a001_file)

        if missing_files:
            invalid_combinations.append((fam_yr, missing_files))
            print(f"Missing files for {family}-{year}: {missing_files}")
        else:
            valid_combinations.append(fam_yr)
            print(f"✓ All required files found for {family}-{year}")

    return valid_combinations, invalid_combinations


def get_decoding_summary(decoded_df):
    """
    Generate a summary report of the decoding results.

    Args:
        decoded_df: DataFrame with decoded results

    Returns:
        dict: Summary statistics
    """
    if decoded_df is None:
        return {"error": "No decoded dataframe provided"}

    summary = {}

    # Basic counts
    summary["total_rows"] = decoded_df.count()
    summary["total_columns"] = len(decoded_df.columns)

    # Family breakdown
    family_counts = decoded_df.groupBy("MODEL_CODE", "MODEL_YEAR").count().collect()
    summary["families"] = {
        f"{row.MODEL_CODE}-{row.MODEL_YEAR}": row.count for row in family_counts
    }

    # Column breakdown
    decoded_columns = [
        col for col in decoded_df.columns if col.startswith("decoded_BCM_")
    ]
    flattened_columns = [
        col
        for col in decoded_df.columns
        if not col.startswith(
            ("vin", "MODEL", "app_timestamp", "DIAG_", "decoded_BCM_", "goodDiagInput_")
        )
    ]

    summary["decoded_json_columns"] = len(decoded_columns)
    summary["flattened_data_columns"] = len(flattened_columns)

    # Error analysis
    error_columns = []
    for col_name in decoded_columns:
        error_count = decoded_df.filter(F.col(col_name).contains("error")).count()
        if error_count > 0:
            error_columns.append({"column": col_name, "error_count": error_count})

    summary["errors"] = error_columns

    return summary


def save_results_with_metadata(
    decoded_df, table_name, config, family_year_combinations
):
    """
    Save the decoded results with metadata.

    Args:
        decoded_df: DataFrame to save
        table_name: Name of the table to save to
        config: Configuration dictionary
        family_year_combinations: List of processed family-year combinations
    """
    if decoded_df is None:
        print("No dataframe to save!")
        return

    # Add processing metadata
    current_time = F.current_timestamp()

    df_with_metadata = (
        decoded_df.withColumn("processing_timestamp", current_time)
        .withColumn("pipeline_version", F.lit("v1.0"))
        .withColumn("config_version", F.lit(config.get("version", "unknown")))
    )

    # Save the main table
    print(f"Saving decoded results to {table_name}...")
    df_with_metadata.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(table_name)

    # Create and save summary table
    summary = get_decoding_summary(decoded_df)
    summary_df = spark.createDataFrame([summary])
    summary_table = f"{table_name}_summary"

    print(f"Saving summary to {summary_table}...")
    summary_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        summary_table
    )

    print(f"Results saved successfully!")
    print(f"Main table: {table_name} ({decoded_df.count()} rows)")
    print(f"Summary table: {summary_table}")


def process_families_in_parallel(
    df, family_year_combinations, spark, config, max_parallelism=3
):
    """
    Process families in parallel for better performance.
    Note: This is a conceptual function - actual parallel processing in Spark
    would require careful resource management.

    Args:
        df: Input DataFrame
        family_year_combinations: List of family-year tuples
        spark: Spark session
        config: Configuration dictionary
        max_parallelism: Maximum number of families to process simultaneously

    Returns:
        DataFrame: Combined results
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_single_family_wrapper(fam_yr):
        """Wrapper function for parallel processing"""
        try:
            return decode_single_family(df, fam_yr, spark, config)
        except Exception as e:
            print(f"Error processing {fam_yr}: {e}")
            return None

    decoded_dataframes = []

    # Process families in parallel
    with ThreadPoolExecutor(max_workers=max_parallelism) as executor:
        # Submit all tasks
        future_to_family = {
            executor.submit(process_single_family_wrapper, fam_yr): fam_yr
            for fam_yr in family_year_combinations
        }

        # Collect results as they complete
        for future in as_completed(future_to_family):
            fam_yr = future_to_family[future]
            try:
                result = future.result()
                if result is not None:
                    decoded_dataframes.append(result)
                    print(f"✓ Completed {fam_yr}")
                else:
                    print(f"✗ Failed {fam_yr}")
            except Exception as e:
                print(f"✗ Exception in {fam_yr}: {e}")

    # Union all results
    if decoded_dataframes:
        final_df = decoded_dataframes[0]
        for df_to_union in decoded_dataframes[1:]:
            final_df = final_df.unionByName(df_to_union, allowMissingColumns=True)
        return final_df
    else:
        return None


def create_decoding_report(decoded_df, output_path=None):
    """
    Create a detailed report of the decoding process.

    Args:
        decoded_df: Decoded DataFrame
        output_path: Optional path to save the report

    Returns:
        str: Report content
    """
    if decoded_df is None:
        return "No decoded dataframe provided for reporting."

    summary = get_decoding_summary(decoded_df)

    report = f"""
    # DDT Decoding Report
    Generated: {pd.Timestamp.now()}

    ## Summary
    - Total Rows: {summary['total_rows']:,}
    - Total Columns: {summary['total_columns']}
    - Decoded JSON Columns: {summary['decoded_json_columns']}
    - Flattened Data Columns: {summary['flattened_data_columns']}

    ## Family Breakdown
    """

    for family, count in summary["families"].items():
        report += f"- {family}: {count:,} rows\n"

    if summary["errors"]:
        report += "\n## Errors Found\n"
        for error in summary["errors"]:
            report += f"- {error['column']}: {error['error_count']} errors\n"
    else:
        report += "\n## No Errors Found ✓\n"

    report += f"""
    ## Data Quality
    - Error Rate: {len(summary['errors'])}/{summary['decoded_json_columns']} columns have errors
    - Success Rate: {((summary['decoded_json_columns'] - len(summary['errors'])) / summary['decoded_json_columns'] * 100):.1f}%
    """

    if output_path:
        with open(output_path, "w") as f:
            f.write(report)
        print(f"Report saved to {output_path}")

    return report


def optimized_flatten_json(df, diag_alrt, json_column_name, column_types=None, 
                          fam_var=None, my_var=None):
    """
    Flattens a JSON column using Spark's native JSON functions.
    Casts columns to specified data types from a dictionary.
    
    Args:
        df: DataFrame containing the JSON column
        diag_alrt: The diagnostic alert type (e.g., "A001", "A007")
        json_column_name: Name of the column containing the JSON strings
        column_types: Dictionary mapping column names to data types
                     (e.g., {'Engine_Speed': 'int', 'Vehicle_Status': 'string'})
                     Supported types: 'string', 'int', 'float', 'boolean'
        
    Returns:
        DataFrame with flattened columns cast to specified types
    """
    
    from pyspark.sql.functions import col, length, when, get_json_object, regexp_extract
    import pyspark.sql.functions as F
    from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType
    from helpers.alert_string_lengths import alrt_len
    import json
    
    # Initialize column_types if not provided
    if column_types is None:
        column_types = {}
        
    # Map string type names to PySpark types
    type_mapping = {
        'string': StringType(),
        'str': StringType(),
        'int': IntegerType(),
        'integer': IntegerType(),
        'float': FloatType(),
        'double': FloatType(),
        'bool': BooleanType(),
        'boolean': BooleanType()
    }

    # Build the query string dynamically
    # Check if alrt_len is properly defined and accessible
    try:
        expected_length = alrt_len[fam_var][my_var][diag_alrt]
        query_string = f'(length(col("BCM_{diag_alrt}")) == {expected_length})'
    except (NameError, KeyError) as e:
        import sys
        print(f"Error: Could not find expected length for {fam_var}/{my_var}/{diag_alrt}.")
        sys.exit(1)
    
    # Get a sample row with valid data
    df_good_row = df.filter(eval(query_string)).limit(1).collect()
    
    # This will store description -> position mapping
    description_positions = {}
    json_structure = None
    
    # Check if we got any rows and if the column exists
    if df_good_row and json_column_name in df_good_row[0].asDict():
        try:
            # Get the JSON string from the first (and only) row
            json_data = df_good_row[0][json_column_name]
            if json_data:  # Check if the JSON data is not null
                data = json.loads(json_data)
                
                # Determine the JSON structure
                if isinstance(data, dict) and "decoded_values" in data:
                    decoded_values = data["decoded_values"]
                    json_structure = "nested"
                elif isinstance(data, list):
                    decoded_values = data
                    json_structure = "array"
                else:
                    print(f"Unexpected JSON structure: {type(data)}")
                    decoded_values = []
                    
                # Extract descriptions WITH their positions
                for idx, entry in enumerate(decoded_values):
                    if isinstance(entry, dict):
                        desc = entry.get("description")
                        if desc and desc != "Reserved":
                            # Store the actual position of this description in the JSON array
                            description_positions[desc] = {
                                'position': idx,
                                'unit': entry.get("units", "")  # Note: "units" not "unit"
                            }
                            
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            print(f"JSON data: {json_data[:200] if json_data else 'None'}")  # Print first 200 chars

    if not description_positions:
        print(f"Warning: No descriptions found for {diag_alrt}. Check the JSON structure.")
        # Return original dataframe with the goodDiagInput column
        return df.withColumn(
            f"goodDiagInput_{diag_alrt}",
            F.when(eval(query_string), "x").otherwise("")
        )

    # Create a clean column name function
    def clean_column_name(name):
        import re
        base_name = re.sub(r'[^a-zA-Z0-9_]', '_', name).replace('__', '_')
        return base_name

    # Start with the original dataframe
    result_df = df

    # For each description, create a new column using its ACTUAL position
    print(f"Extracting {len(description_positions)} fields from {diag_alrt} data...")
    
    for desc, info in sorted(description_positions.items()):
        clean_desc = clean_column_name(desc)
        position = info['position']  # Use the actual position from the JSON
        unit = info['unit']
        
        # Create extraction based on the JSON structure and actual position
        if json_structure == "nested":
            # For nested structure: {"decoded_values": [...]}
            expr_str = f"get_json_object({json_column_name}, '$.decoded_values[{position}].decoded_value')"
        else:
            # For array structure: [...]
            expr_str = f"get_json_object({json_column_name}, '$[{position}].decoded_value')"
        
        # Add the new column to the DataFrame
        result_df = result_df.withColumn(clean_desc, F.expr(expr_str))
        
        # Cast the column to the specified type if provided
        column_type = None
        for pattern, dtype in column_types.items():
            if pattern == clean_desc or (
                    pattern.endswith('*') and clean_desc.startswith(pattern[:-1])
            ):
                column_type = dtype
                break
                
        if column_type and column_type.lower() in type_mapping:
            # Create safe casting with null handling
            result_df = result_df.withColumn(
                clean_desc,
                F.when(col(clean_desc).isNull(), None)
                .otherwise(col(clean_desc).cast(type_mapping[column_type.lower()]))
            )

    # Add the goodDiagInput column
    result_df = result_df.withColumn(
        f"goodDiagInput_{diag_alrt}",
        F.when(eval(query_string), "x").otherwise("")
    )
    
    return result_df

