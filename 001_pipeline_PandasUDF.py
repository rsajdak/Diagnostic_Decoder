# Databricks notebook source
# Import necessary libraries
import json
import pandas as pd
import os
import time
from pyspark.sql.functions import (
    udf,
    col,
    to_timestamp,
    length,
    to_date,
    regexp_replace,
    date_format,
)
from pathlib import Path
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.window import Window
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.types import StringType
from decode_func import decode_ddt

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

dbutils.widgets.text("target_catalog", "gadp_scratch")
dbutils.widgets.text("target_schema", "heather_goldsby")
dbutils.widgets.text("relative_config_path", "config.json")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("overwrite", "true")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
relative_config_path = dbutils.widgets.get("relative_config_path")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
overwrite = dbutils.widgets.get("overwrite")

# COMMAND ----------

def get_current_directory():
    """Get current directory in a way that works in both scripts and notebooks"""
    try:
        # Try to use __file__ if available (when running as script)
        return Path(__file__).parent.absolute()
    except NameError:
        # Fallback for notebook environments (Databricks, Jupyter, etc.)
        return Path(os.getcwd()).absolute()

# COMMAND ----------

def load_data(config, diagnostic_columns, start_date=None, end_date=None):
    """Load data from the specified table"""

    # Build date filter condition based on provided start and end dates
    if start_date and end_date:
        condition = (F.col("dt") >= start_date) & (F.col("dt") < end_date)
    elif start_date:
        condition = F.col("dt") >= start_date
    elif end_date:
        condition = F.col("dt") < end_date
    else:
        condition = None

    # Read main datastream table and select required columns
    df = spark.read.table(config["input_data"]["ada_datastream"]["path"]).select(
        *config["input_data"]["ada_datastream"]["cols_to_load"],
        *diagnostic_columns,
    )
    # Apply date filter if condition is set
    if condition is not None:
        df = df.filter(condition)

    # Read vehicle data and select relevant columns
    df_vehicle = spark.read.table(config["input_data"]["vehicle_data"]).select(
        "VIN",
        "MODEL_YEAR",
        F.expr("substring(MODEL_CODE, 1, 2)").alias("MODEL_CODE"),
        "MODEL_NAME",
    )

    # Join datastream with vehicle data on VIN
    df = df.join(df_vehicle, on="vin", how="inner")

    # Forward fill specified columns using window function
    w = (
        Window.partitionBy("vin")
        .orderBy("app_timestamp")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    for c in config["input_data"]["ada_datastream"]["cols_to_ffill"]:
        df = df.withColumn(f"{c}_reference", F.last(c, ignorenulls=True).over(w))

    # Convert app_timestamp column to timestamp type
    df = df.withColumn(
        "app_timestamp", to_timestamp(col("app_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    return df
    # except Exception as e:
    #     print(f"Error loading data: {e}")
    #     return None

# COMMAND ----------

def load_config(relative_path, abs_path):
    """Load config and convert relative paths to absolute paths"""

    config_path = os.path.join(abs_path, relative_path)
    with open(config_path, "r") as f:
        config = json.load(f)
    return config

# COMMAND ----------

def load_json_file(family, model_year, ecu, ddt_msg, base_dir=None):
    """
    Load a JSON file for a specific vehicle family, model year, ECU, and DDT message.

    Args:
        family (str): Vehicle family name.
        model_year (str/int): Model year.
        ecu (str): ECU identifier.
        ddt_msg (str): DDT message identifier.
        base_dir (str, optional): Base directory path.

    Returns:
        dict or None: Parsed JSON structure if file exists, else None.
    """

    # Construct the full path to the JSON file
    ddt_filename = f"{base_dir}{family}/{model_year}/DDT_{ecu}_{ddt_msg}_{family}_{model_year}.json"
    print(ddt_filename)

    # Check if file exists
    if not os.path.exists(ddt_filename):
        print("failed...")
        return None

    # Load the JSON structure from file
    with open(ddt_filename, "r") as file:
        json_structure = json.load(file)
    return json_structure

# COMMAND ----------

def load_conversion_tables(model_code, model_year, config):
    """Load conversion tables for a specific family and year"""

    # Get the absolute conversions path from config
    conversions_path = config["paths"]["conversions"]

    # Construct file names for linear and table conversion CSVs
    conv_L = f"conv_L_BCM_{model_code}_{model_year}.csv"
    conv_T = f"conv_T_BCM_{model_code}_{model_year}.csv"

    # Build full file paths for both conversion tables
    conv_L_path = os.path.join(conversions_path, model_code, str(model_year), conv_L)
    conv_T_path = os.path.join(conversions_path, model_code, str(model_year), conv_T)

    try:
        print("🔄 Trying pandas direct read...")
        # Read table conversion CSV into pandas DataFrame
        conv_tbl_pd_T = pd.read_csv(conv_T_path)
        # Ensure 'ID' column is integer type
        conv_tbl_pd_T['ID'] = conv_tbl_pd_T['ID'].astype(int)
        
        # Read linear conversion CSV into pandas DataFrame
        conv_tbl_pd_L = pd.read_csv(conv_L_path)
        # Ensure 'ID' column is integer type
        conv_tbl_pd_L['ID'] = conv_tbl_pd_L['ID'].astype(int)
        # Replace inf/-inf and NaN in 'Decimal Places' column, then convert to integer
        conv_tbl_pd_L["Decimal Places"] = (
            conv_tbl_pd_L["Decimal Places"]
            .replace([float('inf'), float('-inf')], 0)
            .fillna(0)
            .astype(int)
        )
        return conv_tbl_pd_L, conv_tbl_pd_T

    except Exception as e2:
        # Print error if reading fails and return None for both tables
        print(f"❌ Reading failed also failed: {e2}")
        return None, None

# COMMAND ----------

@F.pandas_udf(returnType=StringType())
def decode_pandas(
    ddt_strings: pd.Series, 
    model_codes: pd.Series, 
    model_years: pd.Series, 
    ddt_msg_codes: pd.Series
) -> pd.Series:
    """
    Vectorized decode function using Pandas UDF.
    Processes entire Series at once for better performance.
    """
    results = []
    conversion_data = b_conversion_data.value  # Access broadcasted conversion data
    
    # Iterate over each row in the input Series
    for ddt_string, model_code, model_year, ddt_msg_code in zip(
        ddt_strings, model_codes, model_years, ddt_msg_codes
    ):
        try:
            # Skip rows with any null values
            if pd.isna(ddt_string) or pd.isna(model_code) or pd.isna(model_year):
                results.append(None)
                continue
                
            key = f"{model_code}_{model_year}"  # Build key for conversion data
            
            # Check if conversion data exists for this key
            if key not in conversion_data:
                results.append(None)
                continue
                
            conv_data = conversion_data[key]
            
            # Check if diagnostic message type exists in conversion data
            if ddt_msg_code not in conv_data:
                results.append(None)
                continue
            
            # Perform the decode using the decode_ddt function
            result = decode_ddt(
                ddt_string, 
                conv_data[ddt_msg_code], 
                conv_data.get("table"), 
                conv_data.get("linear")
            )
            
            # Add metadata to the result
            result["vehicle_family"] = model_code
            result["model_year"] = model_year
            result["ddt_type"] = ddt_msg_code
            
            # Serialize result to JSON string
            results.append(json.dumps(result))
            
        except Exception as e:
            # On error, append None (logging can be added if needed)
            results.append(None)
    
    return pd.Series(results)  # Return results as a Pandas Series

# COMMAND ----------

@F.udf(StringType())
def decode(ddt_string, model_code, model_year, ddt_msg_code):
    """
    UDF to decode a DDT string using conversion data for a specific model and year.

    Args:
        ddt_string (str): Raw DDT string to decode.
        model_code (str): Vehicle model code.
        model_year (str/int): Model year.
        ddt_msg_code (str): Diagnostic message code.

    Returns:
        str or None: JSON string of decoded result with metadata, or None if decoding fails.
    """
    try:
        # Build key for conversion data lookup
        key = f"{model_code}_{model_year}"
        # Retrieve conversion data from broadcast variable
        conv_data = b_conversion_data.value[key]

        # Decode the DDT string using conversion tables
        result = decode_ddt(
            ddt_string, conv_data[ddt_msg_code], conv_data["table"], conv_data["linear"] 
        )

        # Add metadata to the result
        result["vehicle_family"] = model_code
        result["model_year"] = model_year
        result["ddt_type"] = ddt_msg_code
        # Serialize result to JSON string
        return json.dumps(result)
    except:
        # Return None if any error occurs during decoding
        return None

# COMMAND ----------

def length_lookup(model_name, model_year, diagnostic_message):
    # Retrieve the config dictionary from broadcast variable
    c = b_config.value
    try:
        # Look up the expected length for the given model, year, and diagnostic message
        return c["diagnostic_messages"][diagnostic_message]["versions"][model_name][
            str(model_year)
        ]["length"]
    except KeyError:
        # Return None if any key is missing in the lookup
        return None

# Register the length_lookup function as a Spark UDF returning IntegerType
lookup_udf = F.udf(length_lookup, IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1: Load config. 

# COMMAND ----------

# Determine the absolute path of the current working directory (works in notebooks and scripts)
abs_path = get_current_directory()

# Load the configuration JSON, converting any relative paths to absolute using the base path
config = load_config(relative_config_path, abs_path)

# Build the full path to the conversions directory based on the config settings
conversions_path = os.path.join(abs_path, config["paths"]["conversions"])

# Define the ECU (Electronic Control Unit) type to be used for loading conversion tables
ecu = "BCM"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Find Diagnostic Messages

# COMMAND ----------

# Get diagnostic messages of interest from config
diagnostic_messages = []      # List of diagnostic message keys
diagnostic_columns = []       # List of column names for each diagnostic message
model_name_year_combos = []   # List of (model_name, model_year) tuples for all versions

for key, value in config["diagnostic_messages"].items():
    diagnostic_messages.append(key)                # Add diagnostic message key
    diagnostic_columns.append(value["column_name"])# Add corresponding column name
    for version_key, version_value in value["versions"].items():
        for y in version_value.keys():
            model_name_year_combos.append((version_key, y)) # Add (model_name, model_year) combo

print(f"{diagnostic_columns}, {diagnostic_messages} {model_name_year_combos}")

# COMMAND ----------

# given model name year combos, build a dictionary of inputs
# load all files into memory
conversion_data = {}
for model_name, model_year in model_name_year_combos:
    print(f"\nLoading family: {model_name} {model_year}")
    key = f"{model_name}_{model_year}"
    linear_df, table_df = load_conversion_tables(model_name, model_year, config)
    if linear_df is not None and table_df is not None:
        conversion_data[key] = {}
        conversion_data[key]["linear"] = linear_df
        conversion_data[key]["table"] = table_df

        for d in diagnostic_messages:
            print(f"Loading {d} for {model_name} {model_year} {ecu} {conversions_path}")
            structure_df = load_json_file(
                model_name, model_year, ecu, d, conversions_path
            )
            if structure_df is not None:
                conversion_data[key][d] = structure_df
            else:
                print(f"issue: {conversions_path}")

# COMMAND ----------

import pickle

# Serialize the 'conversion_data' dictionary to bytes using the highest pickle protocol
size_bytes = len(pickle.dumps(conversion_data, protocol=pickle.HIGHEST_PROTOCOL))

# Convert the size from bytes to megabytes
size_mb = size_bytes / (1024 * 1024)

# Print the size of the serialized object in megabytes
print(f"Serialized size: {size_mb:.2f} MB")

# COMMAND ----------

# Broadcast the conversion data dictionary for use in UDFs
b_conversion_data = spark.sparkContext.broadcast(conversion_data)

# Broadcast the config dictionary for use in UDFs
b_config = spark.sparkContext.broadcast(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load data.

# COMMAND ----------

start_date

# COMMAND ----------

df = load_data(config, diagnostic_columns, start_date=start_date, end_date=end_date)

# COMMAND ----------

for key, value in config["diagnostic_messages"].items():
    print(f"Processing diagnostic message: {key}")

    # Calculate actual length of the diagnostic message string
    df = df.withColumn(
        f"{key}_actual_len", 
        F.length(F.col(value["column_name"]))
    )
    # Look up the expected reference length for the diagnostic message using MODEL_CODE and MODEL_YEAR
    df = df.withColumn(
        f"{key}_reference_len",
        lookup_udf(F.col("MODEL_CODE"), F.col("MODEL_YEAR"), F.lit(key))
    )

    # Create a validity flag for this diagnostic message
    # Valid if: value is not null, not "E#1", matches reference length, and reference length is not null
    df = df.withColumn(
        f"{key}_is_valid",
        (F.col(value["column_name"]).isNotNull()) & 
        (F.col(value["column_name"]) != "E#1") &
        (F.col(f"{key}_actual_len") == F.col(f"{key}_reference_len")) &
        (F.col(f"{key}_reference_len").isNotNull())  # Ensure we have a reference length
    )

# Build a list of validity conditions for all diagnostic messages
valid_conditions = [
    F.col(f"{key}_is_valid") for key in config["diagnostic_messages"].keys()
]

# Filter to keep only rows where all diagnostic messages are valid
condition = reduce(lambda a, b: a & b, valid_conditions)
df = df.filter(condition)

# COMMAND ----------

# Repartition the DataFrame into 200 partitions, using MODEL_CODE and MODEL_YEAR as partitioning columns.
# This groups records with the same model and year together in the same partition,
# which can improve performance for downstream operations that use these columns (e.g., UDFs, joins).
# It also helps reduce the number of broadcast variable lookups by keeping related data together.
df = df.repartition(200, "MODEL_CODE", "MODEL_YEAR")

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Apply the UDFs to Decode the Data

# COMMAND ----------

# Enable Apache Arrow for optimized Pandas UDF performance in Spark
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")

# Apply Pandas UDF to decode each diagnostic message
for key, value in config["diagnostic_messages"].items():
    print(f"Decoding {key} using Pandas UDF")
    
    # Only decode when the message is valid (as determined by previous validity checks)
    df = df.withColumn(
        f"{key}_decode",
        F.when(
            F.col(f"{key}_is_valid"),
            decode_pandas(  # Use the Pandas UDF for vectorized decoding
                F.col(value["column_name"]),
                F.col("MODEL_CODE"),
                F.col("MODEL_YEAR"),
                F.lit(key)
            )
        ).otherwise(None)  # Set to None if not valid
    )

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Apply JSON Flattening and Display Results

# COMMAND ----------

from pyspark.sql import types as T

# Define the schema for the decoded JSON column
schema = T.StructType(
    [
        T.StructField(
            "decoded_values",
            T.MapType(T.StringType(), T.MapType(T.StringType(), T.StringType())),
        ),
    ]
)

# Map string type names to PySpark types for casting
type_mapping = {
    "string": T.StringType(),
    "str": T.StringType(),
    "int": T.IntegerType(),
    "integer": T.IntegerType(),
    "float": T.DoubleType(),
    "double": T.DoubleType(),
    "bool": T.BooleanType(),
    "boolean": T.BooleanType(),
}

# For each diagnostic message, parse the JSON and extract/flatten fields
for key, value in config["diagnostic_messages"].items():
    # Parse the JSON string in the decode column into a struct using the defined schema
    df = df.withColumn(f"{key}_parsed", F.from_json(F.col(f"{key}_decode"), schema))
    
    # For each column defined in the diagnostic message, extract and cast the decoded value
    for col_name, col_values in value["columns"].items():
        df = df.withColumn(
            f"{key}_{col_values['clean_name']}",
            F.col(f"{key}_parsed.decoded_values")[col_name]["decoded_value"].cast(
                type_mapping[col_values["type"]]
            ),
        )

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to output. 
# MAGIC _Modify path as needed_

# COMMAND ----------

# Write the DataFrame to a table, overwriting or appending based on the 'overwrite' flag.
if (overwrite == "true") | (overwrite == True) | (overwrite == "True"):
    # Overwrite the table and schema if overwrite is enabled
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(
        f"{target_catalog}.{target_schema}.12v_battery_ibs_decode_dev"
    )
else:
    # Append to the table and merge schema if overwrite is not enabled
    df.write.mode("append").option("mergeSchema", True).saveAsTable(
        f"{target_catalog}.{target_schema}.12v_battery_ibs_decode_dev"
    )

# COMMAND ----------


