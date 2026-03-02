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
dbutils.widgets.text("target_schema", "rich")
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
    if start_date and end_date:
        condition = (F.col("dt") >= start_date) & (F.col("dt") < end_date)
    elif start_date:
        condition = F.col("dt") >= start_date
    elif end_date:
        condition = F.col("dt") < end_date
    else:
        condition = None

    # try:

    df = spark.read.table(config["input_data"]["ada_datastream"]["path"]).select(
        *config["input_data"]["ada_datastream"]["cols_to_load"],
        *diagnostic_columns,
    )
    if condition is not None:
        df = df.filter(condition)

    df_vehicle = spark.read.table(config["input_data"]["vehicle_data"]).select(
        "VIN",
        "MODEL_YEAR",
        F.expr("substring(MODEL_CODE, 1, 2)").alias("MODEL_CODE"),
        "MODEL_NAME",
    )

    df = df.join(df_vehicle, on="vin", how="inner")

    # for cols in cols to forward fill...
    w = (
        Window.partitionBy("vin")
        .orderBy("app_timestamp")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    for c in config["input_data"]["ada_datastream"]["cols_to_ffill"]:
        df = df.withColumn(f"{c}_reference", F.last(c, ignorenulls=True).over(w))

    # convert to date format
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

    ddt_filename = f"{base_dir}{family}/{model_year}/DDT_{ecu}_{ddt_msg}_{family}_{model_year}.json"
    print(ddt_filename)

    # Check if file exists
    if not os.path.exists(ddt_filename):
        print("failed...")
        return None

    # Load the JSON structure
    with open(ddt_filename, "r") as file:
        json_structure = json.load(file)
    return json_structure

# COMMAND ----------

def load_conversion_tables(model_code, model_year, config):
    """Load conversion tables for a specific family and year"""

    # Get the absolute conversions path
    conversions_path = config["paths"]["conversions"]

    # Construct file paths
    conv_L = f"conv_L_BCM_{model_code}_{model_year}.csv"
    conv_T = f"conv_T_BCM_{model_code}_{model_year}.csv"

    # Build full paths
    conv_L_path = os.path.join(conversions_path, model_code, str(model_year), conv_L)
    conv_T_path = os.path.join(conversions_path, model_code, str(model_year), conv_T)

    try:
        print("🔄 Trying pandas direct read...")
        conv_tbl_pd_T = pd.read_csv(conv_T_path)
        # Convert 'ID' column to integer 
        conv_tbl_pd_T['ID'] = conv_tbl_pd_T['ID'].astype(int)
        

        conv_tbl_pd_L = pd.read_csv(conv_L_path)
        # Convert 'ID' and 'Decimal Places' columns to integer 
        conv_tbl_pd_L['ID'] = conv_tbl_pd_L['ID'].astype(int)
        conv_tbl_pd_L["Decimal Places"] = (conv_tbl_pd_L["Decimal Places"].replace([float('inf'), float('-inf')], 0).fillna(0).astype(int))
        return conv_tbl_pd_L, conv_tbl_pd_T

    except Exception as e2:
        print(f"❌ Reading failed also failed: {e2}")
        return None, None

# COMMAND ----------

@F.udf(StringType())
def decode(ddt_string, model_code, model_year, ddt_msg_code):
    try:
        key = f"{model_code}_{model_year}"
        conv_data = b_conversion_data.value[key]

        result = decode_ddt(
            ddt_string, conv_data[ddt_msg_code], conv_data["table"], conv_data["linear"] 
        )

        result["vehicle_family"] = model_code
        result["model_year"] = model_year
        result["ddt_type"] = ddt_msg_code
        return json.dumps(result)
    except:
        return None

# COMMAND ----------

def length_lookup(model_name, model_year, diagnostic_message):
    c = b_config.value
    try:
        return c["diagnostic_messages"][diagnostic_message]["versions"][model_name][
            str(model_year)
        ]["length"]
    except KeyError:
        return None


lookup_udf = F.udf(length_lookup, IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Load config. 

# COMMAND ----------

abs_path = get_current_directory()
config = load_config(relative_config_path, abs_path)
conversions_path = os.path.join(abs_path, config["paths"]["conversions"])
ecu = "BCM"

# COMMAND ----------

# get diagnostic messages of interest
diagnostic_messages = []
diagnostic_columns = []
model_name_year_combos = []
for key, value in config["diagnostic_messages"].items():
    diagnostic_messages.append(key)
    diagnostic_columns.append(value["column_name"])
    for version_key, version_value in value["versions"].items():
        for y in version_value.keys():
            model_name_year_combos.append((version_key, y))


print(f"{diagnostic_columns}, \n{diagnostic_messages}, \n\n{model_name_year_combos}")

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

size_bytes = len(pickle.dumps(conversion_data, protocol=pickle.HIGHEST_PROTOCOL))
size_mb = size_bytes / (1024 * 1024)

print(f"Serialized size: {size_mb:.2f} MB")

# COMMAND ----------

b_conversion_data = spark.sparkContext.broadcast(conversion_data)
b_config = spark.sparkContext.broadcast(config)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Load data.

# COMMAND ----------

df = load_data(config, diagnostic_columns, start_date=start_date, end_date=end_date)

# COMMAND ----------

# create a filter for each diagnostic column.
condition = reduce(
    lambda a, b: a | b,
    [(F.col(c).isNotNull()) & (F.col(c) != "E#1") for c in diagnostic_columns],
)

df = df.filter(condition)

# COMMAND ----------

# df = df.sample(fraction=0.0005)

# COMMAND ----------

# df.count()

# COMMAND ----------

# display(df)

# COMMAND ----------

for key, value in config["diagnostic_messages"].items():
    print(f"Processing diagnostic message: {key}")

    # Calculate actual length of the diagnostic message
    df = df.withColumn(
        f"{key}_actual_len", 
        F.length(F.col(value["column_name"]))
    )
    # Look up the expected length based on MODEL_CODE and MODEL_YEAR
    df = df.withColumn(
        f"{key}_reference_len",
        lookup_udf(F.col("MODEL_CODE"), F.col("MODEL_YEAR"), F.lit(key))
    )

    # Create a validity flag for this diagnostic message
    # Valid means: not null, not "E#1", and exact length match
    df = df.withColumn(
        f"{key}_is_valid",
        (F.col(value["column_name"]).isNotNull()) & 
        (F.col(value["column_name"]) != "E#1") &
        (F.col(f"{key}_actual_len") == F.col(f"{key}_reference_len")) &
        (F.col(f"{key}_reference_len").isNotNull())  # Ensure we have a reference length
    )

# Create a filter condition that checks if at least one diagnostic message is valid
valid_conditions = [
    F.col(f"{key}_is_valid") for key in config["diagnostic_messages"].keys()
]

# Filter to keep only rows where all diagnostic messages has valid data
condition = reduce(lambda a, b: a & b, valid_conditions)
df = df.filter(condition)

# COMMAND ----------

df_dt = df.filter(F.col("MODEL_CODE") == "DT").limit(100)
df_ws = df.filter(F.col("MODEL_CODE") == "WS").limit(100)
df = df_dt.unionByName(df_ws)


# COMMAND ----------

display(df)

# COMMAND ----------

# Repartition by model / year
df = df.repartition(200, "MODEL_CODE", "MODEL_YEAR")

# This ensures records with same model are processed together
# Reduces broadcast variable lookups

# COMMAND ----------


# df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Decode

# COMMAND ----------

for key, value in config["diagnostic_messages"].items():
    print(f"Decoding {key}")
    # df = df.withColumn(f"{key}_actual_len", F.length(F.col(value["column_name"])))

    # # look up desired length
    # df = df.withColumn(
    #     f"{key}_reference_len",
    #     lookup_udf(F.col("MODEL_CODE"), F.col("MODEL_YEAR"), F.lit(key)),
    # )
    # Only decode when the message is valid (already checked above)
    df = df.withColumn(
        f"{key}_decode",
        F.when(
            F.col(f"{key}_is_valid"),
            decode(
                F.col(value["column_name"]),
                F.col("MODEL_CODE"),
                F.col("MODEL_YEAR"),
                F.lit(key)
            )
        ).otherwise(None)  # Explicitly set to None if not valid
    )

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Add json columns

# COMMAND ----------

from pyspark.sql import types as T

schema = T.StructType(
    [
        T.StructField(
            "decoded_values",
            T.MapType(T.StringType(), T.MapType(T.StringType(), T.StringType())),
        ),
    ]
)



# Map string type names to PySpark types
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

for key, value in config["diagnostic_messages"].items():
    df = df.withColumn(f"{key}_parsed", F.from_json(F.col(f"{key}_decode"), schema))
    
    for col_name, col_values in value["columns"].items():
        df = df.withColumn(
            f"{key}_{col_values['clean_name']}",
            F.col(f"{key}_parsed.decoded_values")[col_name]["decoded_value"].cast(
                type_mapping[col_values["type"]]
            ),
        )


# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Save

# COMMAND ----------

if (overwrite == "true") | (overwrite == True) | (overwrite == "True"):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(
        f"{target_catalog}.{target_schema}.12v_battery_ibs_decode_company_dt_ws"
    )
else:
    df.write.mode("append").option("mergeSchema", True).saveAsTable(
        f"{target_catalog}.{target_schema}.12v_b.12v_battery_ibs_decode_company_dt"
    )

# COMMAND ----------


