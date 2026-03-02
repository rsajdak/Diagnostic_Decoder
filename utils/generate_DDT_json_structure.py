"""
Script to convert ddt_A001_bcm.csv into a JSON structure for DDT decoding.
This script generates a Python file containing the DDT structure in the requested format.
"""

import csv
import json
import os

def create_ddt_structure_from_csv(csv_file_path, output_file_path):
    """
    Convert a CSV file into a DDT structure JSON and save it to a Python file.
    
    Parameters:
    - csv_file_path: Path to the input CSV file
    - output_file_path: Path to the output Python file
    """
    # Initialize the DDT structure
    file_name = os.path.basename(csv_file_path)

    ddt_structure = {
        "name": file_name,
        "entries": []
    }
    
    # Read the CSV file
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Process each row
        for row in reader:
            # Create an entry dictionary from the CSV row
            entry = {
                "byte": row["ByteBit"],
                "description": row["ByteBit_Description"],
                "size": row["Size"],
                "type": row["Type"],
                "name": row["Name"],
                "id": row["ID"]
            }
            
            # Add default value if it exists
            if "Default_Value" in row and row["Default_Value"]:
                entry["default_value"] = row["Default_Value"]
            
            # Add the entry to the structure
            ddt_structure["entries"].append(entry)
    
    # Convert the DDT structure to a JSON string
    json_content = json.dumps(ddt_structure, indent=2)
    
    # Write the JSON string to the output file
    with open(output_file_path, 'w', encoding='utf-8') as py_file:
        py_file.write(json_content)

    print(f"Successfully converted {csv_file_path} to {output_file_path}")

if __name__ == "__main__":

    alt_ddt = '2947'
    fam_var = "WS"
    my_var = "2025"


    # Define input and output file paths
    dir_path = f"/Workspace/Users/richard.sajdak@stellantis.com/dsci_12V_battery_ddt_decode/conversions/{fam_var}/{my_var}/"

    csv_file_path = f"{dir_path}DDT_BCM_{alt_ddt}_{fam_var}_{my_var}.csv"
    output_file_path = f"{dir_path}DDT_BCM_{alt_ddt}_{fam_var}_{my_var}.json"
    
    # Convert the CSV to a DDT structure
    create_ddt_structure_from_csv(csv_file_path, output_file_path)