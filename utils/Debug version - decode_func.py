import pandas as pd
import json
import logging
import re
import logging
# logging.basicConfig(level=logging.DEBUG)

ddt_string = '622947101010FF0E10FF1010FF24100E1008FFFFFF0E10'
fam_var = "DT"
my_var = "2025"
msg = "2947"

dir_path = f"/Workspace/Users/richard.sajdak@stellantis.com/dsci_12V_battery_ddt_decode/conversions/{fam_var}/{my_var}/"
conv_tbl_L1  = f'conv_L_BCM_{fam_var}_{my_var}.csv'
conv_tbl_T1 = f'conv_T_BCM_{fam_var}_{my_var}.csv'
DDT_file = f'DDT_BCM_{msg}_{fam_var}_{my_var}.json'
save_path = f'gadp_scratch.rich.{fam_var.lower()}'

conv_tbl_L = pd.read_csv(dir_path + conv_tbl_L1)
conv_tbl_T = pd.read_csv(dir_path + conv_tbl_T1)


# Ensure ID columns are integers for consistent comparison
if "ID" in conv_tbl_L.columns:
    conv_tbl_L["ID"] = conv_tbl_L["ID"].astype(int)
if "Decimal Places" in conv_tbl_L.columns:
    conv_tbl_L["Decimal Places"] = (
        conv_tbl_L["Decimal Places"]
        .replace([float('inf'), float('-inf')], 0)
        .fillna(0)
        .astype(int)
    )

if "ID" in conv_tbl_T.columns:
    conv_tbl_T["ID"] = conv_tbl_T["ID"].astype(int)

print(f"\n\nlinear table datatypes: \n{conv_tbl_L.dtypes}\n")
print(f"table datatypes: \n{conv_tbl_T.dtypes}")
json_structure = json.loads(open(dir_path + DDT_file).read())

#===================================
def extract_bit_field(ddt_string, byte_pos, bit_pos, byte_size, bit_size):
    """
    Extract a bit field from the DDT string using the correct interpretation.
    
    Key insights:
    1. When byte_size = 0 and bit_size > 0, extract bits from current byte
    2. When byte_size > 0 and bit_size = 0, extract complete bytes
    3. When byte_size > 0 and bit_size > 0, it's a multi-byte field with extra bits (backward expansion)
    
    Args:
        ddt_string: Hex string
        byte_pos: Starting byte position 
        bit_pos: Starting bit within byte_pos
        byte_size: Number of complete bytes
        bit_size: Number of additional bits
    """
    
    if byte_size == 0 and bit_size > 0:
        # Extract specific bits from a single byte
        return extract_bits_from_byte(ddt_string, byte_pos, bit_pos, bit_size)
    
    elif byte_size > 0 and bit_size == 0:
        # Extract complete bytes
        # return extract_consecutive_bytes(ddt_string, byte_pos, byte_size, bit_pos)
        return extract_whole_byte(ddt_string, byte_pos, byte_size, bit_pos)
    
    elif byte_size > 0 and bit_size > 0:
        # Complex case: backward expansion required
        # Extract main bytes (starting from byte_pos)
        main_value = extract_consecutive_bytes(ddt_string, byte_pos, byte_size, bit_pos)
        
        # Extract additional bits from previous bytes (backward expansion)
        additional_bits = extract_backward_bits(ddt_string, byte_pos, bit_size)
        
        # Combine: additional bits go in high-order positions
        result = main_value | (additional_bits << (byte_size * 8))
        return result
    
    else:
        # byte_size = 0 and bit_size = 0, extract single byte
        return extract_consecutive_bytes(ddt_string, byte_pos, 1, bit_pos)

def extract_bits_from_byte(ddt_string, byte_pos, bit_start, num_bits):
    """Extract specific bits from a single byte.
    
    Args:
        ddt_string: Hex DDT string
        byte_pos: Byte position
        bit_start: Starting bit position (0-7)
        num_bits: Number of bits to extract
    """
    if byte_pos * 2 + 2 > len(ddt_string):
        raise ValueError(f"Byte {byte_pos} not available in DDT string")
    
    if bit_start < 0 or bit_start > 7:
        raise ValueError(f"Invalid bit_start {bit_start}, must be 0-7")
    
    if num_bits <= 0 or bit_start + num_bits > 8:
        raise ValueError(f"Invalid bit range: start={bit_start}, count={num_bits}")
    
    hex_byte = ddt_string[byte_pos*2:(byte_pos+1)*2]
    byte_value = int(hex_byte, 16)
    
    # Extract bits [bit_start : bit_start + num_bits]
    shifted = byte_value >> bit_start
    mask = (1 << num_bits) - 1
    return shifted & mask

def extract_whole_byte(ddt_string, byte_pos, byte_size, bit_pos):
    """Extract a whole byte from the DDT string.
    
    Args:
        ddt_string: Hex DDT string
        byte_pos: Byte position
        byte_size: Number of bytes to extract
        bit_pos: Bit position within the first byte (default 0)
    """
    if byte_pos * 2 + 2 > len(ddt_string):
        raise ValueError(f"Byte {byte_pos} not available in DDT string")
    if bit_pos < 0 or bit_pos > 7:
        raise ValueError(f"Invalid bit_pos {bit_pos}, must be 0-7")
    
    hex_byte = ddt_string[byte_pos*2:(byte_pos+byte_size)*2]
    value = int(hex_byte, 16)
    return value

def extract_consecutive_bytes(ddt_string, byte_pos, num_bytes, bit_offset=0):
    """Extract value from consecutive bytes with optional bit offset.
    
    This function implements little-endian byte order interpretation:
    - First byte (lowest address) contains least significant bits
    - Second byte contains next 8 bits, etc.
    
    Args:
        ddt_string: Hex DDT string
        byte_pos: Starting byte position
        num_bytes: Number of consecutive bytes to extract
        bit_offset: Bit offset within the starting byte (default 0)
        
    Returns:
        Integer value in little-endian interpretation
    """
    # Input validation
    if byte_pos < 0:
        raise ValueError(f"Invalid byte_pos {byte_pos}, must be >= 0")
    
    if num_bytes <= 0:
        raise ValueError(f"Invalid num_bytes {num_bytes}, must be > 0")
    
    if bit_offset < 0 or bit_offset > 7:
        raise ValueError(f"Invalid bit_offset {bit_offset}, must be 0-7")
    
    # Check if we have enough data in the DDT string
    # Each byte requires 2 hex characters, so we need (byte_pos + num_bytes) * 2 characters total
    required_length = (byte_pos + num_bytes) * 2
    if required_length > len(ddt_string):
        available_bytes = len(ddt_string) // 2
        raise ValueError(f"Insufficient data: need bytes {byte_pos} to {byte_pos + num_bytes - 1}, but only {available_bytes} bytes available in DDT string")
    
    # Extract consecutive bytes in little-endian order
    value = 0
    for i in range(num_bytes):
        # Calculate hex string positions for this byte
        hex_start = (byte_pos + i) * 2
        hex_end = hex_start + 2
        
        # Extract the 2-character hex representation of this byte
        byte_hex = ddt_string[hex_start:hex_end]
        byte_val = int(byte_hex, 16)
        
        # Apply little-endian byte order:
        # - First byte (i=0) goes in bits 0-7 (LSB)
        # - Second byte (i=1) goes in bits 8-15
        # - Third byte (i=2) goes in bits 16-23, etc.
        value |= (byte_val << (i * 8))
    
    # Apply bit offset if needed (for multi-byte bit fields)
    if bit_offset > 0:
        # When there's a bit offset, we may need additional bits from the next byte
        # to maintain the correct total bit count after shifting
        next_byte_pos = byte_pos + num_bytes
        if next_byte_pos * 2 + 2 <= len(ddt_string):
            # Read the next byte to get additional bits
            next_hex = ddt_string[next_byte_pos*2:(next_byte_pos+1)*2]
            next_byte_val = int(next_hex, 16)
            
            # Extract the low-order bits from the next byte
            additional_bits = next_byte_val & ((1 << bit_offset) - 1)
            # Add these bits to the high-order positions
            value |= (additional_bits << (num_bytes * 8))
        
        # Shift right to apply the bit offset
        value = value >> bit_offset
        
        # Mask to ensure we only keep the correct number of bits
        total_bits = num_bytes * 8
        mask = (1 << total_bits) - 1
        value = value & mask
    
    return value

def extract_backward_bits(ddt_string, byte_pos, num_bits):
    """
    Extract additional bits from bytes before byte_pos (backward expansion).
    
    For the "1 [4]" case at byte 57:
    - Need 4 additional bits
    - These come from byte 56, specifically the unused bits
    - Based on structure: byte 56 [4] uses bits 4-7, so bits 0-3 are available
    """
    
    if num_bits == 0:
        return 0
    
    # For now, implement the most common case: taking low-order bits from previous byte
    prev_byte = byte_pos - 1
    
    if prev_byte < 0:
        raise ValueError("Cannot expand backward beyond byte 0")
    
    if prev_byte * 2 + 2 > len(ddt_string):
        raise ValueError(f"Previous byte {prev_byte} not available")
    
    # Extract from previous byte
    hex_byte = ddt_string[prev_byte*2:(prev_byte+1)*2]
    byte_value = int(hex_byte, 16)
    
    # Take the low-order bits (this assumes the high-order bits are used by other fields)
    mask = (1 << num_bits) - 1
    return byte_value & mask

def apply_linear_conversion(raw_value, entry_id, conv_tbl_L):
    """Apply linear conversion to a raw value with special value handling.
    
    Special handling for 0xFFFF and other out-of-range values that are commonly
    used as diagnostic indicators in automotive systems.
    
    Args:
        raw_value: The raw integer value to convert
        entry_id: The ID to match in the conversion table
        conv_tbl_L: DataFrame containing the linear conversion table
    
    Returns:
        tuple: (converted_value, raw_value)
    """
    print(f"raw value data type: {type(raw_value)}")
    print(f"entry_id data type: {type(entry_id)}")
    try:
        # Convert entry_id to int, handling both string and numeric types
        if isinstance(entry_id, str):
            entry_id = int(entry_id)
        elif isinstance(entry_id, float):
            entry_id = int(entry_id)
        elif not isinstance(entry_id, int):
            entry_id = int(entry_id)
            
        matches = conv_tbl_L[conv_tbl_L['ID'] == entry_id]
        if not matches.empty:
            conversion_row = matches.iloc[0]
            
            #====================
            # Check for exact matches with encoding names first
            raw_hex = f"${raw_value:02X}"
            for _, row in matches.iterrows():
                raw_min = str(row["Raw Min"]).strip() if pd.notna(row["Raw Min"]) else None
                raw_max = str(row["Raw Max"]).strip() if pd.notna(row["Raw Max"]) else None
                encoding_name = row["Encoding Name"] if pd.notna(row["Encoding Name"]) else None
                print(f"raw_hex: {raw_hex}, raw_min: {raw_min}, raw_max: {raw_max}, encoding_name: {encoding_name}")

                if encoding_name is not None:
                    # Handle single value case
                    if raw_min == raw_max and raw_min == raw_hex:
                        return encoding_name, raw_value
                    
                    # Handle range case
                    elif raw_min and raw_max and raw_min != raw_max:
                        try:
                            min_val = int(raw_min.replace("$", ""), 16)
                            max_val = int(raw_max.replace("$", ""), 16)
                            if min_val <= raw_value <= max_val:
                                return encoding_name, raw_value
                        except ValueError:
                            continue

            #====================
            # Check for special diagnostic values
            # Get the expected max value from the table
            raw_max_str = str(conversion_row["Raw Max"]).strip() if pd.notna(conversion_row["Raw Max"]) else None
            print(f"Checking raw max for entry {entry_id}: {raw_max_str}")
            if raw_max_str:
                try:
                    # Parse the max value (handle both hex and decimal formats)
                    if raw_max_str.startswith("$"):
                        max_val = int(raw_max_str.replace("$", ""), 16)
                    else:
                        max_val = int(raw_max_str)
                    print(f"Max value: {max_val}")

                    # Special handling for common diagnostic patterns
                    if raw_value == 0xFFFF:  # All bits set - common "invalid" indicator
                        if max_val == 0xFFFE:
                            return "Invalid/Not Available", raw_value
                        elif max_val < 0xFFFF:
                            return "Invalid/Sensor Error", raw_value
                    
                    elif raw_value == 0xFFFE and max_val < 0xFFFE:
                        # Sometimes 0xFFFE is used as a secondary special value
                        return "Not Initialized", raw_value
                    
                    elif raw_value == max_val + 1:
                        # Value is exactly one above maximum - likely a special indicator
                        return "Out of Range/Invalid", raw_value
                    elif raw_value > max_val:
                        return "Out of Range/Invalid", raw_value
                          
                    # Check for other all-bits-set patterns based on field size
                    # For 1-byte fields: 0xFF
                    elif raw_value == 0xFF and max_val == 0xFE:
                        return "Invalid/Not Available", raw_value
                    elif raw_value == 0xFF and max_val < 0xFF:
                            return "Invalid/Sensor Error", raw_value


                    # For 12-bit fields: 0xFFF
                    elif raw_value == 0xFFF and max_val == 0xFFE:
                        return "Invalid/Not Available", raw_value
                    elif raw_value == 0xFFF and max_val < 0xFFF:
                            return "Invalid/Sensor Error", raw_value
                        
                except (ValueError, TypeError):
                    pass  # If we can't parse max value, continue with normal conversion
            
            #====================
            # If no special case or exact match, perform normal linear conversion
            factor = conversion_row['Factor (m)']
            offset = conversion_row['Offset (b)']
            decimal_places = conversion_row['Decimal Places']
            
            converted_value = raw_value * factor + offset
            print(converted_value)
            if decimal_places > 0:
                value = round(converted_value, decimal_places)
            else:
                value = int(converted_value)
                
            return value, raw_value
        else:
            return raw_value, raw_value
    except (ValueError, TypeError, KeyError) as e:
        return f"Conv Error: {str(e)}", raw_value


def apply_table_conversion(raw_value, entry_id, conv_tbl_T, entry):
    """Apply table lookup conversion to a raw value."""
    if entry_id is not None and conv_tbl_T is not None:
        try:
            # Convert entry_id to int
            if isinstance(entry_id, str):
                entry_id = int(entry_id)
            elif isinstance(entry_id, float):
                entry_id = int(entry_id)
            elif not isinstance(entry_id, int):
                entry_id = int(entry_id)
            
            # Build list of hex formats to try
            if raw_value == 0:
                hex_formats = ["$0", "$00"]
            elif raw_value <= 15:  # Single hex digit - try both formats
                hex_formats = [f"${raw_value:X}", f"${raw_value:02X}"]  # e.g., ["$4", "$04"]
            else:
                hex_formats = [f"${raw_value:02X}"]  # e.g., "$20"
            
            for hex_format in hex_formats:
                matches = conv_tbl_T[
                    (conv_tbl_T['ID'] == entry_id) &
                    (conv_tbl_T['Min'].str.strip() == hex_format)
                ]
                if not matches.empty:
                    return matches.iloc[0]['Encoding Name']

            # Fallback logic...
            name = entry.get("name", "")
            if "Present/Not present" in name:
                return "Present" if raw_value == 1 else "Not present"
            elif "Fault/No Fault" in name:
                return "Fault" if raw_value == 1 else "No Fault"
            elif "False / True" in name:
                return "True" if raw_value == 1 else "False"
            else:
                return f"0x{raw_value:X} (No match)"
                
        except (ValueError, TypeError, KeyError) as e:
            return f"0x{raw_value:X} (Conv Error: {str(e)})"
    else:
        return f"0x{raw_value:X}"

def load_conversion_tables(family, model_year, base_dir=None):
    """Load the conversion tables for a specified vehicle family and model year."""
    if base_dir is None:
        base_dir = f"/Workspace/Users/richard.sajdak@stellantis.com/dsci_12V_battery_ddt_decode/conversions/{family}/{model_year}/"
    
    conv_l_file = f"{base_dir}conv_L_BCM_A001_{family}_{model_year}.csv"
    conv_t_file = f"{base_dir}conv_T_BCM_A001_{family}_{model_year}.csv"
    
    try:
        linear_df = pd.read_csv(conv_l_file)
        table_df = pd.read_csv(conv_t_file)
        
        if "ID" in linear_df.columns:
            linear_df["ID"] = linear_df["ID"].astype(str)
        if "ID" in table_df.columns:
            table_df["ID"] = table_df["ID"].astype(str)
        
        return linear_df, table_df
    except Exception as e:
        print(f"Error loading conversion tables: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

#===================================

results = {
    "code": ddt_string,
    "name": json_structure.get("name", "Unknown"),
    "decoded_values": []
}

entries = json_structure.get("entries", [])


for entry in entries:
    byte_info = entry.get("byte", "0 [0]")
    byte_parts = byte_info.split()
    byte_pos = int(byte_parts[0])
    # if byte_info == '12 [0]':
    #     break
    
    if len(byte_parts) > 1 and "[" in byte_parts[1]:
        bit_info = byte_parts[1].strip("[]")
        bit_pos = int(bit_info)
        
    size_info = entry.get("size", "1 [0]")
    size_parts = size_info.split()
    byte_size = int(size_parts[0])
    
    bit_size = 0
    if len(size_parts) > 1 and "[" in size_parts[1]:
        bit_size_info = size_parts[1].strip("[]")
        bit_size = int(bit_size_info)
    
    data_type = entry.get("type", "")
    entry_id = entry.get("id", None)
    
    try:
        # Extract the raw value
        raw_value = extract_bit_field(ddt_string, byte_pos, bit_pos, byte_size, bit_size)
        
        # Apply conversions based on type
        if data_type == "L":
            value = raw_value
            if entry_id is not None and conv_tbl_L is not None:
                print("\nByte position is: ", byte_pos)
                print(f"Raw hex value is: ${raw_value:02X}")
                print(f"Hex --> Dec: {raw_value}")
                print(f"L Table entry_id is: {entry_id}")

                value, decoded_result_raw = apply_linear_conversion(raw_value, entry_id, conv_tbl_L)
                print(value, decoded_result_raw)
        elif data_type == "T":
            value = apply_table_conversion(raw_value, entry_id, conv_tbl_T, entry)
        else:
            value = raw_value
            
    except Exception as e:
        value = f"Error: {str(e)}"
    
    # Build result
    decoded_result = {
        "description": entry.get("description", "Unknown"),
        "decoded_value": value,
        "raw_position": byte_info,
        "size": size_info,
        "msg_type": data_type,
        "msg_id": entry_id
    }
    
    # Add metadata if available
    if data_type == "L" and entry_id is not None and 'decoded_result_raw' in locals():
        decoded_result["raw_value"] = decoded_result_raw
        if conv_tbl_L is not None:
            matches = conv_tbl_L[conv_tbl_L['ID'] == entry_id]
            if not matches.empty and not pd.isna(matches.iloc[0]['Units']):
                decoded_result["units"] = matches.iloc[0]['Units']
    elif data_type == "T" and entry_id is not None and isinstance(value, str) and not value.startswith("Error"):
        decoded_result["raw_value"] = raw_value
            
    results["decoded_values"].append(decoded_result)
        

display(results)

