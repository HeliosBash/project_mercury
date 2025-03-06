import csv
import re
import decimal
import argparse
from decimal import Decimal

def convert_timestamp(timestamp):
    # Replace 'T' with space and remove 'Z'
    return re.sub(r'T', ' ', re.sub(r'Z', '', timestamp))

def format_number(value):
    # Convert to Decimal to ensure precision
    decimal_value = Decimal(str(value))
    
    # Check if the value is zero
    if decimal_value == 0:
        return "0"
    
    # Limit to 2 decimal places and remove trailing zeros
    formatted = f"{decimal_value:.2f}".rstrip('0').rstrip('.')
    return formatted

def process_irradiance_data(solcast_csv_path, irradiance_data_path, node, sid):
    # Set decimal precision
    decimal.getcontext().prec = 28
    
    a = 0
    ghi_prev = Decimal('0')
    
    with open(solcast_csv_path, 'r') as input_file, open(irradiance_data_path, 'w') as output_file:
        csv_reader = csv.reader(input_file)
        
        # Write header
        output_file.write("node,source,date,irradiance,irradianceHours\n")
        
        for row in csv_reader:
            periodend, periodstart, period, ghi = row
            
            # Format periodstart
            periodstart_formatted = convert_timestamp(periodstart)
            
            # Convert ghi to Decimal for precise calculation
            ghi_decimal = Decimal(ghi)
            
            if a == 0:
                # First record
                output_file.write(f"{node},{sid},{periodstart_formatted},{ghi},{format_number(ghi)}\n")
                ghi_prev = ghi_decimal
                a = 1
            else:
                # Calculate 5-minute irradiance (1/12 of an hour)
                ghi_5min = ghi_decimal / Decimal('12')
                
                # Add to previous value
                ghi_new = ghi_prev + ghi_5min
                
                # Format the output using our custom formatting function
                ghi_new_formatted = format_number(ghi_new)
                
                ghi_prev = ghi_new
                output_file.write(f"{node},{sid},{periodstart_formatted},{ghi},{ghi_new_formatted}\n")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process Solcast irradiance data')
    
    # Add arguments
    parser.add_argument('--solcast_csv_path', required=True, help='Path to the input Solcast CSV file')
    parser.add_argument('--irradiance_data_path', required=True, help='Path to the output irradiance data file')
    parser.add_argument('--node', required=True, help='Node identifier')
    parser.add_argument('--sid', required=True, help='Source identifier')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Process the data
    process_irradiance_data(
        args.solcast_csv_path,
        args.irradiance_data_path,
        args.node,
        args.sid
    )

if __name__ == "__main__":
    main()
