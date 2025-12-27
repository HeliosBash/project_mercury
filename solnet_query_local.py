from solarnetwork_python.client import Client
import json
import sys
import argparse
from datetime import datetime, timedelta

def solar_query_batch(client, node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret, header_mode='auto', verbose=False):
    """Fetch a single batch of data"""
    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = startdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate = enddate.replace(" ", "T").replace(":", "%3A")

    if aggregate == "None":
        param_str = f"localEndDate={formatted_enddate}&localStartDate={formatted_startdate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}"
    else:
        param_str = f"aggregation={aggregate}&localEndDate={formatted_enddate}&localStartDate={formatted_startdate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}"

    if verbose:
        print(f"API Query: {param_str[:200]}...", file=sys.stderr)
    
    try:
        response = client.solarquery(param_str)
    except Exception as e:
        print(f"API Error: {str(e)}", file=sys.stderr)
        return None, None
    
    if not response or 'results' not in response:
        if verbose:
            print(f"Invalid response or no results field", file=sys.stderr)
        return None, None
    
    return response.get('results', []), response

def discover_columns(results_list):
    """Discover all columns from a list of result sets, preserving order"""
    columns_ordered = []
    seen = set()
    
    for results in results_list:
        if not results:
            continue
        for result in results:
            for key in result.keys():
                if key not in seen:
                    columns_ordered.append(key)
                    seen.add(key)
    
    return columns_ordered

def get_last_timestamp(last_record):
    """Extract the last timestamp from a record, using created field for seconds precision"""
    if not last_record:
        return None
    
    local_date = last_record.get('localDate', '')
    local_time = last_record.get('localTime', '')
    created = last_record.get('created', '')
    
    if not local_date or not local_time:
        return None
    
    # Extract seconds from the created (UTC) timestamp for precision
    seconds = '00'
    if created:
        try:
            # Parse created timestamp (format: 2025-11-26T08:39:23Z)
            created_dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
            seconds = created_dt.strftime('%S')
        except:
            pass
    
    # Build full local timestamp
    if len(local_time) == 5:  # HH:MM format
        local_time += f':{seconds}'
    
    return f"{local_date} {local_time}"

def normalize_datetime(dt_string):
    """Normalize datetime string to 'YYYY-MM-DD HH:MM:SS' format"""
    # Handle URL-encoded format: 2025-12-12T21%3A59 -> 2025-12-12 21:59:00
    dt_string = dt_string.replace('T', ' ').replace('%3A', ':')
    
    # If no seconds, add :00
    parts = dt_string.split(' ')
    if len(parts) == 2:
        date_part, time_part = parts
        time_components = time_part.split(':')
        if len(time_components) == 2:
            dt_string = f"{date_part} {time_part}:00"
    
    return dt_string

def solar_query(node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret, header_mode='auto', output_file=None, verbose=False):
    """Main query function with automatic pagination"""
    client = Client(token, secret)
    
    all_results = []
    
    # Normalize date formats
    current_start = normalize_datetime(startdate)
    final_end = normalize_datetime(enddate)
    
    batch_num = 0
    previous_timestamp = None
    
    # Convert end date to datetime for comparison
    try:
        end_dt = datetime.strptime(final_end, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error: Invalid end date format: {final_end} ({e})", file=sys.stderr)
        return
    
    while True:
        batch_num += 1
        if verbose:
            print(f"Fetching batch {batch_num} from {current_start}...", file=sys.stderr)
        
        results, response = solar_query_batch(client, node, sourceids, current_start, final_end, 
                                               aggregate, maxoutput, token, secret, header_mode, verbose)
        
        if results is None or len(results) == 0:
            if verbose:
                print(f"No more data returned. Total batches: {batch_num - 1}", file=sys.stderr)
            break
        
        if verbose:
            print(f"Retrieved {len(results)} records in batch {batch_num}", file=sys.stderr)
        all_results.append(results)
        
        # Get last timestamp
        last_timestamp = get_last_timestamp(results[-1])
        
        if not last_timestamp:
            if verbose:
                print("Could not extract last timestamp. Stopping.", file=sys.stderr)
            break
        
        if verbose:
            print(f"Last record timestamp: {last_timestamp}", file=sys.stderr)
        
        # Check if we're stuck on the same timestamp
        if last_timestamp == previous_timestamp:
            if verbose:
                print("Same timestamp repeated. Incrementing by 1 minute.", file=sys.stderr)
            try:
                dt = datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')
                dt = dt + timedelta(minutes=1)
                current_start = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                print("Error incrementing timestamp. Stopping.", file=sys.stderr)
                break
        else:
            # Increment by 1 second
            try:
                dt = datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')
                dt = dt + timedelta(seconds=1)
                current_start = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                print("Error incrementing timestamp. Stopping.", file=sys.stderr)
                break
        
        previous_timestamp = last_timestamp
        
        # Check if we've passed the end date
        try:
            current_dt = datetime.strptime(current_start, '%Y-%m-%d %H:%M:%S')
            if current_dt > end_dt:
                if verbose:
                    print(f"Reached end of date range ({current_start} > {final_end})", file=sys.stderr)
                break
        except:
            pass
    
    # Flatten all results
    flattened_results = []
    for results in all_results:
        flattened_results.extend(results)
    
    if not flattened_results:
        if header_mode in ['always', 'auto']:
            output_line = "created,localDate,localTime,nodeId,sourceId"
            if output_file:
                with open(output_file, 'w') as f:
                    f.write(output_line + '\n')
            else:
                print(output_line)
        return
    
    if verbose:
        print(f"Total records collected: {len(flattened_results)}", file=sys.stderr)
    
    # Discover columns from all results
    sorted_columns = discover_columns(all_results)
    
    # Prepare output
    output_lines = []
    
    # Print header based on mode
    if header_mode == 'always' or (header_mode == 'auto' and flattened_results):
        output_lines.append(','.join(sorted_columns))
    
    # Print data rows
    for element in flattened_results:
        row_values = [str(element.get(col, '')) for col in sorted_columns]
        output_lines.append(','.join(row_values))
    
    # Write to file or stdout
    if output_file:
        with open(output_file, 'w') as f:
            f.write('\n'.join(output_lines) + '\n')
        if verbose:
            print(f"Output written to: {output_file}", file=sys.stderr)
    else:
        for line in output_lines:
            print(line)

def main():
    parser = argparse.ArgumentParser(description="Solar query with automatic pagination!")

    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--localstartdate", required=True, type=str, help="Start date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--localenddate", required=True, type=str, help="End date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--aggregate", required=True, help="Aggregation method")
    parser.add_argument("--maxoutput", required=True, help="Maximum output limit per batch")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")
    parser.add_argument("--header", choices=['always', 'never', 'auto'], default='auto', 
                       help="Header mode: always=always print header, never=never print header, auto=print header only if data exists (default: auto)")
    parser.add_argument("--output", "-o", type=str, default=None,
                       help="Output file path. If not specified, outputs to stdout")
    parser.add_argument("--verbose", "-v", action='store_true',
                       help="Enable verbose logging to stderr")

    args = parser.parse_args()

    try:
        solar_query(args.node, args.sourceids, args.localstartdate, args.localenddate, 
                   args.aggregate, args.maxoutput, args.token, args.secret, args.header,
                   args.output, args.verbose)
    except Exception as e:
        print(f"Error: Unsuccessful API Call. {str(e)}", file=sys.stderr)
        print("Please make sure that the token and secret are valid and have access to query the node and source ID, and check if the parameters are in the correct format", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
