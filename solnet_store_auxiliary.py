from solarnetwork_python.client import Client
import argparse
import json

def store_auxiliary(node_id, source_id, aux_type, created, notes, final_data, start_data, meta_data, token, secret):
    """Store or update a datum auxiliary record"""
    
    client = Client(token, secret)
    
    # Build the auxiliary data object
    auxiliary_data = {
        "created": created,
        "nodeId": int(node_id),
        "sourceId": source_id,
        "type": aux_type
    }
    
    # Add optional notes
    if notes:
        auxiliary_data["notes"] = notes
    
    # Parse and add final data
    if final_data:
        auxiliary_data["final"] = json.loads(final_data)
    
    # Parse and add start data
    if start_data:
        auxiliary_data["start"] = json.loads(start_data)
    
    # Parse and add metadata
    if meta_data:
        auxiliary_data["meta"] = json.loads(meta_data)
    
    try:
        resp = client.store_datum_auxiliary(auxiliary_data)
        print("Success! Datum auxiliary record stored.")
        print(json.dumps(resp, indent=2))
    except Exception as e:
        print(f"Error occurred while storing datum auxiliary: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Store datum auxiliary records to Solar Network.")
    parser.add_argument("--node", required=True, type=str, help="Node ID")
    parser.add_argument("--source", required=True, type=str, help="Source ID")
    parser.add_argument("--type", required=True, type=str, help="Datum auxiliary type (e.g., Reset)")
    parser.add_argument("--created", required=True, type=str, 
                       help="Event date in 'yyyy-MM-dd HH:mm:ss.SSS' format or millisecond epoch")
    parser.add_argument("--notes", type=str, default=None, help="Optional notes describing the event")
    parser.add_argument("--final", type=str, required=True, 
                       help='Final data as JSON string, e.g., \'{"a":{"wattHours":123456789}}\'')
    parser.add_argument("--start", type=str, required=True,
                       help='Start data as JSON string, e.g., \'{"a":{"wattHours":123}}\'')
    parser.add_argument("--meta", type=str, default=None,
                       help='Optional metadata as JSON string, e.g., \'{"m":{"foo":"bar"}}\'')
    parser.add_argument("--token", required=True, type=str, help="API token for authentication")
    parser.add_argument("--secret", required=True, type=str, help="API secret for authentication")
    
    args = parser.parse_args()
    
    store_auxiliary(
        args.node, args.source, args.type, args.created, 
        args.notes, args.final, args.start, args.meta,
        args.token, args.secret
    )
