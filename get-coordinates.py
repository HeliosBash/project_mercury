import requests
import re
from urllib.parse import quote
import argparse

def get_coordinates(address: str) -> tuple[float, float] | None:
    """
    Get coordinates from Google Maps URL
    
    Args:
        address: The address to search for
        
    Returns:
        Tuple of (latitude, longitude) if found, None otherwise
    """
    # Format address for URL
    formatted_address = quote(address.replace(' ', '+'))
    
    # Construct Google Maps URL
    url = f"https://www.google.com/maps/place/{formatted_address}"
    
    # Headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    try:
        # Make the request
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Get the content
        content = response.text
        
        # Look for coordinates in the response
        # Google Maps typically includes coordinates in the HTML in various formats
        # Try multiple patterns
        patterns = [
            r'@([-\d.]+),([-\d.]+)',  # Pattern for URL format
            r'\[null,null,(-?\d+\.\d+),(-?\d+\.\d+)\]',  # Pattern for JSON format
            r'latitude":([-\d.]+).*?longitude":([-\d.]+)'  # Pattern for meta tags
        ]
        
        for pattern in patterns:
            matches = re.search(pattern, content)
            if matches:
                lat = float(matches.group(1))
                lng = float(matches.group(2))
                if -90 <= lat <= 90 and -180 <= lng <= 180:  # Validate coordinates
                    return lat, lng
        
        print("Could not find coordinates in the response")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None
    except Exception as e:
        print(f"Error processing response: {e}")
        return None

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Get coordinates from Google Maps URL')
    parser.add_argument('address', type=str, nargs='+', help='The address to geocode')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Join address parts
    address = ' '.join(args.address)
    
    print(f"Searching for: {address}")
    
    # Get coordinates
    result = get_coordinates(address)
    
    if result:
        lat, lon = result
        print(f"\nFound coordinates:")
        print(f"Latitude: {lat}")
        print(f"Longitude: {lon}")
        print(f"\nGoogle Maps link: https://www.google.com/maps?q={lat},{lon}")
    else:
        print("\nCould not find coordinates. Try:")
        print("1. Verifying the address format")
        print("2. Using a more specific address")
        print("3. Adding state/country information")

if __name__ == "__main__":
    main()
