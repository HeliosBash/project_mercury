# SOLNET AUTOMATION SCRIPTS

## REQUIREMENTS

- [ ] Solcast API Token from https://toolkit.solcast.com.au/
- [ ] Solnet User Token and Secret that has sufficient access to the Node and Source ID
- [ ] Site Latitude and Longitude Coordinates from Ecosuite
- [ ] Site Local Timezone from Ecosuite

## SOLNET GAP FILLER

- [ ] Identifies PYR/GEN gaps and fills the gap with irradiance data or electrical energy data driven by irradiance data, calculated from data downloaded from solcast.

- [ ] Usage:		
	```
	solnet-gap-filler [--help|-h] --node|-n [NODE] --sourceid|-i [SOURCE ID] --startdate|-s [START DATETIME in 'YYYY-MM-DD HH:MM'] --enddatetime|-e [END DATETIME in 'YYYY-MM-DD HH:MM'] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
	```

- [ ] Example: 	
	```
	/bin/bash solnet-gap-filler --node 379 --sourceid /VI/SU/B2/PYR/1 --startdate '2021-08-31 00:00' --enddate '2021-09-03 00:00' --latitude 18.343015 --longitude -64.911997 --api solcasttoken --token solnettoken --secret solnetsecret
	```

## SOLNET DATA FILLER

- [ ] It has a similar process as the solnet-gap-filler but it doesn't identify gaps. Applicable for date ranges that are known to have no data or cases wherein you will need to fill in data from start of production. Make sure to round up the date range to the next 5 minute interval for the startdatetime and add some future offset to the enddatetime but should be less than the gap boundary. 

- [ ] Usage:
	```
	solnet-filler --node|-n [NODE] --sourceid|-i [SOURCEID] --timezone|-z [TIMEZONE] --startdate|-s [STARTDATETIME in 'YYYY-MM-DD HH:MM:SS'] --enddatetime|-e [ENDDATETIME in 'YYYY-MM-DD HH:MM:SS'] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --energyspike|-g [Energy Spike in watthours ; 0 for PYR Fills] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
	```

- [ ] In this example The gap is between 2 boundaries 2023-10-04 19:57:23 and 2023-10-17 00:03:00. For the 'startdatetime', we round up to the nearest 5 minute interval which is 20:00:00. For the enddatetime, we want the last data entry to be '2023-10-17 00:00:00' so we add some future offset like a minute '2023-10-17 00:01:00' which needs to be less than the gap boundary '2023-10-17 00:03:00'.
	```
	/bin/bash solnet-filler --node 372 --sourceid /G2/S2/S1/PYR/2 --startdatetime '2023-10-04 20:00:00' --enddatetime '2023-10-17 00:01:00' --latitude 29.658884 --longitude -82.334525 --energyspike 0 --api solcasttoken --token solnettoken --secret solnetsecret
	```

## BAD DATA DELETER
		
- [ ] Scans for bad data, wherein the values of watts and watthours are 0, for a given year and month and processes its deletion. It does an hourly scan which is found to be effective. 

- [ ] Usage:          
	```
	ee-bad-data-deleter --node [nodeid] --sourceid [GEN sourceids] --month [month in MM format] --year [year in YYYY format] --token [solnet_token] --secret [solnet_secret]
	```

- [ ] Example:       
	```
	/bin/bash ee-bad-data-deleter --node 372 --sourceid /G2/S2/S1/GEN/1 --month 06 --year 2024 --token solnettoken --secret solnetsecret
	```

# PYTHON SCRIPTS FOR SOLARNETWORK API

## Solnet Query 

### Solnet Query UTC
		
- [ ] Lists PYR/GEN data based on a given date range in UTC, sorts, and prints wattHours and irradianceHours at the end
		
- [ ] Usage:		
	```
	solnet_query_utc.py --node=[nodeid] --sourceids=[sourceids] --startdate=[UTC start datetime in 'YYYY-MM-DD HH:MM'] --enddate=[UTC end datetime in 'YYYY-MM-DD HH:MM'] --aggregate=[Day|Hour|FiveMinute|None] --maxoutput=[Max Output] --token=[solnet token] --secret=[solnet secret]
	```
		
- [ ] Example:
	```
	python3 solnet_query_utc.py --node="372" --sourceids="/G2/S2/S1/PYR/1" --startdate="2024-05-01 00:00" --enddate="2024-06-31 23:59" --aggregate="Day" --maxoutput="1000000" --token="ABCD1234" --secret="WXYZ7890"
	```

### Solnet Query Local

- [ ] Lists PYR/GEN data based on a given date range in Local timezone
                
- [ ] Usage:          
	```
	solnet_query_local.py --node=[nodeid] --sourceids=[sourceids] --localstartdate=[Local start datetime in 'YYYY-MM-DD HH:MM'] --localenddate=[Local end datetime in 'YYYY-MM-DD HH:MM'] --aggregate=[Day|Hour|FiveMinute|None] --maxoutput=[Max Output] --token=[solnet token] --secret=[solnet secret] --output=[filename] --verbose
	```                

- [ ] Example:        
	```
	python3 solnet_query_local.py --node="372" --sourceids="/G2/S2/S1/PYR/1" --localstartdate="2024-05-01 00:00" --localenddate="2024-06-31 23:59" --aggregate="Day" --maxoutput="1000000" --token="ABCD1234" --secret="WXYZ7890" --output="/tmp/file.csv" --verbose
	```

## Solnet Expire Datum

- [ ] Has two functions, provides a preview on the number of datum to be deleted in a given date range and deletes datum in a given date range. Date range should be the LOCAL date and time.
		
- [ ] Usage: 		
	```
	solnet_expire.py --action=[preview/confirm] --node=[nodeid] --sourceids=[sourceids] --localstartdate=[LOCAL start datetime in 'YYYY-MM-DD HH:MM'] --localenddatetime=[LOCAL end date in 'YYYY-MM-DD HH:MM'] --token=[solnet token] --secret=[solnet secret]
	```

- [ ] Example:	
	```
	python3 solnet_expire.py --action="preview"  --node="372" --sourceids="/G2/S2/S1/PYR/1" --localstartdate="2024-05-09 19:00:30" --localenddate="2024-06-10 20:59:30" --token="ABCD1234" --secret="WXYZ7890"

	python3 solnet_expire.py --action="confirm"  --node="372" --sourceids="/G2/S2/S1/PYR/1" --localstartdate="2024-05-09 19:00:30" --localenddate="2024-06-10 20:59:30" --token="ABCD1234" --secret="WXYZ7890"
	```
	
## Solnet Import

- [ ] Imports staged data.
		
- [ ] Usage:		
	```
	solnet_import.py --node=[nodeid] --sourceids=[sourceids] --timezone=[must be set to UTC] --compression=[ enabled or disabled ] --filepath=[path of csv file with PYR or EE data] --token=[solnet token] -secret=[solnet secret]
	```		

- [ ] Example: 	
	```
	python3 solnet_import.py --node="372" --sourceids="/G2/S2/S1/PYR/1" --timezone="UTC" --compression="disabled" --filepath="data/372_%2FG2%2FS2%2FS1%2FPYR%2F1_PYRGAP_SolNetIMport_20240915_120914.csv" --token="ABCD1234" --secret="WXYZ7890"
	```

## Solcast Download
		
- [ ]	Downloads solcast data for a given GPS coordinate and date range. The maximum date range that can be set is 30 days.

- [ ] 	Usage:
	```
	python3 solcast_download.py --latitude=[latitude] --longitude=[longitude] --startdate=[UTC start datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --enddate=[UTC end datetime in 'YYYY-MM-DD HH:MM:SS.000Z' ] --token=[solcast_api_token]
	```

- [ ] 	Example:	
	```
	# python3 solcast_download.py --latitude="41.7287427" --longitude="-71.2782335" --startdate="2024-11-07 22:13:40.000Z" --enddate="2024-11-15 02:48:14.000Z" --token="foobar"
	2024-11-07T22:15:00Z,2024-11-07T22:10:00Z,PT5M,0
	2024-11-07T22:20:00Z,2024-11-07T22:15:00Z,PT5M,0
	2024-11-07T22:25:00Z,2024-11-07T22:20:00Z,PT5M,0
	2024-11-07T22:30:00Z,2024-11-07T22:25:00Z,PT5M,0

	```

## Get Auxiliary Records

- [ ]   Retrieves Auxiliary Records

- [ ]   Usage:
        ```
        python3 solnet_get_auxiliary.py --node=[nodeid] --sourceids=[sourceid] --startdate=[UTC start datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --enddate=[UTC end datetime in 'YYYY-MM-DD HH:MM:SS.000Z' ] --token="foo" --secret="foobar"
        ```

- [ ]   Example:
	```
	#  python3 solnet_get_auxiliary.py --node="672" --sourceids="/CT13/OJBF/R1/GEN/1" --startdate="2024-06-01 00:00:00" --enddate="2024-07-30 23:59:59" --token="foo" --secret="foobar"
	== Reset Data Summary ==
	Total Results: 2
	Reset Events: 2
	--- Reset Event 1 ---
	Created: 2024-06-27 03:59:38Z
	Node ID: 672
	Source ID: /CT13/OJBF/R1/GEN/1
	Local Date/Time: 2024-06-26 23:59:38
	Watt Hours: 0 → 726000
	Cause: Discontinuity due to Gap
	Description: Discontinuity due to Gap
	User: Data Support

	--- Reset Event 2 ---
	Created: 2024-07-19 03:55:05Z
	Node ID: 672
	Source ID: /CT13/OJBF/R1/GEN/1
	Local Date/Time: 2024-07-18 23:55:05
	Watt Hours: 0 → 34448000
	Cause: Discontinuity due to Gap
	Description: Discontinuity due to Gap
	User: Data Support

	```

## Store Auxiliary Records

- [ ] Store Auxiliary Records

- [ ] Usage:
        ```
        python3 solnet_store_auxiliary.py --node [nodeid] --source [sourceid] --type RESET --created [UTC datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --notes [description] --final [final watthours before created date] --start [starting watthours after created date]  --token "foo" --secret "foobar"
	```

- [ ] Example:
	```
	python3 solnet_store_auxiliary.py --node 465 --source "/NYVIII/ARCAD/RT1/PYR/2" --type "Reset" --created "2025-11-11 20:34:55Z" --notes "Discontinuity due to Gap - End Border" --final '{"a":{"wattHours":14308.58}}' --start '{"a":{"wattHours":4322142.833299}}' --token "foo" --secret "foobar"
	```

## Delete Auxiliary Records

- [ ] Delete Auxiliary Records. The event date is the UTC date which is the date before the actual fill but after the date where the data ends for the date after the fill but before the date where data resumes. 

- [ ] Usage:
        ```
        python3 solnet_delete_auxiliary.py --node [nodeid] --source [sourceid] --eventdate [UTC datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --token "foo" --secret "foobar"
        ```

- [ ] Example:
        ```
        python3 solnet_delete_auxiliary.py --node 465 --source "/NYVIII/ARCAD/RT1/PYR/2" --eventdate "2025-11-11 20:34:55Z"  --token "foo" --secret "foobar"
        ```


## Solnet Manage Jobs
		
- [ ] 	Lists and delete expire jobs. It also wiews, previews, confirms, and deletes import jobs.   

- [ ]	Usage:		
	```
	python3 solnet_manage_jobs.py [-h] [--job="{expire,import}"] [--action="{list,view,preview,delete,confirm}"] --token="TOKEN" --secret="SECRET" --jobid="JOBID"
	```

- [ ]	Example:        
	```
	python3 solnet_manage_jobs.py --job="expire" --action="list" --token="token123" --secret="secret1234"
	python3 solnet_manage_jobs.py --job="import" --action="preview" --token="token123" --secret="secret1234" --jobid="jobid123"
	python3 solnet_manage_jobs.py --job="import" --action="delete" --token="token123" --secret="secret1234" --jobid="jobid123"
	```
