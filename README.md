# SOLNET AUTOMATION SCRIPTS

## REQUIREMENTS

- [ ] Solcast API Token from https://toolkit.solcast.com.au/
- [ ] Solnet User Token and Secret that has sufficient access to the Node and Source ID
- [ ] Site Latitude and Longitude Coordinates from Ecosuite
- [ ] Site Local Timezone from Ecosuite

## PYR GAP FILLER

- [ ] Identifies PYR gaps and fills the gap with irradiance data calculated from data downloaded from solcast.

- [ ] Usage:		
	```
	pyr-gapfiller [--help|-h] --node|-n [NODE] --sourceid|-i [SOURCE ID] --startdate|-s [START DATETIME in 'YYYY-MM-DD HH:MM'] --enddatetime|-e [END DATETIME in 'YYYY-MM-DD HH:MM'] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
	```

- [ ] Example: 	
	```
	/bin/bash pyr-gapfiller --node 379 --sourceid /VI/SU/B2/PYR/1 --startdate '2021-08-31 00:00' --enddate '2021-09-03 00:00' --latitude 18.343015 --longitude -64.911997 --api solcasttoken --token solnettoken --secret solnetsecret
	```
## PYR FILLER

- [ ] It has a similar process as the pyr gap filler but it doesn't identify gaps. Applicable for date ranges that are known to have no data. Make sure to round up the date range to the nearest 5 minute interval. 

- [ ] Usage:		
	```
	pyr-filler [--help|-h] --node|-n [NODE] --sourceid|-i [SOURCE ID] --timezone|-z [TIMEZONE] --startdatetime|-s [START DATETIME in 'YYYY-MM-DD HH:MM'] --enddatetime|-e [END DATETIME in 'YYYY-MM-DD HH:MM'] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
	```		

- [ ] Example:        
	```
	/bin/bash pyr-filler --node 350 --sourceid /PA/LO/S1/PYR/1 --timezone America/New_York --startdatetime '2024-07-16 20:00' --enddatetime '2024-07-31 23:59' --latitude 39.8712977 --longitude -75.6749004 --api solcasttoken --token solnettoken --secret solnetsecret
	```

## ENERGY BACKFILLER

- [ ] Fixes energy generation spikes caused by energy generation gaps. The total energy generation of the spike is distributed across the gap and the ratio used is driven by the irradiance data downloaded from solcast.  

- [ ] Usage:
	```		
	ee-backfiller --node|-n [NODE] --sourceid|-i [SOURCEID] --startdate|-s [STARTDATETIME in 'YYYY-MM-DD HH:MM'] --enddatetime|-e [ENDDATETIME in 'YYYY-MM-DD HH:MM'] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
	```

- [ ] Example:	
	```
	/bin/bash ee-backfiller --node 372 --sourceid /G2/S2/S1/GEN/1 --startdatetime '2023-10-04 20:00' --enddatetime '2023-10-17 00:00' --latitude 29.658884 --longitude -82.334525 --api solcasttoken --token solnettoken --secret solnetsecret
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

## PYR/GEN DATA EXPORTER

- [ ] Exports PYR/GEN data for a given date range based on local timezeone and dumps it to a file.

- [ ] Usage:		
	```
	data-exporter [nodeid] [GEN sourceids] [start date] [end date] [maxoutput] [solnet_token] [solnet_secret]
	```

- [ ] Example:
	```	
	/bin/bash data-exporter 350 /PA/LO/S1/PYR/2 2024-10-12 2024-10-16 1000000 solnettoken solnetsecret
	```

# PYTHON SCRIPTS FOR SOLARNETWORK API

## Solnet Query 

### Solnet Query UTC
		
- [ ] Lists PYR/GEN data based on a given date range in UTC
		
- [ ] Usage:		
	```
	solnet_query.py --node [nodeid] --sourceids [sourceids] --startdate [UTC start datetime in 'YYYY-MM-DD HH:MM'] --enddate [UTC end datetime in 'YYYY-MM-DD HH:MM'] --aggregate [Day|Hour|FiveMinute|None] --maxoutput [Max Output] --token [solnet token] --secret [solnet secret]
	```
		
- [ ] Example:
	```
	python3 solnet_query.py --node 372 --sourceids /G2/S2/S1/PYR/1 --startdate '2024-05-01 00:00' --enddate '2024-06-31 23:59' --aggregate Day --maxoutput 1000000 --token ABCD1234 --secret WXYZ7890
	```

### Solnet Query Local

- [ ] Lists PYR/GEN data based on a given date range in Local timezone
                
- [ ] Usage:          
	```
	solnet_query_local.py --node [nodeid] --sourceids [sourceids] --localstartdate [Local start datetime in 'YYYY-MM-DD HH:MM'] --localenddate [Local end datetime in 'YYYY-MM-DD HH:MM'] --aggregate [Day|Hour|FiveMinute|None] --maxoutput [Max Output] --token [solnet token] --secret [solnet secret]
	```                

- [ ] Example:        
	```
	python3 solnet_query_local.py --node 372 --sourceids /G2/S2/S1/PYR/1 --localstartdate '2024-05-01 00:00' --localenddate '2024-06-31 23:59' --aggregate Day --maxoutput 1000000 --token ABCD1234 --secret WXYZ7890
	```

## Solnet Expire Datum - Preview

- [ ] Provides a preview on the number of datum to be deleted in a given date range. Date range should be the LOCAL date and time.
		
- [ ] Usage: 		
	```
	solnet_expire_preview.py --node [nodeid] --sourceids [sourceids] --localstartdate [LOCAL start datetime in 'YYYY-MM-DD HH:MM'] --localenddatetime [LOCAL end date in 'YYYY-MM-DD HH:MM'] --token [solnet token] --secret [solnet secret]
	```

- [ ] Example:	
	```
	python3 solnet_expire_preview.py --node 372 --sourceids /G2/S2/S1/PYR/1 --localstartdate '2024-05-09 19:00:30' --localenddate '2024-06-10 20:59:30' --token ABCD1234 --secret WXYZ7890
	```

## Solnet Expire Datum - Confirm

- [ ] Deletes datum in a given date range. Date range should be the LOCAL date and time.

- [ ] Usage:          
	```
	solnet_expire_confirm.py --node [nodeid] --sourceids [sourceids] --localstartdate [Local start datetime in 'YYYY-MM-DD HH:MM'] --localenddate [LOCAL end datetime in 'YYYY-MM-DD HH:MM'] --token [solnet token] --secret [solnet secret]
	```		

- [ ] Example:        
	```
	python3 solnet_expire_confirm.py --node 372 --sourceids /G2/S2/S1/PYR/1 --localstartdate '2024-05-09 19:00:30' --localenddate '2024-06-10 20:59:30' --token ABCD1234 --secret WXYZ7890
	```
	
## Solnet Import

- [ ] Imports staged data.
		
- [ ] Usage:		
	```
	solnet_import.py --node [nodeid] --sourceids [sourceids] --timezone [must be set to UTC] --compression [ enabled or disabled ] --filepath [path of csv file with PYR or EE data] --token [solnet token] -secret [solnet secret]
	```		

- [ ] Example: 	
	```
	python3 solnet_import.py --node 372 --sourceids /G2/S2/S1/PYR/1 --timezone UTC --compression disabled --filepath data/372_%2FG2%2FS2%2FS1%2FPYR%2F1_PYRGAP_SolNetIMport_20240915_120914.csv --token ABCD1234 --secret WXYZ7890
	```

## Solcast Download
		
- [ ]	Downloads solcast data for a given GPS coordinate and date range. The maximum date range that can be set is 30 days.

- [ ] 	Usage:
	```
	python3 solcast_download.py --latitude [latitude] --longitude [longitude] --startdate [UTC start datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --enddate [UTC end datetime in 'YYYY-MM-DD HH:MM:SS.000Z' ] --token [solcast_api_token]
	```

- [ ] 	Example:	
	```
	# python3 solcast_download.py --latitude 41.7287427 --longitude -71.2782335 --startdate '2024-11-07 22:13:40.000Z' --enddate '2024-11-15 02:48:14.000Z' --token foobar
	2024-11-07T22:15:00Z,2024-11-07T22:10:00Z,PT5M,0
	2024-11-07T22:20:00Z,2024-11-07T22:15:00Z,PT5M,0
	2024-11-07T22:25:00Z,2024-11-07T22:20:00Z,PT5M,0
	2024-11-07T22:30:00Z,2024-11-07T22:25:00Z,PT5M,0

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
