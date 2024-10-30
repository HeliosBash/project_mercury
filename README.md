# Documentation

SOLNET AUTOMATION SCRIPTS

	PYR GAP FILLER

		Description:	Identifies PYR gaps and fills the gap with irradiance data calculated from data downloaded from solcast.
		Usage:		pyr-gapfiller [--help|-h] --node|-n [NODE] --sourceid|-i [SOURCE ID] --startdate|-s [START DATE] --enddate|-e [END DATE] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
		Example: 	/bin/bash pyr-gapfiller --node 379 --sourceid /VI/SU/B2/PYR/1 --startdate 2021-08-31 --enddate 2021-09-03 --latitude 18.343015 --longitude -64.911997 --api solcasttoken --token solnettoken --secret solnetsecret

	PYR FILLER

		Description:	It has a similar process as the pyr gap filler but it doesn't identify gaps. Applicable for date ranges that are known to have no data.
		Usage:		pyr-filler [--help|-h] --node|-n [NODE] --sourceid|-i [SOURCE ID] --timezone|-z [TIMEZONE] --startdatetime|-s [START DATETIME] --enddatetime|-e [END DATETIME] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
		Example:        /bin/bash pyr-filler --node 350 --sourceid /PA/LO/S1/PYR/1 --timezone America/New_York --startdatetime 2024-07-16\ 20:00 --enddatetime 2024-07-31\ 23:59 --latitude 39.8712977 --longitude -75.6749004 --api solcasttoken --token solnettoken --secret solnetsecret

	ENERGY BACKFILLER

		Description:	Fixes energy generation spikes caused by energy generation gaps. The total energy generation of the spike is distributed across the gap and the ratio used is driven by the irradiance data downloaded from solcast.  
		Usage:		ee-backfiller --node|-n [NODE] --sourceid|-i [SOURCEID] --startdate|-s [STARTDATETIME in "YYYY-MM-DD HH:MM"] --enddatetime|-e [ENDDATETIME in "YYYY-MM-DD HH:MM"] --latitude|-a [LATITUDE] --longitude|-o [LONGITUDE] --api|-p [SOLCAST API TOKEN] --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET]
		Example:	/bin/bash ee-backfiller --node 372 --sourceid /G2/S2/S1/GEN/1 --startdatetime "2023-10-04 20:00" --enddatetime "2023-10-17 00:00" --latitude 29.658884 --longitude -82.334525 --api solcasttoken --token solnettoken --secret solnetsecret

	BAD DATA DELETER
		
		Description:    Scans for bad data, wherein the values of watts and watthours are 0, for a given year and month and processes its deletion. It does an hourly scan which is found to be effective. 
		Usage:          ee-bad-data-deleter --node [nodeid] --sourceid [GEN sourceids] --month [month in MM format] --year [year in YYYY format] --token [solnet_token] --secret [solnet_secret]
		Example:        /bin/bash ee-bad-data-deleter --node 372 --sourceid /G2/S2/S1/GEN/1 --month 06 --year 2024 --token solnettoken --secret solnetsecret

	PYR/GEN DATA EXPORTER

		Description:	Exports PYR/GEN data for a given date range based on local timezeone and dumps it to a file. 
		Usage:		data-exporter [nodeid] [GEN sourceids] [start date] [end date] [maxoutput] [solnet_token] [solnet_secret]
		Example:	/bin/bash data-exporter 350 /PA/LO/S1/PYR/2 2024-10-12 2024-10-16 1000000 solnettoken solnetsecret


PYTHON SCRIPTS FOR SOLARNETWORK API

	Solnet Query 

		Solnet Query UTC
		
		Description:	Lists PYR/GEN data for a given date range in UTC
		Usage:		solnet_query.py --node [nodeid] --sourceids [sourceids] --startdate [UTC startdate] --enddate [UTC enddate] --aggregate [Day|Hour|FiveMinute|None] --maxoutput [Max Output] --token [solnet token] --secret [solnet secret]
		Example:	python3 solnet_query.py --node 372 --sourceids %2FG2%2FS2%2FS1%2FPYR%2F1 --startdate 2024-05-01T00%3A00 --enddate 2024-06-31T23%3A59 --aggregate Day --maxoutput 1000000 --token ABCD1234 --secret WXYZ7890


		Solnet Query Local

                Description:    Lists PYR/GEN data for a given date range in Local timezone
                Usage:          solnet_query_local.py --node [nodeid] --sourceids [sourceids] --localstartdate [Local startdate] --localenddate [Local enddate] --aggregate [Day|Hour|FiveMinute|None] --maxoutput [Max Output] --token [solnet token] --secret [solnet secret]
                Example:        python3 solnet_query_local.py --node 372 --sourceids %2FG2%2FS2%2FS1%2FPYR%2F1 --localstartdate 2024-05-01T00%3A00 --localenddate 2024-06-31T23%3A59 --aggregate Day --maxoutput 1000000 --token ABCD1234 --secret WXYZ7890


	Solnet Expire Datum - Preview

		Description:    Provides a preview on the number of datum to be deleted in a given date range. Date range should be the LOCAL date and time.
		Usage: 		solnet_expire_preview.py --node [nodeid] --sourceids [sourceids] --localstartdate [LOCAL start date] --localenddate [LOCAL end date] --token [solnet token] --secret [solnet secret]
		Example:	python3 solnet_expire_preview.py --node 372 --sourceids %2FG2%2FS2%2FS1%2FPYR%2F1 --localstartdate 2024-05-09T19%3A00%3A30 --localenddate 2024-06-10T20%3A59%3A30 --token ABCD1234 --secret WXYZ7890


	Solnet Expire Datum - Confirm

		Description:    Deletes datum in a given date range. Date range should be the LOCAL date and time.
		Usage:          solnet_expire_confirm.py --node [nodeid] --sourceids [sourceids] --localstartdate [Local start date] --localenddate [LOCAL end date] --token [solnet token] --secret [solnet secret]
		Example:        python3 solnet_expire_confirm.py --node 372 --sourceids %2FG2%2FS2%2FS1%2FPYR%2F1 --localstartdate 2024-05-09T19%3A00%3A30 --localenddate 2024-06-10T20%3A59%3A30 --token ABCD1234 --secret WXYZ7890

	Solnet Import

		Description:    Imports staged data.
		Usage:		solnet_import.py --node [nodeid] --sourceids [sourceids] --timezone [must be set to UTC] --filepath [path of csv file with PYR or EE data] --token [solnet token] -secret [solnet secret]
		Example: 	python3 solnet_import.py --node 372 --sourceids %2FG2%2FS2%2FS1%2FPYR%2F1 --timezone UTC --filepath data/372_%2FG2%2FS2%2FS1%2FPYR%2F1_PYRGAP_SolNetIMport_20240915_120914.csv --token ABCD1234 --secret WXYZ7890

	Solcast Download
		
		Description:	Downloads solcast data for a given GPS coordinate and date range. The maximum date range that can be set is 30 days.
		Usage:		python3 solcast_download.py [latitude] [longitude] [UTC start date] [UTC end date] [solcast_api_token]
		Example:	python3 solcast_download.py 29.658884 -82.334525 2024-06-11T11%3A10%3A00.000Z 2024-06-11T11%3A25%3A00.000Z foobar

	Solnet Manage Jobs
		
		Description:	Lists and delete expire jobs. It also wiews, previews, confirms, and deletes import jobs.   
		Usage:		python3 solnet_manage_jobs.py [jobs] [action] [token] [secret] [jobid]
		Example:        python3 solnet_manage_jobs.py expire list token123 secret1234 
				python3 solnet_manage_jobs.py import preview token123 secret1234 jobid123
				python3 solnet_manage_jobs.py import delete token123 secret1234 jobid123

