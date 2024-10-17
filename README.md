# Documentation

SOLNET AUTOMATION SCRIPTS

	PYR GAP FILLER

		Description:	Identifies PYR gaps and fills the gap with irradiance data calculated from data downloaded from solcast.
		Usage 1: 	pyr-gapfiller [nodeid] [PYR sourceids] [UTC start date] [UTC end date] [maxoutput] [lattitue] [longitude] [solcast_token] [solnet_token] [solnet_secret] 
		Example: 	/bin/bash pyr-gapfiller 379 /VI/SU/B2/PYR/1 2021-08-31 2021-09-03 1000000 18.343015 -64.911997 solcasttoken solnettoken solnetsecret

	PYR FILLER

        	Description:	It has a similar process as the pyr gap filler but it doesn't identify gaps. Applicable for date ranges that are known to have no data.
		Usage:		pyr-filler [nodeid] [PYR sourceids] [UTC start date] [UTC end date] [maxoutput] [latitude] [longitude] [solcast_token] [solnet_token] [solnet_secret]

	ENERGY BACKFILLER

		Description:	Fixes energy generation spikes caused by energy generation gaps. The energy generation of the spike is distributed across the gap and the ratio is driven by the irradiance data downloaded from solcast.  
		Usage:		ee-backfiller [nodeid] [GEN sourceids] [UTC start date] [UTC end date] [maxoutput] [lat] [long] [solcast_token] [solnet_token] [solnet_secret]
		Example:	/bin/bash ee-backfiller 372 /G2/S2/S1/GEN/1 2024-05-08 2024-06-27 1000000 29.658884 -82.334525 solcasttoken solnettoken solnetsecret


PYTHON SCRIPTS FOR SOLARNETWORK API

	Solnet Query 

		Description:	Lists irraddiance data for a given date range in UTC
		Usage:		solnet_query.py --node [nodeid] --sourceids [sourceids] --startdate [UTC startdate] --enddate [UTC enddate] --aggregate [Day|Hour|FiveMinute|None] --maxoutput [Max Output] --token [solnet token] --secret [solnet secret]
		Example:	python3 solnet_query.py --node 372 --sourceids %2FG2%2FS2%2FS1%2FPYR%2F1 --startdate 2024-05-01T00%3A00 --enddate 2024-06-31T23%3A59 --aggregate Day --maxoutput 1000000 --token ABCD1234 --secret WXYZ7890


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
