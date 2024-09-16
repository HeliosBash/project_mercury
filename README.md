# Documentation

GAP FILLER PROCESSES:

1. Identify gaps with aggregation set to Days
2. Identify gaps with aggregation set to Hours
3. Identify gaps with aggregation set to FiveMinute
4. Attempts to identify any data within the gap and expire them
5. Download solcast data. If the number of days required exceeds 30 days, it will download solcast data in batches
6. Computes irradiance hours and backfill data
7. Import Data. If data exceeds 5000 rows, data will be divided in batches.


MAIN COMMAND: gap-filler [ nodeid ] [ sourceids ] [ startdate ] [ enddate ] [ maxoutput ] [ lat ] [ long ] [ solcast_token ] [ solne_token ] [ solnet_secret ]

Example: /bin/bash gap-filler 372 /G2/S2/S1/PYR/1 2024-05-01 2024-06-31 1000000 29.658884 -82.334525 XXXXXX ABCD1234 WXYZ7890


SUB COMMANDS:

1. solnet_query.py [ nodeid ] [ sourceids ] [ startdate ] [ enddate ] [ Day | Hour | FiveMinute | None ] [ Max Output ] [ solne_token ] [ solnet_secret ]

Example: python3 solnet_query.py 372 %2FG2%2FS2%2FS1%2FPYR%2F1 2024-05-01T00%3A00 2024-06-31T23%3A59 Day 1000000 ABCD1234 WXYZ7890


2. solnet_expire_preview.py [ nodeid ] [ sourceids ] [ startdate ] [ enddate ] [ solne_token ] [ solnet_secret ]

Example: python3 solnet_expire_preview.py 372 %2FG2%2FS2%2FS1%2FPYR%2F1 2024-05-09T19%3A00%3A30 2024-06-10T20%3A59%3A30 ABCD1234 WXYZ7890


3. solnet_expire_confirm.py [ nodeid ] [ sourceids ] [ startdate ] [ enddate ] [ solne_token ] [ solnet_secret ]

Example: python3 solnet_expire_confirm.py 372 %2FG2%2FS2%2FS1%2FPYR%2F1 2024-05-09T19%3A00%3A30 2024-06-10T20%3A59%3A30 ABCD1234 WXYZ7890


4. python3 solcast_download.py [ latitude ] [ longitude ] [ startdate ] [ enddate ] [ solne_token ]

Example: python3 solcast_download.py 29.658884 -82.334525 2024-05-09T00%3A00%3A00.000Z 2024-06-08T23%3A59%3A59.000Z XXXXXX


5. python3 solnet_import.py [ nodeid ] [ sourceids ] [ timezone ] [ csv_file_path_computed_irradiance_data ] [ solne_token ] [ solnet_secret ] 

Example: python3 solnet_import.py 372 %2FG2%2FS2%2FS1%2FPYR%2F1 UTC data/372_%2FG2%2FS2%2FS1%2FPYR%2F1_PYRGAP_SolNetIMport_20240915_120914.csv ABCD1234 WXYZ7890


6. python3 solnet_manage_jobs.py [expire|import] [list] token secret

Example: python3 solnet_manage_jobs.py import list ABCD1234 WXYZ7890


7. python3 solnet_manage_jobs.py [import] [view|preview|delete|confirm] token secret jobid

Example: python3 solnet_manage_jobs.py import preview ABCD1234 WXYZ7890 db14e0ce-2a0d-4ed5-bae0-3874bdcf74ed
