# Documentation

Usage: gap-filler [ node ] [ sourceids ] [ startdate ] [ enddate ] [ maxoutput ] [ lat ] [ long ] [ token ] [ secret ]

Example: /bin/bash gap-filler 111 /TES/TEST/TEST/TEST/1 2021-11-01 2021-11-30 1000000 11.1111 -22.22222 token secret

Processes:

1. Identify gaps with aggregation set to Days
2. Identify gaps with aggregation set to Hours
3. Identify gaps with aggregation set to FiveMinute
4. Checks if there is data between the gap
5. Provides an option to delete the data between the gap
6. Provides an option to generate and download solcast data
7. Provides an option compute irradiance and backfill data
8. Import Solnet Data
