# SOLNET AUTOMATION SCRIPTS

Tools for filling data gaps in SolarNetwork (Solnet) using Solcast irradiance data.

## Contents

- [Requirements](#requirements)
- [What is in this repo](#what-is-in-this-repo)
- [Concepts you need before filling](#concepts-you-need-before-filling)
- [solnet-filler](#solnet-filler)
- [Python scripts](#python-scripts)
  - [solcast_pipeline.py](#solcast_pipelinepy)
  - [solcast_download.py](#solcast_downloadpy)
  - [solnet_query.py](#solnet_querypy)
  - [solnet_expire.py](#solnet_expirepy)
  - [solnet_import.py](#solnet_importpy)
  - [solnet_manage_jobs.py](#solnet_manage_jobspy)
  - [Auxiliary (Reset) record scripts](#auxiliary-reset-record-scripts)
  - [client.py (SolarNetwork API client)](#clientpy-solarnetwork-api-client)
- [Troubleshooting](#troubleshooting)

---

## Requirements

**Accounts and site details**

- Solcast API token from https://toolkit.solcast.com.au/ (not needed when filling from a local Solcast CSV)
- Solnet user token and secret with sufficient access to the node and source ID
- Site latitude and longitude from Ecosuite (not needed when filling from a local Solcast CSV)
- Site local timezone from Ecosuite (an IANA name, e.g. `America/New_York`)

**Software (used by `solnet-filler`)**

- `bash`, GNU `date`, `awk`, `sed`, `dos2unix`, `xz`
- `python3` with `requests` and the `solarnetwork_python` package (`client.py` and `authentication.py`)
- `/usr/share/zoneinfo/tzdata.zi` (used to validate the timezone)
- A writable `data/` directory in the working directory (queries, outputs and logs are written there)
- All Python scripts below in the same directory as `solnet-filler`

---

## What is in this repo

| File | Purpose |
|---|---|
| `solnet-filler` | Fills a known date range (no gap detection); can use the Solcast API or a local Solcast CSV |
| `solcast_pipeline.py` | Gets Solcast GHI (API or local CSV) and calculates the PYR or GEN/CON import file |
| `solcast_download.py` | Standalone Solcast download |
| `solnet_query.py`, `solnet_expire.py`, `solnet_import.py`, `solnet_manage_jobs.py` | Solnet API helpers |
| `solarnetwork_python/client.py` | SolarNetwork API client used by all the `solnet_*.py` scripts (see [client.py](#clientpy-solarnetwork-api-client)) |
| `solnet_get_auxiliary.py`, `solnet_store_auxiliary.py`, `solnet_delete_auxiliary.py` | Reset (auxiliary) record helpers |

---

## Concepts you need before filling

**Gap boundaries.** A gap sits between the last real datum before it and the first real datum after it. The fill must fit strictly inside those two boundaries.

**Window rounding.** Solcast data is in 5-minute steps. Round the `startdatetime` up to the next 5-minute mark and give the `enddatetime` a small offset so the last row lands where you want it, while staying earlier than the gap boundary.

**Which rows are included.** When filling from a local CSV, a row is included when its **period start** falls between the start and end (inclusive), and the row is stamped with that period start. So `04:00:00` to `03:59:59` gives rows stamped `04:00` ... `03:55`. When downloading from the Solcast API, the API decides which periods it returns, and the first row can be stamped up to 5 minutes before the start.

**GEN/CON counter convention.** The `wattHours` on a row is the counter value at that row's timestamp, i.e. the energy accumulated *before* that 5-minute interval. The interval's own energy (`watts / 12`) is added on the next row. As a result the first row equals the starting counter exactly (no artificial step after the last real datum), and the last row equals the starting counter plus the energy spike (unless the final interval has daylight energy).

**Energy spike (`--energyspike`).** The total energy in watt-hours to distribute across the window in proportion to irradiance. Typically the first real `wattHours` after the gap minus the last real `wattHours` before the gap on the same counter. Use another figure (for example the summed inverter deltas) when the counter is unreliable or was reset or replaced inside the gap. For PYR fills set it to `0`.

**Starting counter (`--startwatthours`).** The `wattHours` value the fill starts from; normally the last real `wattHours` before the gap. Defaults to `0`. For monthly fills, use the previous fill's last `wattHours` so the series is continuous.

**Reset events.** `solnet-filler` prints (does not run) `solnet_store_auxiliary.py` commands for the start and end borders of the fill. Only apply them where the fill meets real data. When you run consecutive monthly fills, the borders *between* the fills are not real discontinuities, so skip those Resets. Also check for existing Reset records inside the window (for example a meter swap): a continuous synthetic counter will not match them.

**Typical workflow**

1. Work out the gap boundaries, the energy spike and the starting counter.
2. Run `solnet-filler`. Review the printed head/tail of the output file.
3. Confirm the deletion of existing data in the window if prompted (preview count must match).
4. Review the staged import preview, then confirm to apply.
5. Run the printed `solnet_store_auxiliary.py` commands for the borders that need a Reset.

---

## solnet-filler

Fills a date range that is known to have no data (it does not identify gaps), or fills from the start of production. It can get Solcast data from the Solcast API, or from a Solcast CSV you already have (`--solcastcsv`).

### What it does

1. Converts the local start/end to UTC and checks for existing data in the window.
2. If data exists, offers (Y/N) to delete it with `solnet_expire.py` (preview first; aborts the delete if the preview count does not match the queried count, so you can do it manually).
3. Runs `solcast_pipeline.py` to build the import file (PYR irradiance hours, or GEN/CON watt-hours). Files are written to `data/`.
4. Looks at the readings just before and after the window and prints the two Reset (auxiliary) commands for the start and end borders.
5. Stages the file with `solnet_import.py` (compressed with `xz` if 20 MB or larger; every row is tagged `synthetic`), shows a preview, and asks (Y/N) before applying.
6. Logs everything to `data/solnet-filler-<timestamp>.log`.

### Usage

```
solnet-filler --node|-n [NODE] --sourceid|-i [SOURCEID] --timezone|-z [TIMEZONE] \
  --startdatetime|-s ['YYYY-MM-DD HH:MM:SS'] --enddatetime|-e ['YYYY-MM-DD HH:MM:SS'] \
  --energyspike|-g [ENERGY SPIKE IN Wh ; 0 for PYR] \
  --token|-k [SOLNET TOKEN] --secret|-c [SOLNET SECRET] \
  ( --latitude|-a [LAT] --longitude|-o [LON] --api|-p [SOLCAST API TOKEN]  |  --solcastcsv|-f [SOLCAST CSV] ) \
  [--startwatthours|-w [STARTING wattHours]]
```

### Parameters

| Option | Short | Required | Description |
|---|---|---|---|
| `--node` | `-n` | yes | Node ID |
| `--sourceid` | `-i` | yes | Source ID, e.g. `/MA6/FRAN/C1/GEN/1`. Must contain `PYR`, `GEN` or `CON` |
| `--timezone` | `-z` | yes | Site timezone (IANA name). The start/end are interpreted in this timezone and converted to UTC |
| `--startdatetime` | `-s` | yes | Local start, `YYYY-MM-DD HH:MM:SS` |
| `--enddatetime` | `-e` | yes | Local end, `YYYY-MM-DD HH:MM:SS` |
| `--energyspike` | `-g` | yes | Energy in Wh to distribute across the window. `0` for PYR; must be greater than 0 for GEN |
| `--token` | `-k` | yes | Solnet token |
| `--secret` | `-c` | yes | Solnet secret |
| `--latitude` | `-a` | API mode | Site latitude |
| `--longitude` | `-o` | API mode | Site longitude |
| `--api` | `-p` | API mode | Solcast API token |
| `--solcastcsv` | `-f` | CSV mode | Path to a Solcast CSV (see [format](#input-csv-format)). Skips the Solcast API; `-a`, `-o` and `-p` become optional. The CSV is filtered to the window |
| `--startwatthours` | `-w` | no | Starting `wattHours` for GEN/CON fills. Default `0`. Use the previous fill's last `wattHours` to keep fills continuous |

Options can be written as `--option value` or `--option=value`.

### Choosing the window

For the gap between `2023-10-04 19:57:23` and `2023-10-17 00:03:00`: round the start up to the next 5-minute mark (`20:00:00`), and for the end use a small offset such as `00:01:00`, which is earlier than the gap boundary `00:03:00` but lets the last row land on `2023-10-17 00:00:00`.

### Examples

PYR fill from the Solcast API:

```
/bin/bash solnet-filler --node 372 --sourceid /G2/S2/S1/PYR/2 --timezone America/New_York \
  --startdatetime '2023-10-04 20:00:00' --enddatetime '2023-10-17 00:01:00' \
  --latitude 29.658884 --longitude -82.334525 --energyspike 0 \
  --api solcasttoken --token solnettoken --secret solnetsecret
```

GEN fill using a local Solcast CSV and a starting counter (no Solcast API call):

```
/bin/bash solnet-filler --node 601 --sourceid /MA6/FRAN/C1/GEN/1 --timezone America/New_York \
  --startdatetime "2025-08-07 13:55:00" --enddatetime "2025-08-31 23:59:59" \
  --energyspike 61556865 --startwatthours 1187407700 \
  --token solnettoken --secret solnetsecret \
  --solcastcsv data-nf-29/601_solcast_result.csv
```

### Security note

`solnet-filler` writes the full command it was run with, including `--token`, `--secret` and `--api`, to `data/solnet-filler-<timestamp>.log`. Restrict access to the `data/` directory and delete old logs.

### Monthly fills (continuous counter)

When filling a long gap month by month, run each month with its own `--energyspike`, and set each month's `--startwatthours` to the previous month's last `wattHours` (previous start + previous spike). Start each following month at `00:00:00` local on the 1st and end it at `23:59:59` on the last day: the rows then chain with no overlap or gap.

---

## Python scripts

### solcast_pipeline.py

Gets Solcast GHI data, saves it, and builds the Solnet import file. It is what `solnet-filler` calls, and it can also be run by hand.

1. Gets 5-minute GHI data, either from the Solcast API (split into chunks of at most 30 days) or from a local CSV (`--input_csv`).
2. In API mode, saves the raw Solcast CSV.
3. Calculates the output based on the source ID:
   - contains `PYR`: cumulative irradiance hours
   - contains `GEN` or `CON`: watts and watt-hours, distributing `--energyspike` across the window in proportion to GHI

**Usage**

```
solcast_pipeline.py [--input_csv CSV | --latitude LAT --longitude LON --token TOKEN --solcast_csv_path PATH]
                    --startdate 'YYYY-MM-DD HH:MM:SSZ' --enddate 'YYYY-MM-DD HH:MM:SSZ'
                    --node NODE --sourceids SOURCEID
                    ( --irradiance_data_path OUT.csv                        # PYR
                    | --energyspike WH [--startwatthours WH]
                      --electrical_energy_data_path OUT.csv )                # GEN/CON
```

**Parameters**

| Option | Required | Description |
|---|---|---|
| `--input_csv` | CSV mode | Previously downloaded Solcast CSV. When given, the Solcast API is not called |
| `--latitude`, `--longitude`, `--token` | API mode | Site coordinates and Solcast API token |
| `--solcast_csv_path` | API mode | Where to save the raw Solcast CSV |
| `--startdate`, `--enddate` | API mode; optional filter in CSV mode | UTC, `YYYY-MM-DD HH:MM:SSZ`. In CSV mode, rows whose period start is within the window (inclusive) are kept; if omitted, every row in the file is used |
| `--node` | yes | Node ID written to the output |
| `--sourceids` | yes | Source ID. Decides PYR vs GEN/CON |
| `--irradiance_data_path` | PYR | Output path for the irradiance hours CSV |
| `--energyspike` | GEN/CON | Energy in Wh to distribute across the window |
| `--startwatthours` | no (GEN/CON) | Starting `wattHours` counter; default `0` |
| `--electrical_energy_data_path` | GEN/CON | Output path for the watt-hours CSV |

**Examples**

From a local CSV (no API call):

```
python3 solcast_pipeline.py --input_csv data-nf-29/601_solcast_result.csv \
  --startdate "2025-09-01 04:00:00Z" --enddate "2025-10-01 03:59:59Z" \
  --node 601 --sourceids /MA6/FRAN/C1/GEN/1 \
  --energyspike 57443559 --startwatthours 1248964565 \
  --electrical_energy_data_path out/601_EEGAP_202509.csv
```

From the Solcast API (PYR):

```
python3 solcast_pipeline.py --latitude 29.658884 --longitude -82.334525 \
  --startdate "2023-10-04 20:00:00Z" --enddate "2023-10-17 00:01:00Z" \
  --token solcasttoken --node 372 --sourceids /G2/S2/S1/PYR/2 \
  --solcast_csv_path data/372_solcast_result.csv \
  --irradiance_data_path data/372_PYRGAP.csv
```

#### Input CSV format

No header, one row per 5-minute period, UTC:

```
period_end,period_start,period,ghi
2025-08-07T18:00:00Z,2025-08-07T17:55:00Z,PT5M,884
2025-08-07T18:05:00Z,2025-08-07T18:00:00Z,PT5M,878
```

This is exactly what the pipeline saves in API mode, so a file it produced (or `solcast_download.py` output) can be reused later. In CSV mode the script skips blank lines and a header row, and warns if any period is not `PT5M` or if consecutive rows are not 5 minutes apart (the calculations assume 5-minute periods).

#### Output formats

GEN/CON (`NodeID,SourceID,Date,watts,wattHours`; `Date` is the period start in UTC):

```
601,/MA6/FRAN/C1/GEN/1,2025-08-07 17:55:00,420286.97,1187407700
601,/MA6/FRAN/C1/GEN/1,2025-08-07 18:00:00,417434.34,1187442723.91
```

PYR (`node,source,date,irradiance,irradianceHours`): irradiance hours accumulate `GHI / 12` per 5-minute row. The first row's `irradianceHours` is its raw GHI value.

If the GHI is 0 across the whole window (a night-time gap), `watts` is 0 and `wattHours` stays flat at the starting counter; a warning is printed.

### solcast_download.py

Downloads Solcast data for a given GPS coordinate and date range. The maximum date range is 30 days. (`solcast_pipeline.py` handles longer ranges by chunking.)

**Usage**

```
python3 solcast_download.py --latitude=[latitude] --longitude=[longitude] --startdate=[UTC start datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --enddate=[UTC end datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --token=[solcast_api_token]
```

**Example**

```
# python3 solcast_download.py --latitude="41.7287427" --longitude="-71.2782335" --startdate="2024-11-07 22:13:40.000Z" --enddate="2024-11-15 02:48:14.000Z" --token="foobar"
2024-11-07T22:15:00Z,2024-11-07T22:10:00Z,PT5M,0
2024-11-07T22:20:00Z,2024-11-07T22:15:00Z,PT5M,0
2024-11-07T22:25:00Z,2024-11-07T22:20:00Z,PT5M,0
2024-11-07T22:30:00Z,2024-11-07T22:25:00Z,PT5M,0
```

### solnet_query.py

Lists PYR/GEN data for a given date range in UTC or local time. For `utc`, it sorts and prints `wattHours` and `irradianceHours` as the last columns.

**Usage**

```
solnet_query.py --action=[utc|local] --node=[nodeid] --sourceids=[sourceids] --startdate=[UTC start datetime in 'YYYY-MM-DD HH:MM:SS'] --enddate=[UTC end datetime in 'YYYY-MM-DD HH:MM:SS'] --aggregate=[Day|Hour|FiveMinute|None] --maxoutput=[Max Output] --token=[solnet token] --secret=[solnet secret]
```

Extra parameters for `--action=local`:

```
--localstartdate [start datetime in 'YYYY-MM-DD HH:MM:SS' in local timezone]
--localenddate   [end datetime in 'YYYY-MM-DD HH:MM:SS' in local timezone]
--header [always|never|auto]
--output [filename]
--verbose
```

**Example**

```
python3 solnet_query.py --action="utc" --node="372" --sourceids="/G2/S2/S1/PYR/1" --startdate="2024-05-01 00:00" --enddate="2024-06-30 23:59" --aggregate="Day" --maxoutput="1000000" --token="ABCD1234" --secret="WXYZ7890"
```

### solnet_expire.py

Has two actions: `preview` shows how many datum would be deleted in a date range, and `confirm` deletes them. The date range must be in **local** date and time.

**Usage**

```
solnet_expire.py --action=[preview|confirm] --node=[nodeid] --sourceids=[sourceids] --localstartdate=[LOCAL start datetime in 'YYYY-MM-DD HH:MM:SS'] --localenddate=[LOCAL end datetime in 'YYYY-MM-DD HH:MM:SS'] --token=[solnet token] --secret=[solnet secret]
```

**Example**

```
python3 solnet_expire.py --action="preview" --node="372" --sourceids="/G2/S2/S1/PYR/1" --localstartdate="2024-05-09 19:00:30" --localenddate="2024-06-10 20:59:30" --token="ABCD1234" --secret="WXYZ7890"

python3 solnet_expire.py --action="confirm" --node="372" --sourceids="/G2/S2/S1/PYR/1" --localstartdate="2024-05-09 19:00:30" --localenddate="2024-06-10 20:59:30" --token="ABCD1234" --secret="WXYZ7890"
```

`preview` prints only the datum count. `confirm` prints the node, source and dates, then the server response. Deletion runs as a job; monitor it with `solnet_manage_jobs.py --job="expire" --action="list"`. Before confirming the import, `solnet-filler` checks once that the expire preview count is 0; if it is not, it stops and tells you to run the confirm manually after the deletion finishes.

### solnet_import.py

Imports (stages) data from a CSV file. Nothing is applied until the job is confirmed with `solnet_manage_jobs.py`. On success it prints the import job ID.

Every data row gets an extra `tag` column (default value `synthetic`), so the imported datum are tagged. Change it with `--tag`.

**Usage**

```
solnet_import.py --node=[nodeid] --sourceids=[sourceids] --timezone=[must be set to UTC] --compression=[enabled|disabled] --filepath=[path of csv file with PYR or EE data] --token=[solnet token] --secret=[solnet secret] [--tag=[tag, default synthetic]]
```

**Parameters**

| Option | Required | Description |
|---|---|---|
| `--node` | yes | Node ID |
| `--sourceids` | yes | Source ID, e.g. `/MA6/FRAN/C1/GEN/1` (the URL-encoded `%2F` form also works) |
| `--timezone` | yes | Time zone of the dates in the file. Set it to `UTC`; the pipeline writes UTC dates |
| `--compression` | yes | `enabled` if the file is compressed, `disabled` if it is plain CSV |
| `--filepath` | yes | Path of the CSV file |
| `--token`, `--secret` | yes | Solnet credentials |
| `--tag` | no | Tag added to every row. Default `synthetic` |

**File requirements**

- One header row, then data rows with columns in this order: node ID, source ID, date (`yyyy-MM-dd HH:mm:ss`), instantaneous value (`watts` or `irradiance`), accumulating value (`wattHours` or `irradianceHours`). This is the layout `solcast_pipeline.py` writes.
- With `--compression=enabled` the format is detected from the file extension, so the file must end in `.gz`, `.bz2`, `.xz` or `.lz4` (`.lz4` needs `pip install lz4`). The file is decompressed, tagged and compressed again in the same format. The upload is always labelled `data.csv.xz` by `client.import_compressed_data`, so use `.xz` (what `solnet-filler` produces) unless you have tested the other formats.

**Example**

```
python3 solnet_import.py --node="372" --sourceids="/G2/S2/S1/PYR/1" --timezone="UTC" --compression="disabled" --filepath="data/372_%2FG2%2FS2%2FS1%2FPYR%2F1_PYRGAP_SolNetIMport_20240915_120914.csv" --token="ABCD1234" --secret="WXYZ7890"
```

If the import fails, the script prints `Error occurred while importing data: ...` and still exits with status 0, so `solnet-filler` (which reads the job ID from the output) would treat that message as the job ID. Check the output before confirming anything.

### solnet_manage_jobs.py

Lists and deletes expire jobs. Also views, previews, confirms and deletes import jobs.

**Usage**

```
python3 solnet_manage_jobs.py [-h] [--job="{expire,import}"] [--action="{list,view,preview,delete,confirm}"] --token="TOKEN" --secret="SECRET" --jobid="JOBID"
```

**Example**

```
python3 solnet_manage_jobs.py --job="expire" --action="list" --token="token123" --secret="secret1234"
python3 solnet_manage_jobs.py --job="import" --action="preview" --token="token123" --secret="secret1234" --jobid="jobid123"
python3 solnet_manage_jobs.py --job="import" --action="delete" --token="token123" --secret="secret1234" --jobid="jobid123"
```

### Auxiliary (Reset) record scripts

#### solnet_get_auxiliary.py

Retrieves auxiliary records.

```
python3 solnet_get_auxiliary.py --node=[nodeid] --sourceids=[sourceid] --startdate=[UTC start datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --enddate=[UTC end datetime in 'YYYY-MM-DD HH:MM:SS.000Z'] --token="foo" --secret="foobar"
```

Example:

```
# python3 solnet_get_auxiliary.py --node="672" --sourceids="/CT13/OJBF/R1/GEN/1" --startdate="2024-06-01 00:00:00" --enddate="2024-07-30 23:59:59" --token="foo" --secret="foobar"
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

#### solnet_store_auxiliary.py

Stores a Reset record. `--created` is the UTC time of the event. `--final` is the `wattHours` just before it and `--start` is the `wattHours` just after it. `solnet-filler` prints these commands for the start and end borders of every fill.

```
python3 solnet_store_auxiliary.py --node [nodeid] --source [sourceid] --type "Reset" --created [UTC datetime in 'YYYY-MM-DD HH:MM:SSZ'] --notes [description] --final [final wattHours before created date] --start [starting wattHours after created date] --token "foo" --secret "foobar"
```

Example:

```
python3 solnet_store_auxiliary.py --node 465 --source "/NYVIII/ARCAD/RT1/PYR/2" --type "Reset" --created "2025-11-11 20:34:55Z" --notes "Discontinuity due to Gap - End Border" --final '{"a":{"wattHours":14308.58}}' --start '{"a":{"wattHours":4322142.833299}}' --token "foo" --secret "foobar"
```

#### solnet_delete_auxiliary.py

Deletes a Reset record. `--eventdate` is the UTC `created` time of the record to delete (find it with `solnet_get_auxiliary.py`).

```
python3 solnet_delete_auxiliary.py --node [nodeid] --source [sourceid] --eventdate [UTC datetime in 'YYYY-MM-DD HH:MM:SSZ'] --token "foo" --secret "foobar"
```

Example:

```
python3 solnet_delete_auxiliary.py --node 465 --source "/NYVIII/ARCAD/RT1/PYR/2" --eventdate "2025-11-11 20:34:55Z" --token "foo" --secret "foobar"
```

### client.py (SolarNetwork API client)

`solarnetwork_python/client.py` is the low-level client behind every `solnet_*.py` script. Each method builds a signed request (the `Authorization` header comes from `generate_auth_header` in `solarnetwork_python.authentication`), calls `https://data.solarnetwork.net`, and returns the parsed JSON.

```python
from solarnetwork_python.client import Client

client = Client(token, secret)

# Same query string solnet_expire.py builds: dates as YYYY-MM-DDTHH%3AMM%3ASS, source IDs with %2F
params = ("aggregationKey=0&localEndDate=2025-08-31T23%3A59%3A59&localStartDate=2025-08-07T13%3A55%3A00"
          "&nodeIds=601&sourceIds=%2FMA6%2FFRAN%2FC1%2FGEN%2F1")
print(client.expirepreview(params)["datumCount"])
```

`solnet_expire.py` calls `expirepreview` and `expireconfirm`; `solnet_import.py` calls `import_data` and `import_compressed_data`. Methods that take `paramstr` expect an already URL-encoded query string, without the leading `?`.

**Methods** (paths are relative to `/solaruser/api/v1/sec` unless they start with `/solarquery`)

| Method | HTTP and endpoint | Arguments | Returns |
|---|---|---|---|
| `solarquery` | GET `/solarquery/api/v1/sec/datum/list` | `paramstr` | `data` (datum list) |
| `listdeletejobs` | GET `/user/expire/datum-delete/jobs` | none | `data` (expire/delete jobs) |
| `expirepreview` | POST `/user/expire/datum-delete?<paramstr>` | `paramstr` | `data`, including `datumCount` |
| `expireconfirm` | POST `/user/expire/datum-delete/confirm?<paramstr>` | `paramstr` | `data` (the delete job) |
| `listexpireconfigs` | GET `/user/expire/configs` | none | `data` (expire configurations) |
| `deleteexpireconfig` | DELETE `/user/expire/configs/data/{configid}` | `configid` | `data` |
| `listimportjobs` | GET `/user/import/jobs` | none | `data` (import jobs) |
| `viewimportjobs` | GET `/user/import/jobs/{jobid}` | `jobid` | `data` |
| `previewimportjobs` | GET `/user/import/jobs/{jobid}/preview` | `jobid` | full JSON response |
| `confirmimportjobs` | POST `/user/import/jobs/{jobid}/confirm` | `jobid` | full JSON response |
| `deleteimportjobs` | DELETE `/user/import/jobs/{jobid}` | `jobid` | `data` |
| `import_data` | POST `/user/import/jobs` (multipart: `config` JSON + `data` CSV) | `description` (import config dict), `importdata` (CSV text) | `data` (includes `jobId`) |
| `import_compressed_data` | POST `/user/import/jobs` (multipart, data part base64-encoded) | `description`, `importdata` (compressed bytes) | `data` (includes `jobId`) |
| `store_datum_auxiliary` | POST `/datum/auxiliary` (JSON body) | `auxiliary_data` dict | `data` |
| `get_auxiliary` | GET `/datum/auxiliary?<paramstr>` | `paramstr` | full JSON response |
| `delete_auxiliary` | DELETE `/datum/auxiliary?<paramstr>` | `paramstr` | `data` |

`store_datum_auxiliary` takes a dict with these keys:

| Key | Description |
|---|---|
| `created` | Event date, as a string in `yyyy-MM-dd HH:mm:ss.SSS` format or as a millisecond epoch |
| `nodeId` | Node ID (int) |
| `sourceId` | Source ID (string) |
| `type` | Datum auxiliary type, e.g. `Reset` |
| `notes` | Optional comment |
| `final` | Datum sample for the final reading, e.g. `{"a": {"wattHours": 1482948300}}` |
| `start` | Datum sample for the starting reading, same shape as `final` |
| `meta` | Optional metadata object |

**Behaviour to be aware of**

- **Failures:** most methods raise `Exception("Unsuccessful API call")` when the response's `success` is not true. `deleteexpireconfig` and `listexpireconfigs` include the server's message in the exception. `previewimportjobs`, `confirmimportjobs` and `get_auxiliary` do not check `success` and return the raw response, so callers must check it themselves.
- **Debug output:** `listexpireconfigs` prints two `[debug] ...` lines (request, status and the first 500 characters of the body) to stdout on every call. Don't parse its stdout.
- **Compressed uploads:** `import_compressed_data` always labels the upload `data.csv.xz`, whatever the real compression format.
- **Dates:** requests are timestamped with `datetime.utcnow()`, which Python 3.12 and later flags as deprecated (it still works).
- **Auxiliary delete:** `delete_auxiliary` signs the path `/solaruser/api/v1/sec/datum/auxiliary/` (trailing slash) with a JSON content type, but sends the request to the URL without the trailing slash and with a form-encoded content type. If it ever fails with an authentication error, check that first.

---

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `Error: The following arguments are required ...` | A required option is missing. `--timezone`, `--energyspike`, `--token` and `--secret` are always required. `--latitude`, `--longitude` and `--api` are required only without `--solcastcsv` |
| `For GEN Fills, energyspike should be greater than 0` | GEN fills need a spike above 0. Only PYR fills use `0` |
| `Skipping Process. Count doesn't match, adjust date and run manually.` | The expire preview count differs from the queried datum count (often too many datum to download). Delete the data manually with `solnet_expire.py`, then continue |
| `Process aborted. Expire process not completed.` | Old data in the window is still being expired. Check `solnet_manage_jobs.py --job="expire" --action="list"`, then confirm the import job manually |
| `no rows loaded from input CSV` | The window is outside the CSV's range or the dates are not in UTC `YYYY-MM-DD HH:MM:SSZ` format. Compare the loaded row count to the number of 5-minute steps you expect |
| Fill is a few rows short | The CSV does not cover the whole window. The pipeline does not warn about a window that extends past the file |
| First fill row is stamped before the start (API mode) | The Solcast API returns the period that ends after the start, so the first row is stamped one period earlier. CSV mode filters on period start instead |
| Spike in the data right after the first row | The `--energyspike` is wrong for the counter, or `--startwatthours` was left at 0 on a real counter. Re-check the spike against the real readings on either side of the gap |
| Month fills do not line up | Each month's `--startwatthours` must equal the previous month's last `wattHours` |
| Warning: GHI is 0 across the entire gap | A night-time gap. The fill is flat; the real energy lands on the first reading after the gap |
| Warnings about non-`PT5M` periods or irregular intervals | The CSV is not clean 5-minute data. The energy distribution assumes 5-minute periods, so fix or re-download the data |
