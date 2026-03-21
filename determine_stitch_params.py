"""
determine_stitch_params.py

Determines Solar Network Event (SNE) stitch parameters for gap correction.
Converted from bash script. Integrates solnet_query.py functions directly
instead of calling them as subprocess.

Usage:
    python3 determine_stitch_params.py \
        --node <node_id> \
        --sourceids <source_ids> \
        --sid <sid> \
        --utcstartdate "2025-01-01 08:00:00" \
        --utcenddate "2025-01-02 08:00:00" \
        --localstartdate "2025-01-01 16:00:00" \
        --localenddate "2025-01-02 16:00:00" \
        --file_to_import /path/to/import.csv \
        --maxoutput 1000 \
        --token <token> \
        --secret <secret>
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

# ── Import solnet_query functions directly ────────────────────────────────────
# Assumes solnet_query.py is in the same directory (or on PYTHONPATH)
from solnet_query import solar_query_batch
from solarnetwork_python.client import Client


# ── Logging setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_utc(dt: datetime) -> str:
    """Format a UTC datetime to the API-friendly ISO string (URL-encoded colons)."""
    return dt.strftime("%Y-%m-%dT%H%%3A%M%%3A%SZ")


def parse_utc(s: str) -> datetime:
    """Parse a UTC datetime string like '2025-01-01 08:00:00' into a datetime object."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H%%3A%M%%3A%SZ"):
        try:
            s_clean = s.replace("%3A", ":").replace("T", " ").rstrip("Z")
            return datetime.strptime(s_clean, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse UTC datetime: {s!r}")


def parse_local(s: str) -> datetime:
    """Parse a local datetime string like '2025-01-01 16:00:00' into a naive datetime."""
    s_clean = s.replace("%3A", ":").replace("T", " ").rstrip("Z")
    return datetime.strptime(s_clean, "%Y-%m-%d %H:%M:%S")


def fmt_local_display(dt: datetime) -> str:
    """Format a local datetime for display: 'Jan 01, 2025 16:00:05'."""
    return dt.strftime("%b %d, %Y %H:%M:%S")


def fmt_utc_event(dt: datetime) -> str:
    """Format a UTC datetime for the store-auxiliary --created argument."""
    return dt.strftime("%Y-%m-%d %H:%M:%SZ")


def get_boundary_results(
    node: str,
    sourceids: str,
    start_dt: datetime,
    end_dt: datetime,
    maxoutput: str,
    token: str,
    secret: str,
) -> list:
    """
    Call the SolarNetwork API for a UTC window and return the raw results list.
    Returns an empty list if the API returns nothing.
    """
    client = Client(token, secret)

    start_str = fmt_utc(start_dt)
    end_str = fmt_utc(end_dt)

    results, _ = solar_query_batch(
        client=client,
        node=node,
        sourceids=sourceids,
        startdate=start_str,
        enddate=end_str,
        aggregate="None",
        maxoutput=maxoutput,
        token=token,
        secret=secret,
        action="utc",
    )

    return results or []


def extract_watt_hours(record: dict) -> float:
    """Pull wattHours out of an API result record, defaulting to 0."""
    return float(record.get("wattHours", 0) or 0)


def record_utc_dt(record: dict) -> datetime:
    """Parse the 'created' field of an API record into a UTC datetime."""
    created = record.get("created", "")
    return datetime.fromisoformat(created.replace("Z", "+00:00"))


def get_csv_last_field(filepath: str, line: str = "first") -> float:
    """
    Read the first or last non-empty data line of a CSV and return the last
    comma-separated field as a float.  Skips the header row (line 1).
    """
    with open(filepath, "r") as f:
        lines = [l.strip() for l in f if l.strip()]

    # lines[0] is the header; data starts at lines[1]
    data_lines = lines[1:] if len(lines) > 1 else []
    if not data_lines:
        return 0.0

    target_line = data_lines[0] if line == "first" else data_lines[-1]
    return float(target_line.split(",")[-1])


# ── Core logic ────────────────────────────────────────────────────────────────

def determine_stitch_parameters(
    node: str,
    sourceids: str,
    sid: str,
    utcstartdate: str,
    utcenddate: str,
    localstartdate: str,
    localenddate: str,
    file_to_import: str,
    maxoutput: str,
    token: str,
    secret: str,
) -> dict:
    """
    Determines the SNE1 and SNE2 stitch parameters for a data gap.

    Returns a dict with all derived parameters so the caller can decide
    what to do with them (log, store, dry-run, etc.).
    """

    log.info("Determining Stitch Parameters")

    utc_start = parse_utc(utcstartdate)
    utc_end   = parse_utc(utcenddate)
    loc_start = parse_local(localstartdate)
    loc_end   = parse_local(localenddate)

    # ── SNE1: window 1 hour BEFORE the gap start ──────────────────────────────
    sne1_window_start = utc_start - timedelta(hours=1)
    sne1_window_end   = utc_start - timedelta(seconds=1)

    log.info(
        "Querying SNE1 boundary window: %s → %s",
        fmt_utc_event(sne1_window_start),
        fmt_utc_event(sne1_window_end),
    )

    sne1_results = get_boundary_results(
        node, sourceids, sne1_window_start, sne1_window_end, maxoutput, token, secret
    )

    log.info("Data at start border (last 5 rows):")
    for row in sne1_results[-5:]:
        log.info("  %s", row)

    if not sne1_results:
        # No data before the gap → use 5 seconds before gap start
        snevent1_local_dt = loc_start - timedelta(seconds=5)
        snevent1_utc_dt   = utc_start - timedelta(seconds=5)
        sne1_final_reading = 0.0
    else:
        last = sne1_results[-1]
        last_utc_dt = record_utc_dt(last)
        # Reconstruct the equivalent local datetime by shifting
        # (local = utc + (loc_start - utc_start) offset approximation)
        utc_offset = loc_start.replace(tzinfo=None) - utc_start.replace(tzinfo=None)
        last_local_dt = last_utc_dt.replace(tzinfo=None) + utc_offset

        snevent1_local_dt = last_local_dt + timedelta(seconds=5)
        snevent1_utc_dt   = last_utc_dt.replace(tzinfo=None) + timedelta(seconds=5)
        sne1_final_reading = extract_watt_hours(last)

    # ── SNE2: window 1 hour AFTER the gap end ────────────────────────────────
    sne2_window_start = utc_end + timedelta(seconds=1)
    sne2_window_end   = utc_end + timedelta(hours=1)

    log.info(
        "Querying SNE2 boundary window: %s → %s",
        fmt_utc_event(sne2_window_start),
        fmt_utc_event(sne2_window_end),
    )

    sne2_results = get_boundary_results(
        node, sourceids, sne2_window_start, sne2_window_end, maxoutput, token, secret
    )

    log.info("Data at end border (first 5 rows):")
    for row in sne2_results[:5]:
        log.info("  %s", row)

    if not sne2_results:
        # No data after the gap → use 5 seconds after gap end
        snevent2_local_dt = loc_end + timedelta(seconds=5)
        snevent2_utc_dt   = utc_end + timedelta(seconds=5)
        sne2_start_reading = 0.0
    else:
        first = sne2_results[0]
        first_utc_dt = record_utc_dt(first)
        utc_offset = loc_end.replace(tzinfo=None) - utc_end.replace(tzinfo=None)
        first_local_dt = first_utc_dt.replace(tzinfo=None) + utc_offset

        snevent2_local_dt  = first_local_dt - timedelta(seconds=5)
        snevent2_utc_dt    = first_utc_dt.replace(tzinfo=None) - timedelta(seconds=5)
        sne2_start_reading = extract_watt_hours(first)

    # ── Readings from the import file ─────────────────────────────────────────
    sne1_start_reading = get_csv_last_field(file_to_import, line="first")
    sne2_final_reading = get_csv_last_field(file_to_import, line="last")

    # ── Build result dict ─────────────────────────────────────────────────────
    params = {
        # SNE1
        "sne1": {
            "cause":        "Discontinuity due to Gap - Start Border",
            "description":  "Discontinuity due to Gap - Start Border",
            "node":         node,
            "source":       sid,
            "datetime_local":   fmt_local_display(snevent1_local_dt),
            "datetime_utc":     fmt_utc_event(snevent1_utc_dt),
            "final_reading":    sne1_final_reading,
            "start_reading":    sne1_start_reading,
        },
        # SNE2
        "sne2": {
            "cause":        "Discontinuity due to Gap - End Border",
            "description":  "Discontinuity due to Gap - End Border",
            "node":         node,
            "source":       sid,
            "datetime_local":   fmt_local_display(snevent2_local_dt),
            "datetime_utc":     fmt_utc_event(snevent2_utc_dt),
            "final_reading":    sne2_final_reading,
            "start_reading":    sne2_start_reading,
        },
    }

    return params


def log_summary(params: dict) -> None:
    """Log a human-readable summary and the ready-to-run store-auxiliary commands."""

    for event_key, label in [("sne1", "Solar Network Event 1"), ("sne2", "Solar Network Event 2")]:
        e = params[event_key]
        log.info("─" * 60)
        log.info(label)
        log.info("Cause:          %s", e["cause"])
        log.info("Description:    %s", e["description"])
        log.info("Node ID:        %s", e["node"])
        log.info("Source ID:      %s", e["source"])
        log.info("Start datetime: %s", e["datetime_local"])
        log.info("End datetime:   %s", e["datetime_local"])
        log.info("Final reading:  %s", e["final_reading"])
        log.info("Start reading:  %s", e["start_reading"])

    sne1 = params["sne1"]
    sne2 = params["sne2"]

    log.info("─" * 60)
    log.info("Store Auxiliary Script for Event 1:")
    log.info(
        "python3 solnet_store_auxiliary.py "
        "--node %s --source \"%s\" --type \"Reset\" "
        "--created \"%s\" "
        "--notes \"%s\" "
        "--final '{\"a\":{\"wattHours\":%s}}' "
        "--start '{\"a\":{\"wattHours\":%s}}' "
        "--token <token> --secret <secret>",
        sne1["node"], sne1["source"],
        sne1["datetime_utc"],
        sne1["description"],
        sne1["final_reading"],
        sne1["start_reading"],
    )

    log.info("Store Auxiliary Script for Event 2:")
    log.info(
        "python3 solnet_store_auxiliary.py "
        "--node %s --source \"%s\" --type \"Reset\" "
        "--created \"%s\" "
        "--notes \"%s\" "
        "--final '{\"a\":{\"wattHours\":%s}}' "
        "--start '{\"a\":{\"wattHours\":%s}}' "
        "--token <token> --secret <secret>",
        sne2["node"], sne2["source"],
        sne2["datetime_utc"],
        sne2["description"],
        sne2["final_reading"],
        sne2["start_reading"],
    )


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Determine SolarNetwork stitch parameters for a data gap."
    )
    parser.add_argument("--node",           required=True)
    parser.add_argument("--sourceids",      required=True)
    parser.add_argument("--sid",            required=True,
                        help="Source ID used in the SNE store-auxiliary command")
    parser.add_argument("--utcstartdate",   required=True,
                        help="Gap start in UTC: 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--utcenddate",     required=True,
                        help="Gap end in UTC:   'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--localstartdate", required=True,
                        help="Gap start in local time: 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--localenddate",   required=True,
                        help="Gap end in local time:   'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--file_to_import", required=True,
                        help="CSV file being imported (provides SNE1 start & SNE2 final readings)")
    parser.add_argument("--maxoutput",      required=True,
                        help="Max records per API batch")
    parser.add_argument("--token",          required=True)
    parser.add_argument("--secret",         required=True)

    args = parser.parse_args()

    params = determine_stitch_parameters(
        node=args.node,
        sourceids=args.sourceids,
        sid=args.sid,
        utcstartdate=args.utcstartdate,
        utcenddate=args.utcenddate,
        localstartdate=args.localstartdate,
        localenddate=args.localenddate,
        file_to_import=args.file_to_import,
        maxoutput=args.maxoutput,
        token=args.token,
        secret=args.secret,
    )

    log_summary(params)

    # Return the params dict so callers importing this module can use it directly
    return params


if __name__ == "__main__":
    main()
