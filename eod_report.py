"""Offline EOD report: python eod_report.py --date YYYY-MM-DD."""
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
from trading_engine.audit_log import AuditLog

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--date", default=datetime.now(ZoneInfo("America/New_York")).date().isoformat())
parser.add_argument("--log-dir", default="logs")
parser.add_argument("--journal", default="sandbox_orders.json")
args = parser.parse_args()
print(AuditLog(args.log_dir).report(args.date, args.journal))
