import argparse
from datetime import datetime, timedelta
import sys

def get_args():
    parser = argparse.ArgumentParser(description="Run selected platforms with optional date range.")

    parser.add_argument(
        '--plat',
        nargs='+',
        help="Optional: platforms to run (e.g., thribee stripe). If omitted, runs all."
    )

    parser.add_argument(
        '--exclude',
        nargs='+',
        help="Optional: platforms to exclude (e.g., ga4 stripe)."
    )

    parser.add_argument(
        '--start',
        type=str,
        help="Optional: start date in DDMMYYYY format (must be used with --end)."
    )

    parser.add_argument(
        '--end',
        type=str,
        help="Optional: end date in DDMMYYYY format (must be used with --start)."
    )

    parser.add_argument(
        '--mode',
        type=str,
        choices=['daily', 'weekly', 'monthly'],
        default='daily',
        help="Optional: mode of operation (daily, weekly, monthly). Defaults to daily."
    )

    args = parser.parse_args()
    now = datetime.now()

    # Validate and parse dates
    if args.start and args.end:
        try:
            args.start = datetime.strptime(args.start, "%d%m%Y").replace(hour=0, minute=0, second=0, microsecond=0)
            args.end = datetime.strptime(args.end, "%d%m%Y").replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            print("❌ Dates must be in DDMMYYYY format (e.g., 15042025).")
            sys.exit(1)

        if args.end > now:
            print("❌ End date cannot be in the future.")
            sys.exit(1)

        if args.end < args.start:
            print("❌ End date cannot be before start date.")
            sys.exit(1)

    elif args.start or args.end:
        print("❌ Both --start and --end must be supplied together.")
        sys.exit(1)

    else:
        # Default date logic when no start/end is supplied
        if args.mode == 'daily':
            yesterday = now - timedelta(days=1)
            args.start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            args.end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

        elif args.mode == 'weekly':
            today_iso = now.isocalendar()
            last_week_monday = datetime.fromisocalendar(today_iso.year, today_iso.week - 1, 1)
            last_week_sunday = last_week_monday + timedelta(days=6)
            args.start = last_week_monday.replace(hour=0, minute=0, second=0, microsecond=0)
            args.end = last_week_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)

        elif args.mode == 'monthly':
            first_day_this_month = datetime(now.year, now.month, 1)
            last_day_last_month = first_day_this_month - timedelta(days=1)
            first_day_last_month = datetime(last_day_last_month.year, last_day_last_month.month, 1)
            args.start = first_day_last_month.replace(hour=0, minute=0, second=0, microsecond=0)
            args.end = last_day_last_month.replace(hour=23, minute=59, second=59, microsecond=999999)

    return args
