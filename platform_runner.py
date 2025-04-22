import os
import importlib
from datetime import datetime, timedelta

def get_all_platforms():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    platforms_dir = os.path.join(base_dir, 'platforms')
    print("🔍 Looking in:", platforms_dir)

    if not os.path.exists(platforms_dir):
        print("❌ Folder not found!")
        return []

    folders = [
        name for name in os.listdir(platforms_dir)
        if os.path.isdir(os.path.join(platforms_dir, name)) and
           os.path.isfile(os.path.join(platforms_dir, name, '__init__.py'))
    ]

    return folders

def run_platforms(plat_names, mode, start_date, end_date):
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if end_date >= now:
        print("⚠️ End date is today or in the future. Adjusting to yesterday.")
        end_date = now - timedelta(days=1)

    def date_ranges():
        if mode == 'daily':
            curr = start_date
            while curr <= end_date:
                yield (
                    curr.replace(hour=0, minute=0, second=0, microsecond=0),
                    curr.replace(hour=23, minute=59, second=59, microsecond=999999)
                )
                curr += timedelta(days=1)
        elif mode == 'weekly':
            curr = start_date
            while curr <= end_date:
                week_start = curr - timedelta(days=curr.weekday())
                week_end = week_start + timedelta(days=6)
                yield (
                    week_start.replace(hour=0, minute=0, second=0, microsecond=0),
                    week_end.replace(hour=23, minute=59, second=59, microsecond=999999)
                )
                curr = week_end + timedelta(days=1)
        elif mode == 'monthly':
            curr = start_date.replace(day=1)
            while curr <= end_date:
                next_month = (curr.replace(day=28) + timedelta(days=4)).replace(day=1)
                last_day = next_month - timedelta(days=1)
                yield (
                    curr.replace(hour=0, minute=0, second=0, microsecond=0),
                    last_day.replace(hour=23, minute=59, second=59, microsecond=999999)
                )
                curr = next_month

    for plat_name in plat_names:
        try:
            mod = importlib.import_module(f"platforms.{plat_name}")
            if not hasattr(mod, 'main'):
                print(f"⚠️ {plat_name} has no main() function")
                continue

            for run_start, run_end in date_ranges():
                print(f"\n📆 Running {plat_name} for {run_start.date()} → {run_end.date()} ({mode})")
                mod.main(run_start, run_end, mode)

        except ModuleNotFoundError as e:
            import traceback
            print(f"❌ Failed to import {plat_name}: {e}")
            traceback.print_exc()
