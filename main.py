from datetime import datetime
from cli_args import get_args
from platform_runner import get_all_platforms, run_platforms

from dotenv import load_dotenv
load_dotenv()

if __name__ == '__main__':
    local_time = datetime.now().astimezone()
    print("🕒", local_time.strftime('%Y-%m-%d %H:%M:%S %Z%z'))

    args = get_args()
    all_plats = get_all_platforms()

    if args.plat:
        plat_names = args.plat
    else:
        plat_names = all_plats

    if args.exclude:
        plat_names = [p for p in plat_names if p not in args.exclude]
        print(f"🧼 Excluding platforms: {args.exclude}")

    print(f"🚀 Running platforms: {plat_names}")
    run_platforms(plat_names,
                  mode=args.mode,
                  start_date=args.start,
                  end_date=args.end)
