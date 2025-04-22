from platforms.thribee.runner import run_display_and_bigquery

def main(start_date, end_date, mode):
    print(f"📡 Thribee runner active | Mode: {mode}")
    run_display_and_bigquery(start_date, end_date, mode)
