import json
import re
import ssl
import requests
from urllib3 import poolmanager

import os

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import pickle

from ..common.markets_request_headers import thribee_header


# ---------------- GA4 conversion fetching ----------------

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
GA4_CREDENTIALS = 'secrets/client_secret.json'
GA4_TOKEN = 'secrets/token.pickle'

def authenticate_analytics():
    creds = None
    if os.path.exists(GA4_TOKEN):
        with open(GA4_TOKEN, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GA4_CREDENTIALS, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(GA4_TOKEN, 'wb') as token:
                pickle.dump(creds, token)
    return creds

def fetch_thribee_ga4_conversions(start_date, end_date, ga4_property_id: str) -> float:
    creds = authenticate_analytics()
    analytics_data = build('analyticsdata', 'v1beta', credentials=creds)

    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    response = analytics_data.properties().runReport(
        property=f"properties/{ga4_property_id}",
        body={
            "dateRanges": [{"startDate": start_str, "endDate": end_str}],
            "dimensions": [{"name": "eventName"}, {"name": "sessionSourceMedium"}],
            "metrics": [{"name": "conversions"}],
        }
    ).execute()

    total_conversion = 0
    for row in response.get('rows', []):
        source_medium = row.get('dimensionValues', [{}])[1].get('value', '')
        event = row.get('dimensionValues', [{}])[0].get('value', '')
        if event == 'premium_signup' and any(k in source_medium for k in ['hribee', 'itula', 'rovit', 'iful']):
            total_conversion += float(row.get('metricValues', [{}])[0].get('value', 0))

    return total_conversion

# ---------------- GA4 conversion fetching ----------------

class TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLS,
            ssl_context=ctx
        )

def login():
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'thribee.com',
        'Origin': 'https://thribee.com',
        'Referer': 'https://thribee.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
    }
    session = requests.Session()
    session.mount('https://', TLSAdapter())
    resp = session.post(
        'https://thribee.com/index.php/cod.campaigns_performance',
        data={
            "email": os.getenv("THRIBEE_EMAIL"),
            "password": os.getenv("THRIBEE_PASSWORD")
            },
        headers=headers,
        allow_redirects=False
    )

    cookie = resp.headers.get('Set-Cookie', '')
    match = re.search('user=([A-Za-z0-9]{90});', cookie)
    if not match:
        raise ValueError('Failed to extract Thribee user cookie')
    return match.group(1)

def fetch_cost_and_campaigns(thribee_id, start_date, end_date, ga4_property_id=None):
    start = start_date.strftime('%Y-%m-%d')
    end = end_date.strftime('%Y-%m-%d')
    user_cookie = login()

    session = requests.Session()
    session.mount('https://', TLSAdapter())
    headers = thribee_header.copy()
    headers['Cookie'] = f"user={user_cookie}; dToken=1661422607196; curPartner={thribee_id}; _gat=1"

    body = f"device=&startDate={start}&endDate={end}&parts%5B%5D=list&parts%5B%5D=graph&tz_offset=-120&ajaxCall=1"

    response = session.post(
        "https://thribee.com/index.php/cod.source_overview_report_json",
        headers=headers,
        data=body
    )

    data = json.loads(response.text).get('data', {}).get('list', {})
    cost = data.get('totals', {}).get('cost')
    campaigns = data.get('lines', [])

    conversions = None
    if ga4_property_id:
        try:
            conversions = fetch_thribee_ga4_conversions(start_date, end_date, ga4_property_id)
        except Exception as e:
            print(f"[!] GA4 conversion fetch failed: {e}")
            conversions = None

    market_data = {
        "cost": cost,
        "conversions": conversions
    }

    return market_data, campaigns

