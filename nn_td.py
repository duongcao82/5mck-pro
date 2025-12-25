# src/NN_TD.py
"""
NN_TD.py
Helpers to fetch foreign ('khối ngoại') and proprietary / 'tự doanh' flows for VN market.

Usage:
    from src.NN_TD import get_nn_td_by_symbol, get_daily_market_flow
    df = get_nn_td_by_symbol("HPG")        # DataFrame with recent NN/TD flows
    market = get_daily_market_flow()       # Summary per index or aggregated

This module tries (in order):
 - to use vnstock_data.Trading (paid package) if available
 - else use vnstock.Vnstock functions if available
 - else fall back to an HTTP request skeleton (you must set URL/token)
"""
from pathlib import Path
from typing import Optional, List
import pandas as pd
import time
import os

CACHE_DIR = Path("data_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Try best available clients
HAS_VNSTOCK_DATA = False
HAS_VNSTOCK = False
try:
    from vnstock_data import Trading  # paid market data explorer (if installed)
    HAS_VNSTOCK_DATA = True
except Exception:
    HAS_VNSTOCK_DATA = False

try:
    from vnstock import Vnstock
    HAS_VNSTOCK = True
except Exception:
    HAS_VNSTOCK = False

# If you have a private API for NN/TD, configure here:
API_FLOWS_URL = os.environ.get("NN_TD_API_URL", "")  # e.g. https://myprovider/api/flows
API_TOKEN = os.environ.get("NN_TD_API_TOKEN", "")

def _read_cache(name: str, max_age_sec: int = 300) -> Optional[pd.DataFrame]:
    f = CACHE_DIR / f"{name}.csv"
    if not f.exists():
        return None
    age = time.time() - f.stat().st_mtime
    if age > max_age_sec:
        return None
    try:
        return pd.read_csv(f, parse_dates=True, index_col=0)
    except Exception:
        return None

def _write_cache(name: str, df: pd.DataFrame):
    f = CACHE_DIR / f"{name}.csv"
    try:
        df.to_csv(f)
    except Exception:
        pass

def get_nn_td_by_symbol(symbol: str, days: int = 30, use_cache: bool = True) -> pd.DataFrame:
    """
    Return DataFrame indexed by date with columns:
      ['date','nn_buy','nn_sell','nn_net','td_buy','td_sell','td_net']
    If data not available, returns empty DataFrame.
    """
    if not symbol:
        return pd.DataFrame()

    cache_name = f"nn_td_{symbol}"
    if use_cache:
        dfc = _read_cache(cache_name)
        if dfc is not None:
            return dfc

    df = pd.DataFrame()
    # 1) try vnstock_data.Trading if available
    if HAS_VNSTOCK_DATA:
        try:
            t = Trading()
            # method names depend on package version; adapt if needed
            # Common pattern: t.history(netflow=True, symbol=symbol, ...)
            raw = t.get_institution_flow(symbol=symbol, days=days)  # pseudo - adapt if needed
            # convert raw to DataFrame if raw is JSON-like
            if isinstance(raw, (list, dict)):
                df = pd.DataFrame(raw)
            elif hasattr(raw, "to_frame"):
                df = raw.to_frame()
        except Exception:
            df = pd.DataFrame()

    # 2) try vnstock.Vnstock scraping
    if df.empty and HAS_VNSTOCK:
        try:
            api = Vnstock()
            # vnstock may have function to get foreign flow per day; adapt as available
            raw = api.fund_flow(symbol=symbol, days=days)  # pseudo - check your vnstock version
            if isinstance(raw, (list, dict)):
                df = pd.DataFrame(raw)
        except Exception:
            df = pd.DataFrame()

    # 3) fallback to custom API if provided
    if df.empty and API_FLOWS_URL:
        try:
            import requests
            headers = {}
            if API_TOKEN:
                headers["Authorization"] = f"Bearer {API_TOKEN}"
            resp = requests.get(f"{API_FLOWS_URL}?symbol={symbol}&days={days}", headers=headers, timeout=8)
            if resp.status_code == 200:
                j = resp.json()
                df = pd.DataFrame(j)
        except Exception:
            df = pd.DataFrame()

    # Normalize expected columns
    if not df.empty:
        # try to make sure columns available
        # common names: date, nn_buy, nn_sell, nn_net, td_buy, td_sell, td_net
        # attempt to guess from keys
        colmap = {}
        lower = {c.lower(): c for c in df.columns}
        def _pick(*opts):
            for o in opts:
                if o in lower:
                    return lower[o]
            return None
        date_col = _pick("date","trade_date","day")
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col)
        # if nn columns exist with other names, try to map
        # simple heuristics:
        for std in ["nn_buy","nn_sell","nn_net","td_buy","td_sell","td_net"]:
            if std in df.columns:
                continue
            # try patterns
            if "foreign" in std or "nn" in std:
                # skip heuristic here; keep as-is
                pass
        # keep as best-effort
        df = df.sort_index()
        _write_cache(cache_name, df)

    return df

def get_daily_market_flow(days: int = 7) -> pd.DataFrame:
    """
    Aggregate NN and TD flows for top indexes or market-level summary.
    Returns DataFrame where rows = date, columns include aggregated net flows.
    """
    cache_name = "market_flow_summary"
    dfc = _read_cache(cache_name)
    if dfc is not None:
        return dfc

    df = pd.DataFrame()
    # If vnstock_data provides market-level endpoint try it
    if HAS_VNSTOCK_DATA:
        try:
            trading = Trading()
            raw = trading.get_market_flows(days=days)  # pseudo method - adapt
            df = pd.DataFrame(raw)
        except Exception:
            df = pd.DataFrame()

    if df.empty and API_FLOWS_URL:
        try:
            import requests
            headers = {}
            if API_TOKEN:
                headers["Authorization"] = f"Bearer {API_TOKEN}"
            resp = requests.get(f"{API_FLOWS_URL}/market?days={days}", headers=headers, timeout=8)
            if resp.status_code == 200:
                df = pd.DataFrame(resp.json())
        except Exception:
            df = pd.DataFrame()

    if not df.empty:
        _write_cache(cache_name, df)
    return df


# Example quick CLI usage (if run directly)
if __name__ == "__main__":
    print("Testing NN/TD scraper (best-effort, may require vnstock_data or API):")
    for sym in ["HPG","VCB","VNINDEX"]:
        df = get_nn_td_by_symbol(sym, days=30)
        print(sym, "rows:", len(df))
