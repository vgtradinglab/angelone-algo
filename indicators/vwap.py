"""
VWAP — Volume Weighted Average Price Indicator
Signal: Price crosses above/below VWAP on candle close
"""
import logging
from .base import get_candles

_log = logging.getLogger("VWAP")

def calculate_vwap(candles):
    """
    Calculate session VWAP from candles.
    VWAP = Sum(typical_price * volume) / Sum(volume)
    """
    try:
        total_tp_vol = 0.0
        total_vol    = 0.0
        for c in candles:
            high   = c[2]
            low    = c[3]
            close  = c[4]
            volume = c[5]
            tp     = (high + low + close) / 3
            total_tp_vol += tp * volume
            total_vol    += volume
        if total_vol == 0:
            # No volume data — use simple average of typical prices
            total_tp = sum((c[2]+c[3]+c[4])/3 for c in candles)
            return round(total_tp / len(candles), 2)
        return round(total_tp_vol / total_vol, 2)
    except Exception as e:
        _log.error(f"[VWAP] calculate_vwap error: {e}")
        return None

def get_signal(broker, exchange, symbol, timeframe):
    """
    Returns signal: 'BUY', 'SELL', or None
    BUY  — Price closes above VWAP
    SELL — Price closes below VWAP
    """
    try:
        # Fetch ALL today's candles for accurate session VWAP
        all_candles = get_candles(broker, exchange, symbol, timeframe, num_candles=300)
        if len(all_candles) < 2:
            return None, None
        # Filter only today's candles for VWAP
        from datetime import date
        import time as _t
        today_start = _t.mktime(date.today().timetuple())
        today_candles = [c for c in all_candles if c[0] >= today_start]
        if len(today_candles) < 2:
            today_candles = all_candles  # fallback
        vwap       = calculate_vwap(today_candles)
        candles    = today_candles  # use today's candles for crossover
        if vwap is None:
            return None, None
        curr_close = candles[-1][4]
        prev_close = candles[-2][4]
        signal     = None
        if prev_close <= vwap and curr_close > vwap:
            signal = 'BUY'
        elif prev_close >= vwap and curr_close < vwap:
            signal = 'SELL'
        _log.info(f"[VWAP] {symbol} {timeframe} VWAP={vwap} Price={curr_close} → {signal or 'WAIT'}")
        return signal, vwap
    except Exception as e:
        _log.error(f"[VWAP] get_signal error: {e}")
        return None, None
