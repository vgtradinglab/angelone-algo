"""
EMA — Exponential Moving Average Indicator
Signal: Price crosses above/below EMA on candle close
"""
import logging
from .base import get_candles

_log = logging.getLogger("EMA")

def calculate_ema(closes, length):
    """Calculate EMA for given closes and length."""
    if len(closes) < length:
        return None
    k = 2 / (length + 1)
    ema = sum(closes[:length]) / length
    for price in closes[length:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 2)

def get_signal(broker, exchange, symbol, timeframe, length=9):
    """
    Returns signal: 'BUY', 'SELL', or None
    BUY  — price closes above EMA
    SELL — price closes below EMA
    """
    try:
        candles = get_candles(broker, exchange, symbol, timeframe, num_candles=length+10)
        if len(candles) < length + 2:
            return None, None, None
        closes      = [c[4] for c in candles]
        prev_closes = closes[:-1]
        curr_price  = closes[-1]
        prev_price  = closes[-2]
        curr_ema    = calculate_ema(closes, length)
        prev_ema    = calculate_ema(prev_closes, length)
        if curr_ema is None or prev_ema is None:
            return None, curr_ema, curr_price
        signal = None
        if prev_price <= prev_ema and curr_price > curr_ema:
            signal = 'BUY'
        elif prev_price >= prev_ema and curr_price < curr_ema:
            signal = 'SELL'
        _log.info(f"[EMA] {symbol} {timeframe} L={length} EMA={curr_ema} Price={curr_price} → {signal or 'WAIT'}")
        return signal, curr_ema, curr_price
    except Exception as e:
        _log.error(f"[EMA] get_signal error: {e}")
        return None, None, None
