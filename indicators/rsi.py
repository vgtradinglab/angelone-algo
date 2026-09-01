"""
RSI — Relative Strength Index Indicator
Signal: RSI crosses levels on candle close
"""
import logging
from .base import get_candles

_log = logging.getLogger("RSI")

def calculate_rsi(closes, length=14):
    """Calculate RSI for given closes and length."""
    if len(closes) < length + 1:
        return None
    gains  = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length
    for i in range(length, len(gains)):
        avg_gain = (avg_gain * (length-1) + gains[i]) / length
        avg_loss = (avg_loss * (length-1) + losses[i]) / length
    if avg_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)

def get_signal(broker, exchange, symbol, timeframe,
               length=14, upper=70, lower=30, use_middle=False, middle=50):
    """
    Returns signal: 'BUY', 'SELL', or None
    BUY  — RSI crosses above lower (30) from below
    SELL — RSI crosses below upper (70) from above
    If use_middle: BUY above 50, SELL below 50
    """
    try:
        candles = get_candles(broker, exchange, symbol, timeframe, num_candles=length+2)
        # Filter to today's session only — RSI should not use previous day candles
        from datetime import datetime, date
        _today = date.today()
        _market_open_ts = int(datetime(_today.year, _today.month, _today.day, 9, 0).timestamp())
        candles = [c for c in candles if c[0] >= _market_open_ts]
        if len(candles) < length + 2:
            return None, None
        closes   = [c[4] for c in candles]
        curr_rsi = calculate_rsi(closes, length)
        prev_rsi = calculate_rsi(closes[:-1], length)
        if curr_rsi is None or prev_rsi is None:
            return None, None
        signal = None
        if use_middle:
            if prev_rsi <= middle and curr_rsi > middle:
                signal = 'BUY'
            elif prev_rsi >= middle and curr_rsi < middle:
                signal = 'SELL'
        else:
            if prev_rsi <= lower and curr_rsi > lower:
                signal = 'BUY'
            elif prev_rsi >= upper and curr_rsi < upper:
                signal = 'SELL'
        _log.info(f"[RSI] {symbol} {timeframe} L={length} RSI={curr_rsi} → {signal or 'WAIT'}")
        return signal, curr_rsi
    except Exception as e:
        _log.error(f"[RSI] get_signal error: {e}")
        return None, None
