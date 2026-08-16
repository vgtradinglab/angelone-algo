"""
SuperTrend Indicator
Signal: Bullish flip (BUY) or Bearish flip (SELL) on candle close
"""
import logging
from .base import get_candles

_log = logging.getLogger("SuperTrend")

def calculate_supertrend(candles, length=10, factor=3):
    """Calculate SuperTrend. Returns list of (value, direction) per candle."""
    if len(candles) < length + 1:
        return []
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    # Calculate ATR
    trs = []
    for i in range(1, len(candles)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i]  - closes[i-1]))
        trs.append(tr)
    # Smooth ATR
    atrs = []
    atr  = sum(trs[:length]) / length
    atrs.append(atr)
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
        atrs.append(atr)
    # Calculate SuperTrend
    results    = []
    prev_upper = prev_lower = prev_st = None
    prev_dir   = 1
    offset     = len(candles) - len(atrs)
    for i, atr in enumerate(atrs):
        idx    = i + offset
        hl2    = (highs[idx] + lows[idx]) / 2
        upper  = hl2 + factor * atr
        lower  = hl2 - factor * atr
        if prev_upper is not None:
            upper = upper if upper < prev_upper or closes[idx-1] > prev_upper else prev_upper
            lower = lower if lower > prev_lower or closes[idx-1] < prev_lower else prev_lower
        direction = 1
        if prev_st is not None:
            if prev_dir == -1 and closes[idx] > prev_upper:
                direction = 1
            elif prev_dir == 1 and closes[idx] < prev_lower:
                direction = -1
            else:
                direction = prev_dir
        st_val     = lower if direction == 1 else upper
        results.append((st_val, direction))
        prev_upper = upper
        prev_lower = lower
        prev_st    = st_val
        prev_dir   = direction
    return results

def get_signal(broker, exchange, symbol, timeframe, length=10, factor=3):
    """
    Returns signal: 'BUY', 'SELL', or None
    BUY  — SuperTrend flips bullish
    SELL — SuperTrend flips bearish
    """
    try:
        candles = get_candles(broker, exchange, symbol, timeframe, num_candles=length+20)
        if len(candles) < length + 2:
            return None, None
        results = calculate_supertrend(candles, length, factor)
        if len(results) < 2:
            return None, None
        curr_dir = results[-1][1]
        prev_dir = results[-2][1]
        curr_val = results[-1][0]
        signal   = None
        if prev_dir == -1 and curr_dir == 1:
            signal = 'BUY'
        elif prev_dir == 1 and curr_dir == -1:
            signal = 'SELL'
        _log.info(f"[SuperTrend] {symbol} {timeframe} L={length} F={factor} ST={curr_val:.2f} Dir={curr_dir} → {signal or 'WAIT'}")
        return signal, curr_val
    except Exception as e:
        _log.error(f"[SuperTrend] get_signal error: {e}")
        return None, None
