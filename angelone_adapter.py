import logging, threading, time, requests, queue
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp

_log = logging.getLogger("AngelOneAdapter")

SCRIP_MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

EXCHANGE_TYPE = {"NSE":1,"NFO":2,"BSE":3,"BFO":4,"MCX":5}
EXCHANGE_MAP  = {"nse_cm":"NSE","nse_fo":"NFO","bse_cm":"BSE","bse_fo":"BFO","mcx_fo":"MCX","NSE":"NSE","NFO":"NFO","BSE":"BSE","BFO":"BFO","MCX":"MCX"}
PRODUCT_MAP   = {"MIS":"INTRADAY","NRML":"CARRYFORWARD","CNC":"DELIVERY"}
OTYPE_MAP     = {"L":"LIMIT","M":"MARKET","SL":"STOPLOSS_LIMIT","SL-M":"STOPLOSS_MARKET"}

import threading as _threading
_CANDLE_LOCK = _threading.Lock()
_CANDLE_LAST_TS = [0.0]  # shared across all instances

class AngelOneAdapter:
    name = "Angel One"

    def __init__(self):
        self._api_key=self._client_code=self._password=self._totp_key=""
        self._jwt_token=self._refresh_token=self._feed_token=""
        self._smart_api=self._on_tick_cb=self._notifier=None
        self._instruments={}
        self._sub_tokens=[]
        self._connected=False
        self._last_order_error=""
        self._last_tick_ts=0.0
        self._feed_healthy=False
        self._watchdog_started=self._watchdog_stop=False
        self._ws=None
        self._ws_connected=threading.Event()
        self._tick_queue=queue.Queue(maxsize=10000)
        # ── Local candle cache (built from WebSocket ticks — zero REST API calls) ──
        self._candle_cache={}      # {symbol: [closed_candles]}
        self._candle_current={}    # {symbol: {o,h,l,c,ts,token}}
        self._candle_lock=threading.Lock()
        self._token_symbol_map={}  # {token: symbol} reverse lookup
        self._load_candle_cache()  # load yesterday's candles from disk
        self._worker_started=self._worker_stop=False
        self._lock=threading.Lock()

    def set_notifier(self, n): self._notifier=n
    def get_last_order_error(self): return self._last_order_error

    def login(self, api_key="", client_code="", password="", totp_key="", **kw):
        try:
            self._api_key=api_key.strip()
            self._client_code=client_code.strip()
            self._password=password.strip()
            self._totp_key=totp_key.strip()
            self._smart_api=SmartConnect(api_key=self._api_key)
            totp=pyotp.TOTP(self._totp_key).now()
            _log.info(f"[AngelOne] Logging in as {self._client_code}...")
            data=self._smart_api.generateSession(self._client_code,self._password,totp)
            if not data or data.get("status")==False:
                _log.error(f"[AngelOne] Login failed: {data.get('message','') if data else 'No response'}")
                return False
            self._jwt_token=data["data"]["jwtToken"]
            self._refresh_token=data["data"]["refreshToken"]
            self._feed_token=self._smart_api.getfeedToken()
            profile=self._smart_api.getProfile(self._refresh_token)
            name=profile.get("data",{}).get("name","") if profile else ""
            _log.info(f"[AngelOne] Login OK — {name} ({self._client_code})")
            self._connected=True
            self._download_instruments()
            self._start_token_refresh()
            return True
        except Exception as e:
            _log.error(f"[AngelOne] Login error: {e}")
            return False

    def _start_token_refresh(self):
        def _loop():
            while not self._watchdog_stop:
                time.sleep(6*3600)
                try:
                    data=self._smart_api.generateToken(self._refresh_token)
                    if data and data.get("status")!=False:
                        self._jwt_token=data["data"]["jwtToken"]
                        self._refresh_token=data["data"]["refreshToken"]
                        _log.info("[AngelOne] Token refreshed.")
                except Exception as e:
                    _log.warning(f"[AngelOne] Token refresh error: {e}")
        threading.Thread(target=_loop,name="TokenRefresh",daemon=True).start()

    def _process_instruments(self, data):
        SKIP_SEGMENTS = {"NCO","CDS","NCDEX"}
        count = 0
        for item in data:
            sym=item.get("symbol","").strip()
            token=item.get("token","").strip()
            exch=item.get("exch_seg","").strip()
            if exch in SKIP_SEGMENTS: continue
            if sym and token:
                self._instruments[f"{exch}:{sym}"]={
                    "token":token,"symbol":sym,"exchange":exch,
                    "name":item.get("name","").strip(),
                    "lotsize":item.get("lotsize","1"),
                    "expiry":item.get("expiry",""),
                    "strike":item.get("strike",""),
                    "opttype":item.get("instrumenttype",""),
                    "tick_size":item.get("tick_size","0.05"),
                }
                count+=1
        return count

    def _download_instruments(self):
        import os, json as _json
        from datetime import date
        CACHE_FILE = "/tmp/angelone_instruments_cache.json"
        CACHE_DATE_FILE = "/tmp/angelone_instruments_cache_date.txt"
        today = date.today().isoformat()
        # Use cache if downloaded today
        if os.path.exists(CACHE_FILE) and os.path.exists(CACHE_DATE_FILE):
            try:
                cached_date = open(CACHE_DATE_FILE).read().strip()
                if cached_date == today:
                    _log.info("[AngelOne] Loading instruments from local cache...")
                    data = _json.load(open(CACHE_FILE))
                    count = self._process_instruments(data)
                    _log.info(f"[AngelOne] Instruments loaded from cache: {count} symbols.")
                    return
            except Exception as _ce:
                _log.warning(f"[AngelOne] Cache load failed: {_ce} — downloading fresh.")
        try:
            _log.info("[AngelOne] Downloading instrument master...")
            r=requests.get(SCRIP_MASTER_URL,timeout=60)
            data=r.json()
            count = self._process_instruments(data)
            _log.info(f"[AngelOne] Instruments loaded: {count} symbols.")
            # Save to local cache
            try:
                _json.dump(data, open(CACHE_FILE,'w'))
                open(CACHE_DATE_FILE,'w').write(today)
                _log.info(f"[AngelOne] Instruments cached locally for {today}")
            except Exception as _se:
                _log.warning(f"[AngelOne] Cache save failed: {_se}")
        except Exception as e:
            _log.error(f"[AngelOne] Instrument download failed: {e}")

    def get_symbol_token(self,exchange,symbol):
        return self._instruments.get(f"{exchange}:{symbol}",{}).get("token","")

    def get_option_chain(self,instrument,expiry_str):
        results=[]
        try:
            for key,info in self._instruments.items():
                if info.get("name","").upper() != instrument.upper(): continue
                if info.get("expiry","") != expiry_str: continue
                opttype = info.get("opttype","")
                sym     = info.get("symbol","").upper()
                # Angel One uses OPTIDX/OPTSTK/OPTFUT — extract CE/PE from symbol
                if opttype in ("OPTIDX","OPTSTK","OPTFUT"):
                    if sym.endswith("CE"):   opttype = "CE"
                    elif sym.endswith("PE"): opttype = "PE"
                    else: continue
                elif opttype not in ("CE","PE"):
                    continue
                results.append({
                    "instrument_token":info["token"],
                    "tradingsymbol":info["symbol"],
                    "exchange":info["exchange"],
                    "strike":float(info.get("strike",0) or 0)/100,
                    "instrument_type":opttype,
                    "name":info.get("name",""),
                    "lot_size":int(info.get("lotsize",1) or 1),
                    "expiry":info["expiry"],
                    "tick_size":float(info.get("tick_size",0.05) or 0.05),
                })
        except Exception as e:
            _log.error(f"[AngelOne] get_option_chain error: {e}")
        return results

    def get_fut_chain(self,instrument,expiry_str):
        results=[]
        try:
            for key,info in self._instruments.items():
                if(info.get("name","").upper()==instrument.upper() and
                   info.get("expiry","")==expiry_str and
                   info.get("opttype","") in ("FUTIDX","FUTSTK","FUTCOM")):
                    results.append({
                        "instrument_token":info["token"],
                        "tradingsymbol":info["symbol"],
                        "exchange":info["exchange"],
                        "lot_size":int(info.get("lotsize",1) or 1),
                        "expiry":info["expiry"],
                        "tick_size":float(info.get("tick_size",0.05) or 0.05),
                        "instrument_type":"FUT",
                        "strike":0,
                    })
        except Exception as e:
            _log.error(f"[AngelOne] get_fut_chain error: {e}")
        return results

    def get_available_expiries(self,instrument,opt_only=False):
        expiries=set()
        try:
            from datetime import datetime, date
            today=date.today()
            # For MCX: separate options(OPTFUT) from futures(FUTCOM)
            # opt_only=True returns only OPTFUT expiries (for option chain)
            # opt_only=False returns all valid expiries
            mcx_opt_types=("OPTFUT",)
            mcx_fut_types=("FUTCOM",)
            nse_types=("CE","PE","OPTIDX","OPTSTK","FUTIDX","FUTSTK","OPTFUT")
            for key,info in self._instruments.items():
                if info.get("name","").upper()!=instrument.upper(): continue
                ot=info.get("opttype","")
                exp_str=info.get("expiry","")
                if not exp_str: continue
                if opt_only:
                    if ot not in mcx_opt_types+("CE","PE","OPTIDX","OPTSTK"): continue
                else:
                    if ot not in mcx_opt_types+mcx_fut_types+("CE","PE","OPTIDX","OPTSTK","FUTIDX","FUTSTK"): continue
                try:
                    exp_d=datetime.strptime(exp_str,"%d%b%Y").date()
                    if exp_d>=today:
                        expiries.add(exp_str)
                except: pass
            return sorted(list(expiries), key=lambda x: datetime.strptime(x,"%d%b%Y"))
        except Exception as e:
            _log.error(f"[AngelOne] get_available_expiries error: {e}")
            return []

    def supports_mcx_options(self): return True

    def subscribe_feed(self,tokens,on_tick):
        """Zerodha-pattern: first call starts WS, subsequent calls add tokens to live connection."""
        self._on_tick_cb=on_tick
        if not self._worker_started:
            self._worker_started=True
            threading.Thread(target=self._tick_worker,daemon=True,name="TickWorker").start()
        # Deduplicate — only subscribe new tokens not already subscribed
        existing=set(str(t.get("instrument_token","")) for t in self._sub_tokens)
        new_tokens=[t for t in tokens if str(t.get("instrument_token","")) not in existing]
        if not new_tokens and self._ws is not None:
            _log.info("[AngelOne] No new tokens to subscribe")
            return
        self._sub_tokens=self._sub_tokens+new_tokens
        # Register token→instrument mapping for candle builder
        # Only register INDEX tokens and MCX FUTURES tokens — not option tokens
        _KNOWN_INSTRS = {"NIFTY","BANKNIFTY","SENSEX","CRUDEOIL","CRUDEOILM","NATURALGAS","NATGASMINI"}
        for t in new_tokens:
            tok = str(t.get("instrument_token",""))
            sym = str(t.get("instrument","") or t.get("tradingsymbol",""))
            exch = str(t.get("exchange_segment","") or t.get("exchange","")).upper()
            # Only register if it is a known base instrument (index or MCX futures)
            if tok and sym and sym in _KNOWN_INSTRS:
                self.register_symbol_token(sym, tok)
        if self._ws is None:
            # First call — create WebSocket connection
            self._connect_websocket()
        else:
            # Subsequent call — add tokens to live connection
            try:
                self._subscribe_tokens(new_tokens)
                _log.info(f"[AngelOne] Added {len(new_tokens)} tokens (total: {len(self._sub_tokens)})")
            except Exception as e:
                _log.warning(f"[AngelOne] Add tokens failed: {e}")
        if not self._watchdog_started:
            self._watchdog_started=True
            threading.Thread(target=self._watchdog,daemon=True,name="WSWatchdog").start()

    def _subscribe_tokens(self,tokens_list):
        """Subscribe tokens in batches of 999 — AngelOne max is 1000 per subscribe call."""
        if not tokens_list: return
        all_pairs=[]
        for t in tokens_list:
            tok=str(t.get("instrument_token",""))
            exch=(t.get("exchange","") or t.get("exchange_segment","NSE")).upper()
            exch=EXCHANGE_MAP.get(exch,exch)
            etype=EXCHANGE_TYPE.get(exch,1)
            if tok: all_pairs.append((etype,tok))
        BATCH=999
        total=0
        for i in range(0,len(all_pairs),BATCH):
            batch=all_pairs[i:i+BATCH]
            exch_tokens={}
            for etype,tok in batch:
                if etype not in exch_tokens: exch_tokens[etype]=[]
                exch_tokens[etype].append(tok)
            token_list=[{"exchangeType":k,"tokens":v} for k,v in exch_tokens.items() if v]
            if token_list:
                self._ws.subscribe("algo01",1,token_list)
                total+=len(batch)
                _log.info(f"[AngelOne] Subscribed batch {i//BATCH+1}: {len(batch)} tokens (total so far: {total})")
                if i+BATCH<len(all_pairs):
                    time.sleep(0.5)

    def _connect_websocket(self):
        """Create and start single SmartWebSocketV2 connection (official AngelOne pattern)."""
        try:
            self._ws=SmartWebSocketV2(
                self._jwt_token,self._api_key,self._client_code,self._feed_token,
                max_retry_attempt=0)
            self._ws.on_open =self._on_open
            self._ws.on_data =self._on_data
            self._ws.on_error=self._on_error
            self._ws.on_close=self._on_close
            threading.Thread(target=self._ws.connect,daemon=True,name="WSConnect").start()
            _log.info("[AngelOne] WebSocket connecting...")
        except Exception as e:
            _log.error(f"[AngelOne] {name} connect error: {e}")
            return None

    def _on_open(self,wsapp):
        """Called by SDK when WebSocket connects. Subscribe all accumulated tokens."""
        _log.info("[AngelOne] WebSocket connected.")
        self._ws_connected.set()
        self._last_tick_ts=time.time()
        self._feed_healthy=True
        # Resubscribe all tokens on every connect/reconnect
        if self._sub_tokens:
            try:
                self._subscribe_tokens(self._sub_tokens)
            except Exception as e:
                _log.error(f"[AngelOne] Subscribe error on open: {e}")

    def _on_data(self,wsapp,message):
        """Called by SDK for each tick. Queue for async processing."""
        try:
            self._last_tick_ts=time.time()
            self._feed_healthy=True
            self._tick_queue.put_nowait(message)
        except queue.Full:
            pass

    def _on_error(self,wsapp,error):
        _log.warning(f"[AngelOne] WebSocket error: {error}")
        self._feed_healthy=False

    def _on_close(self,wsapp,close_status_code=None,close_msg=None):
        _log.warning("[AngelOne] WebSocket closed.")
        self._feed_healthy=False
        self._ws_connected.clear()

    def _tick_worker(self):
        """Background thread: drain tick queue and call on_tick_cb."""
        while not self._worker_stop:
            try:
                msg=self._tick_queue.get(timeout=1)
                if self._on_tick_cb and isinstance(msg,dict):
                    token=str(msg.get("token",""))
                    ltp=float(msg.get("last_traded_price",0))/100
                    if token and ltp>0:
                        self._on_tick_cb(token,ltp)
                        self._build_candle(token,ltp)
            except queue.Empty: continue
            except Exception as e: _log.error(f"[AngelOne] Tick worker error: {e}")

    def _reconnect_ws(self,num):
        """Close and reconnect one WS connection."""
        try:
            if num==1:
                try:
                    if self._ws1: self._ws1.close_connection()
                except: pass
                self._ws=self._ws1
            elif num==2:
                try:
                    if self._ws2: self._ws2.close_connection()
                except: pass
            elif num==3:
                try:
                    if self._ws3: self._ws3.close_connection()
                except: pass
        except Exception as e:
            _log.error(f"[AngelOne] Reconnect WS{num} error: {e}")

    def _watchdog(self):
        """Monitor feed health. Reconnect if stale for 120 seconds."""
        while not self._watchdog_stop:
            time.sleep(30)
            try:
                if time.time()-self._last_tick_ts>120 and self._connected:
                    _log.warning("[AngelOne] Feed stale — reconnecting all connections...")
                    self._feed_healthy=False
                    if self._notifier:
                        self._notifier.telegram("[Angel One] Feed stale — reconnecting...")
                    try:
                        if self._ws: self._ws.close_connection()
                    except: pass
                    self._ws=None
                    self._ws_connected.clear()
                    time.sleep(3)
                    self._connect_websocket()
            except Exception as e:
                _log.error(f"[AngelOne] Watchdog error: {e}")

    def unsubscribe_feed(self,tokens):
        try:
            if self._ws:
                exch_tokens={}
                for t in tokens:
                    tok=str(t.get("instrument_token",""))
                    exch=EXCHANGE_MAP.get((t.get("exchange","") or t.get("exchange_segment","NSE")).upper(),"NSE")
                    etype=EXCHANGE_TYPE.get(exch,1)
                    if etype not in exch_tokens: exch_tokens[etype]=[]
                    if tok: exch_tokens[etype].append(tok)
                token_list=[{"exchangeType":k,"tokens":v} for k,v in exch_tokens.items() if v]
                if token_list: self._ws.unsubscribe("algo01",1,token_list)
        except Exception as e: _log.error(f"[AngelOne] Unsubscribe error: {e}")

    def place_order(self,exchange,symbol,qty,side,price,order_type,product,tag=""):
        try:
            ao_exch=EXCHANGE_MAP.get(exchange,exchange.upper())
            token=self.get_symbol_token(ao_exch,symbol)
            if not token:
                self._last_order_error=f"Token not found for {ao_exch}:{symbol}"
                _log.error(f"[AngelOne] {self._last_order_error}")
                return ""
            params={
                "variety":"NORMAL",
                "tradingsymbol":symbol,
                "symboltoken":token,
                "transactiontype":"BUY" if side.upper() in ("B","BUY") else "SELL",
                "exchange":ao_exch,
                "ordertype":OTYPE_MAP.get(order_type.upper(),"LIMIT"),
                "producttype":PRODUCT_MAP.get(product.upper(),"INTRADAY"),
                "duration":"DAY",
                "price":str(round(price,2)),
                "squareoff":"0","stoploss":"0",
                "quantity":str(qty),
                "ordertag":tag[:20] if tag else "",
            }
            _log.info(f"[AngelOne] {params['transactiontype']} {ao_exch}:{symbol} qty={qty} price={price}")
            r=self._smart_api.placeOrder(params)
            if r and r.get("status")!=False:
                oid=r.get("data","")
                _log.info(f"[AngelOne] Order OK — {oid}")
                return str(oid)
            self._last_order_error=r.get("message","Unknown") if r else "No response"
            _log.error(f"[AngelOne] Order failed: {self._last_order_error}")
            return ""
        except Exception as e:
            self._last_order_error=str(e)
            _log.error(f"[AngelOne] place_order error: {e}")
            return ""

    def get_order_status(self,order_id):
        try:
            r=self._smart_api.orderBook()
            if r and r.get("status")!=False:
                for o in (r.get("data") or []):
                    if str(o.get("orderid",""))==str(order_id):
                        s=o.get("orderstatus","").upper()
                        fp=float(o.get("averageprice",0) or 0)
                        if s=="COMPLETE": return {"status":"COMPLETE","fill_price":fp,"reason":""}
                        if s in ("REJECTED","CANCELLED"): return {"status":"REJECTED","fill_price":0.0,"reason":o.get("text","")}
                        return {"status":"PENDING","fill_price":0.0,"reason":""}
        except Exception as e: _log.error(f"[AngelOne] get_order_status error: {e}")
        return {"status":"PENDING","fill_price":0.0,"reason":""}

    def cancel_order(self,order_id):
        try:
            self._smart_api.cancelOrder("NORMAL",order_id)
            _log.info(f"[AngelOne] Order cancelled: {order_id}")
        except Exception as e: _log.error(f"[AngelOne] cancel_order error: {e}")

    def modify_order(self,order_id,price,qty=0,order_type="L"):
        try:
            params={"variety":"NORMAL","orderid":order_id,
                    "ordertype":OTYPE_MAP.get(order_type.upper(),"LIMIT"),
                    "price":str(round(price,2))}
            if qty: params["quantity"]=str(qty)
            r=self._smart_api.modifyOrder(params)
            if r and r.get("status")!=False:
                _log.info(f"[AngelOne] Order modified: {order_id}")
                return True
            _log.error(f"[AngelOne] Modify failed: {r.get('message','') if r else ''}")
            return False
        except Exception as e:
            _log.error(f"[AngelOne] modify_order error: {e}")
            return False

    def get_positions(self):
        try:
            r=self._smart_api.position()
            if r and r.get("status")!=False: return r.get("data") or []
        except Exception as e: _log.error(f"[AngelOne] get_positions error: {e}")
        return []

    def get_order_book(self):
        try:
            r=self._smart_api.orderBook()
            if r and r.get("status")!=False: return r.get("data") or []
        except Exception as e: _log.error(f"[AngelOne] get_order_book error: {e}")
        return []

    def get_trade_book(self):
        try:
            r=self._smart_api.tradeBook()
            if r and r.get("status")!=False: return r.get("data") or []
        except Exception as e: _log.error(f"[AngelOne] get_trade_book error: {e}")
        return []

    def get_funds(self):
        try:
            r=self._smart_api.rmsLimit()
            if r and r.get("status")!=False: return r.get("data") or {}
        except Exception as e: _log.error(f"[AngelOne] get_funds error: {e}")
        return {}

    def get_candles(self,exchange,symbol,interval,from_date,to_date):
        """Return candles from local WebSocket-built cache. Zero REST API calls."""
        try:
            # Aggregate 5 Min candles into requested interval
            INTERVAL_MINS = {"FIVE_MINUTE":5,"FIFTEEN_MINUTE":15,"THIRTY_MINUTE":30,"ONE_HOUR":60}
            target_mins = INTERVAL_MINS.get(interval, 5)
            base_mins   = 5  # we store 5 Min candles
            # Normalize symbol — strip expiry suffix for MCX futures
            # e.g. "CRUDEOIL19AUG26FUT" → "CRUDEOIL"
            _sym = symbol
            for _sfx in ["FUT","CE","PE"]:
                if _sym.endswith(_sfx):
                    # Extract base name by removing date+suffix
                    import re as _re
                    _base = _re.sub(r'\d{2}[A-Z]{3}\d{2}(FUT|CE|PE)$','',_sym)
                    if _base and _base in self._candle_cache:
                        _sym = _base
                        break
            with self._candle_lock:
                candles = list(self._candle_cache.get(_sym, []))
            if not candles:
                return []
            if target_mins == base_mins:
                return candles
            # Aggregate: combine N×5Min candles into target timeframe
            result = []
            n = target_mins // base_mins
            for i in range(0, len(candles)-n+1, n):
                group = candles[i:i+n]
                if len(group) < n: break
                agg = [
                    group[0][0],                          # timestamp
                    group[0][1],                          # open
                    max(c[2] for c in group),             # high
                    min(c[3] for c in group),             # low
                    group[-1][4],                         # close
                    sum(c[5] for c in group),             # volume
                ]
                result.append(agg)
            return result
        except Exception as e:
            _log.error(f"[AngelOne] get_candles error: {e}")
            return []

    def _build_candle(self,token,ltp):
        """Build 5-Min OHLC candles from WebSocket ticks."""
        try:
            sym = self._token_symbol_map.get(token)
            if not sym: return
            now = time.time()
            # 5-minute bucket
            bucket = int(now // 300) * 300
            with self._candle_lock:
                cur = self._candle_current.get(sym)
                if cur is None or cur["ts"] != bucket:
                    # Close previous candle
                    if cur is not None:
                        closed = [cur["ts"],cur["o"],cur["h"],cur["l"],cur["c"],0]
                        if sym not in self._candle_cache:
                            self._candle_cache[sym] = []
                        self._candle_cache[sym].append(closed)
                        # Keep max 300 candles (~25 hours)
                        if len(self._candle_cache[sym]) > 300:
                            self._candle_cache[sym] = self._candle_cache[sym][-300:]
                    # Start new candle
                    self._candle_current[sym] = {"ts":bucket,"o":ltp,"h":ltp,"l":ltp,"c":ltp}
                else:
                    if ltp > cur["h"]: cur["h"] = ltp
                    if ltp < cur["l"]: cur["l"] = ltp
                    cur["c"] = ltp
        except Exception as e:
            _log.error(f"[AngelOne] Candle builder error: {e}")

    def register_symbol_token(self,symbol,token):
        """Register symbol→token mapping for candle builder."""
        if symbol and token:
            self._token_symbol_map[str(token)] = symbol

    def save_candle_cache(self):
        """Save candle cache to disk at midnight for next day startup."""
        try:
            import json, os
            for sym, candles in self._candle_cache.items():
                path = f"/tmp/candle_cache_{sym}.json"
                with open(path,"w") as f:
                    json.dump(candles[-300:], f)
            _log.info(f"[AngelOne] Candle cache saved for {len(self._candle_cache)} instruments")
        except Exception as e:
            _log.error(f"[AngelOne] Save candle cache error: {e}")

    def _load_candle_cache(self):
        """Load yesterday's candle cache from disk on startup."""
        try:
            import json, glob
            for path in glob.glob("/tmp/candle_cache_*.json"):
                sym = path.replace("/tmp/candle_cache_","").replace(".json","")
                with open(path,"r") as f:
                    self._candle_cache[sym] = json.load(f)
            if self._candle_cache:
                _log.info(f"[AngelOne] Loaded candle cache for {len(self._candle_cache)} instruments")
        except Exception as e:
            _log.error(f"[AngelOne] Load candle cache error: {e}")

    def get_rest_ltp(self, exchange: str, symbol: str, token: str) -> float:
        """REST fallback LTP using ltpData — used when WebSocket has no price yet."""
        try:
            ao_exch = EXCHANGE_MAP.get(exchange, exchange.upper())
            if not token:
                token = self.get_symbol_token(ao_exch, symbol)
            if not token:
                return 0.0
            r = self._smart_api.ltpData(ao_exch, symbol, token)
            if r and r.get("status") != False:
                return float(r.get("data",{}).get("ltp", 0) or 0)
        except Exception as e:
            _log.error(f"[AngelOne] get_rest_ltp error: {e}")
        return 0.0

    def get_all_fo_instruments(self) -> list:
        """
        Return all F&O instruments for engine to build INSTRUMENTS dict.
        Called by refresh_instruments_from_broker after login.
        """
        rows = []
        try:
            for key, info in self._instruments.items():
                exch = info.get("exchange","")
                if exch not in ("NFO","BFO","MCX"):
                    continue
                rows.append({
                    "exchange"       : exch,
                    "tradingsymbol"  : info.get("symbol",""),
                    "name"           : info.get("name",""),
                    "instrument_type": info.get("opttype",""),
                    "lot_size"       : int(info.get("lotsize",1) or 1),
                    "tick_size"      : float(info.get("tick_size",0.05) or 0.05),
                    "expiry"         : info.get("expiry",""),
                    "strike"         : float(info.get("strike",0) or 0)/100,
                    "instrument_token": info.get("token",""),
                })
        except Exception as e:
            _log.error(f"[AngelOne] get_all_fo_instruments error: {e}")
        return rows
