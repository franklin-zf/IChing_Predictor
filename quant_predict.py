import pandas as pd
import numpy as np

BAGUA_LINES = {
    1: [1,1,1], 2: [1,1,0], 3: [1,0,1], 4: [1,0,0],
    5: [0,1,1], 6: [0,1,0], 7: [0,0,1], 8: [0,0,0]
}
BAGUA_NAMES = {1:"乾", 2:"兑", 3:"离", 4:"震", 5:"巽", 6:"坎", 7:"艮", 8:"坤"}

def load_enhanced_data(csv_path):
    df = pd.read_csv(csv_path, parse_dates=['datetime'])
    df.set_index('datetime', inplace=True)
    df.sort_index(inplace=True)
    
    dates = df.index.normalize().unique()
    sessions = []
    
    for i in range(1, len(dates)):
        prev_date = dates[i-1]
        curr_date = dates[i]
        
        rth_prev = df[(df.index >= prev_date + pd.Timedelta(hours=9, minutes=30)) & 
                      (df.index < prev_date + pd.Timedelta(hours=16, minutes=1))]
        if rth_prev.empty: continue
        prev_close = rth_prev['close'].iloc[-1]
        
        overnight = df[(df.index >= prev_date + pd.Timedelta(hours=16)) & 
                       (df.index < curr_date + pd.Timedelta(hours=9, minutes=30))]
                       
        rth_curr = df[(df.index >= curr_date + pd.Timedelta(hours=9, minutes=30)) & 
                      (df.index < curr_date + pd.Timedelta(hours=16, minutes=1))]
                      
        o_vol = overnight['volume'].sum() if not overnight.empty else 0
        o_high = overnight['high'].max() if not overnight.empty else prev_close
        o_low = overnight['low'].min() if not overnight.empty else prev_close
        o_close = overnight['close'].iloc[-1] if not overnight.empty else prev_close
        
        if rth_curr.empty:
            sessions.append({
                'date': curr_date.date(),
                'prev_close': prev_close,
                'overnight_volume': o_vol,
                'overnight_high': o_high,
                'overnight_low': o_low,
                'latest_premarket_price': o_close,
                'open': np.nan, 'high': np.nan, 'low': np.nan, 'close': np.nan,
                'volume': np.nan, 'vwap': np.nan, 'tr': np.nan
            })
            continue
            
        c_open = rth_curr['open'].iloc[0]
        c_high = rth_curr['high'].max()
        c_low = rth_curr['low'].min()
        c_close = rth_curr['close'].iloc[-1]
        c_vol = rth_curr['volume'].sum()
        
        typ = (c_high + c_low + c_close) / 3
        vwap = (typ * rth_curr['volume']).sum() / c_vol if c_vol > 0 else c_close
        
        tr = max(c_high - c_low, abs(c_high - prev_close), abs(c_low - prev_close))
        
        sessions.append({
            'date': curr_date.date(),
            'prev_close': prev_close,
            'overnight_volume': o_vol,
            'overnight_high': o_high,
            'overnight_low': o_low,
            'latest_premarket_price': o_close,
            'open': c_open,
            'high': c_high,
            'low': c_low,
            'close': c_close,
            'volume': c_vol,
            'vwap': vwap,
            'tr': tr
        })
        
    daily = pd.DataFrame(sessions)
    if daily.empty: return daily
    
    daily.set_index('date', inplace=True)
    daily['atr_5'] = daily['tr'].rolling(5, min_periods=1).mean()
    daily['avg_ov_vol_20'] = daily['overnight_volume'].rolling(20, min_periods=1).mean()
    
    return daily

def find_chan_fractals(daily_data):
    highs = daily_data['high'].dropna().values
    lows = daily_data['low'].dropna().values
    if len(highs) < 3: 
        return (highs[-1] if len(highs)>0 else 0), (lows[-1] if len(lows)>0 else 0)
    
    tops, bottoms = [], []
    for i in range(1, len(highs)-1):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]: tops.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]: bottoms.append(lows[i])
    
    chan_top = max(tops[-2:]) if len(tops) > 1 else highs[-1]
    chan_bottom = min(bottoms[-2:]) if len(bottoms) > 1 else lows[-1]
    return chan_top, chan_bottom

def quant_iching_prediction(csv_path, target_date_str, event_type='none', symbol_type='QQQ'):
    daily = load_enhanced_data(csv_path)
    if daily.empty: return None
    
    target_d = pd.to_datetime(target_date_str).date()
    past = daily[daily.index < target_d]
    if len(past) < 5: return None
    
    # Check for NaN in historical data which will break IChing int conversion
    if pd.isna(past.iloc[-1]['high']) or pd.isna(past.iloc[-1]['low']) or pd.isna(past.iloc[-1]['close']):
        print("Warning: Missing RTH data in the previous day. Scanning backwards...")
        past = past.dropna(subset=['high', 'low', 'close'])
        if len(past) < 5: return None
        
    yest = past.iloc[-1]
    prev_close = yest['close']
    ATR = yest['atr_5']
    VWAP = yest['vwap']
    avg_ov_vol = yest['avg_ov_vol_20']
    
    # Fetch real-time data using yfinance directly to get the current pre-market info
    import yfinance as yf
    today_open = prev_close
    ov_vol = 0
    try:
        yf_symbol = 'QQQ' if symbol_type == 'QQQ' else 'SPY'
        
        # Get today's real-time pre-market data
        import datetime
        import pytz
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.datetime.now(ny_tz)
        
        # Download recent 1m data including pre-market
        rt_df = yf.download(yf_symbol, period='5d', interval='1m', prepost=True, progress=False)
        if not rt_df.empty:
            if isinstance(rt_df.columns, pd.MultiIndex):
                rt_df.columns = rt_df.columns.droplevel(1)
            rt_df.columns = [c.lower() for c in rt_df.columns]
            
            # Find previous RTH close to be safe
            prev_rth = rt_df[(rt_df.index.time >= datetime.time(9, 30)) & 
                             (rt_df.index.time < datetime.time(16, 0)) &
                             (rt_df.index.date < target_d)]
            if not prev_rth.empty:
                yf_prev_close = prev_rth['close'].iloc[-1]
            else:
                yf_prev_close = prev_close
            
            # Find today's pre-market data
            today_pre = rt_df[(rt_df.index.date == target_d) & 
                              (rt_df.index.time < datetime.time(9, 30))]
                              
            if not today_pre.empty:
                yf_today_open = today_pre['close'].iloc[-1] # latest pre-market price
                ov_vol = today_pre['volume'].sum()
                
                # We need to map yfinance prices back to our local data's scale. 
                # This is crucial because local data for SPX is actually SPX Index (~6600) 
                # but we fetch SPY ETF from yfinance (~660)
                price_scale_ratio = prev_close / yf_prev_close if yf_prev_close > 0 else 1.0
                today_open = yf_today_open * price_scale_ratio
                
                print(f"✅ Fetched YF Pre-market for {yf_symbol}: Raw Price {yf_today_open:.2f}, Scaled Price {today_open:.2f}")
            else:
                today_pre_all = rt_df[(rt_df.index.date == target_d)]
                if not today_pre_all.empty:
                    yf_today_open = today_pre_all['open'].iloc[0]
                    price_scale_ratio = prev_close / yf_prev_close if yf_prev_close > 0 else 1.0
                    today_open = yf_today_open * price_scale_ratio
                print(f"⚠️ No strictly pre-market data found today for {yf_symbol}, using latest available.")
                
    except Exception as e:
        print(f"❌ yfinance fetch error: {e}")
        
    gap_pct = (today_open - prev_close) / prev_close * 100 if prev_close > 0 else 0
    vol_ratio = ov_vol / avg_ov_vol if avg_ov_vol > 0 else 1.0
    
    # ---------------------------------------------------------
    # 1. 第一性原理：量价时空定势 (Gap & Volume Analysis)
    # ---------------------------------------------------------
    quant_bias = "Neutral"
    quant_strategy = "无明显跳空，按常态震荡处理，依靠中枢高抛低吸。"
    timing_window = "无特殊时间窗口，重点关注动爻位置的支撑/阻力。"
    
    is_extreme_gap = abs(gap_pct) >= 1.5
    is_sig_gap = 0.5 <= abs(gap_pct) < 1.5
    is_high_vol = vol_ratio >= 2.0
    
    if is_extreme_gap or is_high_vol:
        quant_bias = "Trending"
        if gap_pct > 0:
            quant_strategy = f"【动能延续/逼空】跳空 +{gap_pct:.2f}%, 隔夜量能比 {vol_ratio:.1f}x。均值回归失效，切忌做空！顺势找机会买 Call。"
            timing_window = "开盘后稍作回踩（或横盘企稳）即是买点。"
        else:
            quant_strategy = f"【动能延续/屠杀】跳空 {gap_pct:.2f}%, 隔夜量能比 {vol_ratio:.1f}x。均值回归失效，切忌抄底！顺势找机会买 Put。"
            timing_window = "开盘后任何微弱反抽都是放空良机。"
    elif is_sig_gap:
        quant_bias = "Mean_Reversion"
        if gap_pct > 0:
            quant_strategy = f"【均值回归/高开低走】跳空 +{gap_pct:.2f}% (量比 {vol_ratio:.1f}x)。动能已被透支，极易触发获利了结，日内看空，逢高买 Put。"
            timing_window = "🎯 重点狙击窗口：09:30 - 09:45 冲高诱多顶点。"
        else:
            quant_strategy = f"【均值回归/低开高走】跳空 {gap_pct:.2f}% (量比 {vol_ratio:.1f}x)。恐慌盘将在开盘释放，极易引发抄底反包，日内看多，逢低买 Call。"
            timing_window = "🎯 重点狙击窗口：09:40 - 10:15 恐慌杀跌竭尽点。"
    
    # ---------------------------------------------------------
    # 2. 易经时空矩阵 (I-Ching Pivot Points)
    # ---------------------------------------------------------
    time_factor = 1 
    burst_multiplier = 1.6 if symbol_type == 'QQQ' else 1.3 
    
    if event_type in ['cpi', 'pce']:
        time_factor = 5 
        burst_multiplier = 1.6 if symbol_type == 'SPY' else 1.8 
    elif event_type == 'fomc':
        time_factor = 9 
        burst_multiplier = 2.0 
    elif event_type == 'nfp': 
        time_factor = 5
        burst_multiplier = 1.4 if symbol_type == 'SPY' else 1.6
        
    upper_num = int((yest['high'] + yest['low']) * 100) % 8 or 8
    lower_num = int((today_open + yest['close']) * 100) % 8 or 8
    moving_line = (upper_num + lower_num + target_d.day + time_factor) % 6 or 6
    
    base_hexa = BAGUA_LINES[lower_num] + BAGUA_LINES[upper_num]
    changed_hexa = list(base_hexa)
    changed_hexa[moving_line - 1] = 1 - changed_hexa[moving_line - 1]
    
    burst_atr = ATR * burst_multiplier
    pivot = (today_open * 2 + yest['high'] + yest['low'] + VWAP) / 5
    chan_top, chan_bottom = find_chan_fractals(past)
    
    R3 = pivot + burst_atr * 1.382
    R2 = pivot + burst_atr * 0.854
    R1 = pivot + burst_atr * 0.500
    S1 = pivot - burst_atr * 1.15 * 0.500
    S2 = pivot - burst_atr * 1.15 * 0.854
    S3 = pivot - burst_atr * 1.15 * 1.382
    
    levels = [
        min(S3, chan_bottom - burst_atr*0.2), 
        S2, S1, R1, R2, 
        max(R3, chan_top + burst_atr*0.2)
    ]
    
    points = []
    for i, is_yang in enumerate(changed_hexa):
        lvl = levels[i]
        adj = (burst_atr * 0.05) if is_yang else -(burst_atr * 0.05)
        is_moving = (i == moving_line - 1)
        final_p = lvl + adj
        desc = "阳爻(坚壁/突破)" if is_yang else "阴爻(深渊/陷阱)"
        if is_moving: 
            desc = f"【动爻★】{desc} - 变盘眼"
        points.append((i+1, final_p, desc))
        
    return {
        'date': target_d, 'upper': upper_num, 'lower': lower_num, 'moving': moving_line,
        'pivot': pivot, 'points': points, 'atr': burst_atr,
        'symbol_type': symbol_type, 'gap_pct': gap_pct, 'vol_ratio': vol_ratio,
        'quant_bias': quant_bias, 'quant_strategy': quant_strategy, 'timing_window': timing_window,
        'prev_close': prev_close, 'today_open': today_open, 'multiplier': burst_multiplier
    }
