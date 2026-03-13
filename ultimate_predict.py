import pandas as pd
import numpy as np

# 八卦与爻象 (1为阳，0为阴。下到上：初, 二, 三)
BAGUA_LINES = {
    1: [1,1,1], 2: [1,1,0], 3: [1,0,1], 4: [1,0,0],
    5: [0,1,1], 6: [0,1,0], 7: [0,0,1], 8: [0,0,0]
}
BAGUA_NAMES = {1:"乾", 2:"兑", 3:"离", 4:"震", 5:"巽", 6:"坎", 7:"艮", 8:"坤"}

def load_data(csv_path):
    df = pd.read_csv(csv_path, parse_dates=['datetime']).set_index('datetime').sort_index()
    # 提取正盘
    df_reg = df.between_time('09:30:00', '16:00:00').copy()
    
    # Calculate daily VWAP
    df_reg['typ'] = (df_reg['high'] + df_reg['low'] + df_reg['close']) / 3
    df_reg['vol_typ'] = df_reg['typ'] * df_reg['volume']
    
    daily = df_reg.resample('D').agg({
        'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum', 'vol_typ': 'sum'
    }).dropna()
    
    daily['vwap'] = daily['vol_typ'] / daily['volume']
    
    # 计算 ATR5
    daily['prev_close'] = daily['close'].shift(1)
    daily['tr'] = np.maximum(
        daily['high'] - daily['low'],
        np.maximum(abs(daily['high'] - daily['prev_close']), abs(daily['low'] - daily['prev_close']))
    )
    daily['atr_5'] = daily['tr'].rolling(5).mean()
    return daily, df_reg

def find_chan_fractals(daily_data):
    highs, lows = daily_data['high'].values, daily_data['low'].values
    if len(highs) < 3: return highs[-1], lows[-1]
    
    tops, bottoms = [], []
    for i in range(1, len(highs)-1):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]: tops.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]: bottoms.append(lows[i])
    
    chan_top = max(tops[-2:]) if len(tops) > 1 else highs[-1]
    chan_bottom = min(bottoms[-2:]) if len(bottoms) > 1 else lows[-1]
    return chan_top, chan_bottom

def ultimate_iching_prediction(csv_path, target_date='2026-03-11', event_type='none', today_open=None, symbol_type='QQQ'):
    daily, df_reg = load_data(csv_path)
    target = pd.to_datetime(target_date).date()
    past = daily[daily.index.date < target]
    
    if len(past) < 10: return None
    
    yest = past.iloc[-1]
    O, H, L, C = yest['open'], yest['high'], yest['low'], yest['close']
    VWAP = yest['vwap']
    ATR = yest['atr_5'] if pd.notna(yest['atr_5']) else (H - L)
    
    if today_open is None:
        today_open = C
        
    # ---------------------------------------------------------
    # 1. 天人地三才起卦法 (融入 宏观事件 外应)
    # ---------------------------------------------------------
    time_factor = 1 
    
    # 针对不同标的，设置基础爆发乘数
    burst_multiplier = 1.6 if symbol_type == 'QQQ' else 1.3 
    
    if event_type == 'cpi' or event_type == 'pce':
        time_factor = 5 
        burst_multiplier = 1.6 if symbol_type == 'SPY' else 1.8 
    elif event_type == 'fomc':
        time_factor = 9 
        burst_multiplier = 2.0 
    elif event_type == 'nfp': 
        time_factor = 5
        burst_multiplier = 1.4 if symbol_type == 'SPY' else 1.6
    
    upper_num = int((H + L) * 100) % 8 or 8
    lower_num = int((today_open + C) * 100) % 8 or 8
    
    moving_line = (upper_num + lower_num + target.day + time_factor) % 6 or 6
    
    base_hexa = BAGUA_LINES[lower_num] + BAGUA_LINES[upper_num]
    changed_hexa = list(base_hexa)
    changed_hexa[moving_line - 1] = 1 - changed_hexa[moving_line - 1]
    
    # ---------------------------------------------------------
    # 2. 空间矩阵构建
    # ---------------------------------------------------------
    burst_atr = ATR * burst_multiplier
    
    down_atr = burst_atr * 1.15
    up_atr = burst_atr * 1.0
    
    # 动态中枢
    pivot = (today_open * 2 + H + L + VWAP) / 5
    
    chan_top, chan_bottom = find_chan_fractals(past)
    
    R3 = pivot + up_atr * 1.382
    R2 = pivot + up_atr * 0.854
    R1 = pivot + up_atr * 0.500
    S1 = pivot - down_atr * 0.500
    S2 = pivot - down_atr * 0.854
    S3 = pivot - down_atr * 1.382
    
    levels = [
        min(S3, chan_bottom - down_atr*0.2), 
        S2,                                   
        S1,                                   
        R1,                                   
        R2,                                   
        max(R3, chan_top + up_atr*0.2)        
    ]
    
    # ---------------------------------------------------------
    # 3. 爻象赋能微调
    # ---------------------------------------------------------
    points = []
    for i, is_yang in enumerate(changed_hexa):
        lvl = levels[i]
        adj = (burst_atr * 0.05) if is_yang else -(burst_atr * 0.05)
        
        is_moving = (i == moving_line - 1)
        final_p = lvl + adj
        desc = "阳爻(坚壁/突破)" if is_yang else "阴爻(深渊/陷阱)"
        if is_moving: 
            desc = f"【动爻★】{desc} - 变盘眼"
            if event_type != 'none':
                desc += f" ({event_type.upper()})"
            
        points.append((i+1, final_p, desc))
        
    return {
        'date': target, 'upper': upper_num, 'lower': lower_num, 'moving': moving_line,
        'base': base_hexa, 'changed': changed_hexa, 'pivot': pivot, 'points': points, 'atr': burst_atr,
        'symbol_type': symbol_type, 'multiplier': burst_multiplier
    }
