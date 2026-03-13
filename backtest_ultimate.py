import pandas as pd
import numpy as np
import argparse
from ultimate_predict import load_data, ultimate_iching_prediction

def evaluate_day(target_date, daily_data, csv_path, symbol_type):
    actual_row = daily_data.loc[target_date]
    actual_O = actual_row['open']
    actual_H = actual_row['high']
    actual_L = actual_row['low']
    actual_C = actual_row['close']
    
    try:
        res = ultimate_iching_prediction(csv_path, str(target_date.date()), event_type='none', today_open=actual_O, symbol_type=symbol_type)
    except Exception as e:
        return None
        
    if not res: return None
    
    pts = [p[1] for p in res['points']]
    pivot = res['pivot']
    atr = res['atr']
    
    upper_pts = [p for p in pts if p >= pivot]
    if not upper_pts: upper_pts = [pivot]
    err_H = min([abs(actual_H - p) for p in upper_pts])
    closest_H = upper_pts[np.argmin([abs(actual_H - p) for p in upper_pts])]
    
    lower_pts = [p for p in pts if p <= pivot]
    if not lower_pts: lower_pts = [pivot]
    err_L = min([abs(actual_L - p) for p in lower_pts])
    closest_L = lower_pts[np.argmin([abs(actual_L - p) for p in lower_pts])]
    
    # 容差：QQQ 0.2%, SPY 0.3%
    price_tolerance = 0.002 if symbol_type == 'QQQ' else 0.003
    hit_H = 1 if err_H < (atr * 0.15) or (err_H / actual_H) < price_tolerance else 0
    hit_L = 1 if err_L < (atr * 0.15) or (err_L / actual_L) < price_tolerance else 0
    
    return {
        'date': target_date.date(),
        'actual_H': actual_H,
        'actual_L': actual_L,
        'actual_C': actual_C,
        'pred_H': closest_H,
        'pred_L': closest_L,
        'err_H': err_H,
        'err_L': err_L,
        'hit_H': hit_H,
        'hit_L': hit_L,
        'atr': atr,
        'hexa': f"{res['base']} -> {res['changed']}"
    }

def run_full_backtest(year='2026', symbol_choice='1'):
    if symbol_choice == '1':
        csv_path = 'data/qqq_1m_sim.csv'
        symbol_type = 'QQQ'
        output_file = f'Ultimate_Backtest_Report_{symbol_type}_{year}.md'
    else:
        csv_path = 'data/spx_1m_sim.csv'
        symbol_type = 'SPY'
        output_file = f'Ultimate_Backtest_Report_{symbol_type}_{year}.md'
        
    daily, _ = load_data(csv_path)
    eval_dates = daily[daily.index.year == int(year)].index
    
    results = []
    for date in eval_dates:
        past = daily[daily.index < date]
        if len(past) < 10: continue
            
        r = evaluate_day(date, daily, csv_path, symbol_type)
        if r: results.append(r)
            
    df = pd.DataFrame(results)
    
    if len(df) == 0:
        print("无足够数据进行回测。")
        return
        
    total = len(df)
    hit_h_rate = df['hit_H'].mean() * 100
    hit_l_rate = df['hit_L'].mean() * 100
    both_hit_rate = ((df['hit_H'] == 1) & (df['hit_L'] == 1)).mean() * 100
    avg_err_h = df['err_H'].mean()
    avg_err_l = df['err_L'].mean()
    avg_atr = df['atr'].mean()
    
    print(f"========== 易经 x 缠论 {symbol_type} 预测模型 {year}年回测 (动态优化版) ==========")
    print(f"测试天数: {total} 天 | 日均爆发 ATR: {avg_atr:.2f} USD")
    print(f"----------------------------------------------")
    print(f"📈 抓顶 (High) 精度:")
    print(f"绝对误差: {avg_err_h:.2f} USD | 相对ATR误差: {avg_err_h/avg_atr*100:.1f}% | 命中率: {hit_h_rate:.1f}%")
    print(f"----------------------------------------------")
    print(f"📉 抓底 (Low) 精度:")
    print(f"绝对误差: {avg_err_l:.2f} USD | 相对ATR误差: {avg_err_l/avg_atr*100:.1f}% | 命中率: {hit_l_rate:.1f}%")
    print(f"----------------------------------------------")
    print(f"🎯 双顶底神仙命中率: {both_hit_rate:.1f}%")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', choices=['1', '2'], default='1', help='1 for QQQ, 2 for SPY')
    args = parser.parse_args()
    run_full_backtest('2026', args.symbol)
