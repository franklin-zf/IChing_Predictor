import pandas as pd
from ultimate_predict import load_data, ultimate_iching_prediction
from iching_strategy import IChingOptionStrategy
import argparse

def backtest_strategy_2026(symbol_choice='1'):
    if symbol_choice == '1':
        csv_path = 'data/qqq_1m_sim.csv'
        symbol_type = 'QQQ'
    else:
        csv_path = 'data/spx_1m_sim.csv'
        symbol_type = 'SPY'
        
    daily, df_reg = load_data(csv_path)
    eval_dates = daily[daily.index.year == 2026].index
    
    trades = []
    
    for target_date in eval_dates:
        target_str = str(target_date.date())
        past = daily[daily.index < target_date]
        if len(past) < 10: continue
            
        try:
            res = ultimate_iching_prediction(csv_path, target_str, 'none', symbol_type=symbol_type)
        except Exception:
            continue
            
        if not res: continue
        
        strategy = IChingOptionStrategy(pivot=res['pivot'], levels=res['points'], atr=res['atr'])
        intraday_data = df_reg.loc[target_str]
        
        trade = strategy.run(intraday_data)
        if trade:
            trades.append(trade)
            
    if not trades:
        print(f"未触发任何 {symbol_type} 交易。")
        return
        
    df_trades = pd.DataFrame([vars(t) for t in trades])
    
    wins = df_trades[df_trades['pnl_pct'] > 0]
    losses = df_trades[df_trades['pnl_pct'] <= 0]
    
    win_rate = len(wins) / len(df_trades) * 100
    avg_pnl = df_trades['pnl_pct'].mean() * 100
    
    print("=========================================")
    print(f"🎯 {symbol_type} 易经时空矩阵 期权实战策略回测 (2026年)")
    print("=========================================")
    print(f"总交易天数: {len(eval_dates)}")
    print(f"触发交易天数: {len(df_trades)}")
    print(f"策略总体胜率: {win_rate:.2f}%")
    print(f"单笔平均正股收益: {avg_pnl:.3f}% (由于期权带杠杆，这相当于期权的大幅盈利)")
    print("-----------------------------------------")
    
    print("\n【出场原因统计】:")
    print(df_trades['exit_reason'].value_counts())
    
    print("\n【最近 5 笔经典交易复盘】:")
    for _, t in df_trades.tail(5).iterrows():
        print(f"[{t['date']}] {t['side'].upper()} @ {t['trigger_level']} | "
              f"入场: {t['entry_price']:.2f} ({t['entry_time'].strftime('%H:%M')}) -> "
              f"出场: {t['exit_price']:.2f} ({t['exit_time'].strftime('%H:%M')}) | "
              f"盈亏: {t['pnl_pct']*100:+.2f}% | 结果: {t['exit_reason']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', choices=['1', '2'], default='1', help='1 for QQQ, 2 for SPY')
    args = parser.parse_args()
    backtest_strategy_2026(args.symbol)
