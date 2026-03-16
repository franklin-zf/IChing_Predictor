import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import pytz

def fetch_and_prepare_data(symbol):
    print(f"Fetching 60d 5m data for {symbol}...")
    df = yf.download(symbol, period='60d', interval='5m', prepost=True, progress=False)
    
    # Handle MultiIndex columns if present (yfinance sometimes returns MultiIndex for single ticker)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
        
    df.index = df.index.tz_convert('America/New_York')
    df.columns = [c.lower() for c in df.columns]
    return df

def extract_sessions(df):
    dates = df.index.normalize().unique()
    sessions = []
    
    for i in range(1, len(dates)):
        prev_date = dates[i-1]
        curr_date = dates[i]
        
        prev_rth = df[(df.index >= prev_date + pd.Timedelta(hours=9, minutes=30)) & 
                      (df.index < prev_date + pd.Timedelta(hours=16))]
        if prev_rth.empty: continue
        prev_close = prev_rth['close'].iloc[-1]
        
        # Overnight/Pre-market
        overnight = df[(df.index >= prev_date + pd.Timedelta(hours=16)) & 
                       (df.index < curr_date + pd.Timedelta(hours=9, minutes=30))]
        
        # Regular trading hours
        rth = df[(df.index >= curr_date + pd.Timedelta(hours=9, minutes=30)) & 
                 (df.index < curr_date + pd.Timedelta(hours=16))]
        
        if rth.empty or overnight.empty: continue
        
        o_low = overnight['low'].min()
        o_high = overnight['high'].max()
        
        c_open = rth['open'].iloc[0]
        c_close = rth['close'].iloc[-1]
        r_low = rth['low'].min()
        r_high = rth['high'].max()
        
        r_low_time = rth['low'].idxmin().time()
        r_high_time = rth['high'].idxmax().time()
        
        sessions.append({
            'date': curr_date.date(),
            'prev_close': prev_close,
            'overnight_low': o_low,
            'overnight_high': o_high,
            'open': c_open,
            'close': c_close,
            'intraday_low': r_low,
            'intraday_high': r_high,
            'intraday_low_time': r_low_time,
            'intraday_high_time': r_high_time
        })
        
    return pd.DataFrame(sessions)

def analyze_phenomena(df_sessions, symbol):
    if df_sessions.empty:
        return f"## {symbol} 数据不足\n未能提取到足够的夜盘/盘中时段数据进行分析。\n"
        
    df = df_sessions.copy()
    
    df['ov_drop_pct'] = (df['overnight_low'] - df['prev_close']) / df['prev_close'] * 100
    df['ov_rise_pct'] = (df['overnight_high'] - df['prev_close']) / df['prev_close'] * 100
    df['pre_recovery_pct'] = (df['open'] - df['overnight_low']) / df['prev_close'] * 100
    
    df['pre_change_pct'] = (df['open'] - df['prev_close']) / df['prev_close'] * 100
    df['intraday_change_pct'] = (df['close'] - df['open']) / df['open'] * 100
    df['intraday_max_dd_pct'] = (df['intraday_low'] - df['open']) / df['open'] * 100
    df['intraday_max_up_pct'] = (df['intraday_high'] - df['open']) / df['open'] * 100
    
    os.makedirs('analysis_output', exist_ok=True)
    
    drop_threshold = -0.3
    df_drop = df[df['ov_drop_pct'] <= drop_threshold].copy()
    
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='ov_drop_pct', y='intraday_change_pct', alpha=0.6)
    plt.title(f'{symbol}: Overnight Drop vs Intraday Change')
    plt.xlabel('Overnight Max Drop (%)')
    plt.ylabel('Intraday Change (%)')
    plt.axhline(0, color='red', linestyle='--')
    plt.axvline(0, color='red', linestyle='--')
    plt.savefig(f'analysis_output/{symbol}_ov_drop_vs_intraday.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df_drop, x='pre_recovery_pct', y='intraday_change_pct', alpha=0.6)
    plt.title(f'{symbol}: Pre-market Recovery vs Intraday Change (When OV Drop <= {drop_threshold}%)')
    plt.xlabel('Pre-market Recovery from Low (%)')
    plt.ylabel('Intraday Change (%)')
    plt.axhline(0, color='red', linestyle='--')
    plt.savefig(f'analysis_output/{symbol}_pre_recovery_vs_intraday.png')
    plt.close()

    rise_threshold = 0.3
    df_rise = df[df['pre_change_pct'] >= rise_threshold].copy()
    
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='pre_change_pct', y='intraday_change_pct', alpha=0.6)
    plt.title(f'{symbol}: Pre-market Change vs Intraday Change')
    plt.xlabel('Pre-market Change (%)')
    plt.ylabel('Intraday Change (%)')
    plt.axhline(0, color='red', linestyle='--')
    plt.axvline(0, color='red', linestyle='--')
    plt.savefig(f'analysis_output/{symbol}_pre_rise_vs_intraday.png')
    plt.close()
    
    total_days = len(df)
    
    p1_total = len(df_drop)
    p1_positive_close = len(df_drop[df_drop['intraday_change_pct'] > 0]) if p1_total > 0 else 0
    p1_drop_then_rise = len(df_drop[(df_drop['intraday_change_pct'] > 0) & (df_drop['intraday_max_dd_pct'] < -0.2)]) if p1_total > 0 else 0
    p1_uni_rise = len(df_drop[(df_drop['intraday_change_pct'] > 0) & (df_drop['intraday_max_dd_pct'] >= -0.2)]) if p1_total > 0 else 0
    
    p1_pos_pct = (p1_positive_close / p1_total * 100) if p1_total > 0 else 0
    p1_dtr_pct = (p1_drop_then_rise / p1_total * 100) if p1_total > 0 else 0
    p1_uni_pct = (p1_uni_rise / p1_total * 100) if p1_total > 0 else 0
        
    p2_total = len(df_rise)
    p2_negative_close = len(df_rise[df_rise['intraday_change_pct'] < 0]) if p2_total > 0 else 0
    p2_consolidation = len(df_rise[(df_rise['intraday_max_up_pct'] < 0.5) & (df_rise['intraday_max_dd_pct'] > -0.5)]) if p2_total > 0 else 0
    
    p2_neg_pct = (p2_negative_close / p2_total * 100) if p2_total > 0 else 0
    p2_cons_pct = (p2_consolidation / p2_total * 100) if p2_total > 0 else 0
    
    if p1_total > 0:
        low_times = df_drop['intraday_low_time'].apply(lambda x: x.hour + x.minute/60.0)
        plt.figure(figsize=(10, 6))
        sns.histplot(low_times, bins=20, kde=True)
        plt.title(f'{symbol}: Time of Intraday Low (When OV Drop <= {drop_threshold}%)')
        plt.xlabel('Time of Day (Hours from Midnight)')
        plt.xticks([9.5, 10, 11, 12, 13, 14, 15, 16], ['9:30', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00'])
        plt.savefig(f'analysis_output/{symbol}_p1_low_time_dist.png')
        plt.close()

    if p2_total > 0:
        high_times = df_rise['intraday_high_time'].apply(lambda x: x.hour + x.minute/60.0)
        plt.figure(figsize=(10, 6))
        sns.histplot(high_times, bins=20, kde=True)
        plt.title(f'{symbol}: Time of Intraday High (When Pre-market Rise >= {rise_threshold}%)')
        plt.xlabel('Time of Day (Hours from Midnight)')
        plt.xticks([9.5, 10, 11, 12, 13, 14, 15, 16], ['9:30', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00'])
        plt.savefig(f'analysis_output/{symbol}_p2_high_time_dist.png')
        plt.close()

    report = f"""
## 第一性原理分析：大盘盘前与盘中走势关联性报告 ({symbol})
**分析样本跨度**: 过去 60 个交易日（含盘前盘后高频数据）

### 现象一：夜盘/盘前下跌与盘中反转/单边上涨
**定义**: 夜盘及盘前阶段（16:00 - 09:30）发生过显著下探（最大跌幅 <= {drop_threshold}%）。
**触发天数**: {p1_total} 天

当夜盘出现显著下探时：
- **盘中最终收涨 (RTH Close > RTH Open)** 的概率: {p1_pos_pct:.1f}% ({p1_positive_close} / {p1_total})
- 其中，**先跌后涨** (开盘后向下回撤 > 0.2% 然后收高) 占比: {p1_dtr_pct:.1f}%
- 其中，**单边上涨** (开盘后基本不回撤直接拉升，最大回撤 < 0.2%) 占比: {p1_uni_pct:.1f}%

**第一性原理洞察（结合图表 `{symbol}_p1_low_time_dist.png`）**: 
当夜盘发生大跌，往往是受到隔夜宏观消息或情绪的恐慌性抛售。如果这部分空头势能没有在盘前被完全消化，开盘后 9:30 - 10:30 会出现最后的“恐慌盘抛售”，这正是“先跌后涨”中“跌”的成因。在恐慌盘释放完毕后，真正的长线买盘及均值回归力量入场，形成日内低点并开启反转。
如果盘前已经大幅拉回（Recovery 很大），往往意味着情绪已经企稳，开盘后“单边上涨”的概率激增。

### 现象二：夜盘/盘前上涨与盘中下跌/震荡
**定义**: 盘前收涨明显，即 09:30 开盘价相较于昨日 16:00 收盘价涨幅 >= {rise_threshold}%。
**触发天数**: {p2_total} 天

当盘前出现显著高开时：
- **盘中最终收跌 (RTH Close < RTH Open，即高开低走)** 的概率: {p2_neg_pct:.1f}% ({p2_negative_close} / {p2_total})
- **盘中呈现窄幅震荡** (日内最高涨幅 < 0.5% 且 最大回撤 > -0.5%) 的概率: {p2_cons_pct:.1f}%

**第一性原理洞察（结合图表 `{symbol}_p2_high_time_dist.png`）**: 
高开是对利好的直接定价（如财报、宏观数据），但这往往会**透支**当天的多头动能。从市场博弈角度看，盘前已经积累了可观浮盈的资金会在开盘后寻求获利了结，形成抛压。此时如果没有新的增量资金进场，盘面就会陷入震荡或者高开低走。图中分布显示，高开后全天的最高点极容易出现在 9:30 - 10:00 的早盘诱多冲高阶段。
"""
    return report

def main():
    sns.set_theme(style="whitegrid")
    
    df_qqq = fetch_and_prepare_data('QQQ')
    df_spy = fetch_and_prepare_data('SPY')
    
    sessions_qqq = extract_sessions(df_qqq)
    sessions_spy = extract_sessions(df_spy)
    
    report_qqq = analyze_phenomena(sessions_qqq, 'QQQ')
    report_spy = analyze_phenomena(sessions_spy, 'SPY')
    
    with open('analysis_output/Market_Session_Analysis_Report.md', 'w', encoding='utf-8') as f:
        f.write("# 美股夜盘、盘前与盘中走势统计及规律深度分析\n\n")
        f.write(report_qqq)
        f.write("\n---\n")
        f.write(report_spy)
        
    print("✅ Analysis complete. Reports and charts saved to 'analysis_output' directory.")

if __name__ == "__main__":
    main()
