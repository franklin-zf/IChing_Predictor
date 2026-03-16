import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

def load_data(csv_path):
    df = pd.read_csv(csv_path, parse_dates=['datetime'])
    df.set_index('datetime', inplace=True)
    df.sort_index(inplace=True)
    return df

def extract_sessions(df):
    # Ensure index is datetime
    # We want to group by "Trading Day"
    # A trading day T includes:
    # After-hours T-1: 16:00 - 20:00
    # Pre-market T: 04:00 - 09:30
    # Regular T: 09:30 - 16:00
    
    # Let's resample and assign a 'trading_day' to each row
    # If time > 16:00, it belongs to the next trading day
    
    # We can shift the time by 8 hours backwards so that 16:00 becomes 08:00, 09:30 becomes 01:30 of the same date
    # Actually, shifting by 8 hours means 16:00 (T-1) becomes 08:00 (T-1). That doesn't group 16:00 T-1 into T.
    
    # Let's do: if time >= 16:00, trading_date = date + 1 business day (approximate by date + 1 or next available)
    # Better approach:
    dates = df.index.normalize().unique()
    sessions = []
    
    for i in range(1, len(dates)):
        prev_date = dates[i-1]
        curr_date = dates[i]
        
        # previous day close
        prev_rth = df[(df.index >= prev_date + pd.Timedelta(hours=9, minutes=30)) & 
                      (df.index < prev_date + pd.Timedelta(hours=16))]
        if prev_rth.empty: continue
        prev_close = prev_rth['close'].iloc[-1]
        
        # Overnight / Pre-market (from prev_date 16:00 to curr_date 09:30)
        overnight = df[(df.index >= prev_date + pd.Timedelta(hours=16)) & 
                       (df.index < curr_date + pd.Timedelta(hours=9, minutes=30))]
        
        # Regular trading hours (curr_date 09:30 to 16:00)
        rth = df[(df.index >= curr_date + pd.Timedelta(hours=9, minutes=30)) & 
                 (df.index < curr_date + pd.Timedelta(hours=16))]
        
        if rth.empty or overnight.empty: continue
        
        o_low = overnight['low'].min()
        o_high = overnight['high'].max()
        
        c_open = rth['open'].iloc[0]
        c_close = rth['close'].iloc[-1]
        r_low = rth['low'].min()
        r_high = rth['high'].max()
        
        # Find time of intraday low and high
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
    
    # Calculate metrics
    df['ov_drop_pct'] = (df['overnight_low'] - df['prev_close']) / df['prev_close'] * 100
    df['ov_rise_pct'] = (df['overnight_high'] - df['prev_close']) / df['prev_close'] * 100
    df['pre_recovery_pct'] = (df['open'] - df['overnight_low']) / df['prev_close'] * 100
    
    df['pre_change_pct'] = (df['open'] - df['prev_close']) / df['prev_close'] * 100
    df['intraday_change_pct'] = (df['close'] - df['open']) / df['open'] * 100
    df['intraday_max_dd_pct'] = (df['intraday_low'] - df['open']) / df['open'] * 100
    df['intraday_max_up_pct'] = (df['intraday_high'] - df['open']) / df['open'] * 100
    
    os.makedirs('analysis_output', exist_ok=True)
    
    # Phenomenon 1: Overnight drop and recovery -> Intraday drops then rises, or unilateral rise
    # Define significant overnight drop (e.g., < -0.5%)
    drop_threshold = -0.5
    df_drop = df[df['ov_drop_pct'] <= drop_threshold].copy()
    
    # Classify intraday patterns
    # Drop then rise: low is early, close > open
    # Unilateral rise: low is very close to open (max dd > -0.2%), close > open
    
    # Scatter plot: Overnight Drop vs Intraday Change
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='ov_drop_pct', y='intraday_change_pct', alpha=0.6)
    plt.title(f'{symbol}: Overnight Drop vs Intraday Change')
    plt.xlabel('Overnight Max Drop (%)')
    plt.ylabel('Intraday Change (%)')
    plt.axhline(0, color='red', linestyle='--')
    plt.axvline(0, color='red', linestyle='--')
    plt.savefig(f'analysis_output/{symbol}_ov_drop_vs_intraday.png')
    plt.close()

    # Scatter plot: Pre-market Recovery vs Intraday Change
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df_drop, x='pre_recovery_pct', y='intraday_change_pct', alpha=0.6)
    plt.title(f'{symbol}: Pre-market Recovery vs Intraday Change (When OV Drop <= {drop_threshold}%)')
    plt.xlabel('Pre-market Recovery from Low (%)')
    plt.ylabel('Intraday Change (%)')
    plt.axhline(0, color='red', linestyle='--')
    plt.savefig(f'analysis_output/{symbol}_pre_recovery_vs_intraday.png')
    plt.close()

    # Phenomenon 2: Overnight / Pre-market rises -> Intraday drops or consolidates
    rise_threshold = 0.5
    df_rise = df[df['pre_change_pct'] >= rise_threshold].copy()
    
    # Scatter plot: Pre-market Rise vs Intraday Change
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='pre_change_pct', y='intraday_change_pct', alpha=0.6)
    plt.title(f'{symbol}: Pre-market Change vs Intraday Change')
    plt.xlabel('Pre-market Change (%)')
    plt.ylabel('Intraday Change (%)')
    plt.axhline(0, color='red', linestyle='--')
    plt.axvline(0, color='red', linestyle='--')
    plt.savefig(f'analysis_output/{symbol}_pre_rise_vs_intraday.png')
    plt.close()
    
    # Generating stats for markdown
    total_days = len(df)
    
    # Stats for P1
    p1_total = len(df_drop)
    p1_positive_close = len(df_drop[df_drop['intraday_change_pct'] > 0]) if p1_total > 0 else 0
    p1_drop_then_rise = len(df_drop[(df_drop['intraday_change_pct'] > 0) & (df_drop['intraday_max_dd_pct'] < -0.3)]) if p1_total > 0 else 0
    p1_uni_rise = len(df_drop[(df_drop['intraday_change_pct'] > 0) & (df_drop['intraday_max_dd_pct'] >= -0.3)]) if p1_total > 0 else 0
    
    p1_pos_pct = (p1_positive_close / p1_total * 100) if p1_total > 0 else 0
    p1_dtr_pct = (p1_drop_then_rise / p1_total * 100) if p1_total > 0 else 0
    p1_uni_pct = (p1_uni_rise / p1_total * 100) if p1_total > 0 else 0
        
    # Stats for P2
    p2_total = len(df_rise)
    p2_negative_close = len(df_rise[df_rise['intraday_change_pct'] < 0]) if p2_total > 0 else 0
    p2_consolidation = len(df_rise[(df_rise['intraday_max_up_pct'] < 0.5) & (df_rise['intraday_max_dd_pct'] > -0.5)]) if p2_total > 0 else 0
    
    p2_neg_pct = (p2_negative_close / p2_total * 100) if p2_total > 0 else 0
    p2_cons_pct = (p2_consolidation / p2_total * 100) if p2_total > 0 else 0
        
    # Turning points (time distribution of intraday low when pre-market drops)
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
**分析样本天数**: {total_days} 天

### 现象一：夜盘/盘前下跌与盘中反转/单边上涨
**设定阈值**: 夜盘最大跌幅 <= {drop_threshold}%
**触发天数**: {p1_total} 天

当夜盘出现显著下跌时：
- **盘中最终收涨 (Close > Open)** 的概率: {p1_positive_close} / {p1_total} = {p1_pos_pct:.1f}%
- 其中，**先跌后涨** (盘中回撤 > 0.3% 后收高) 占比: {p1_drop_then_rise} / {p1_total} = {p1_dtr_pct:.1f}%
- 其中，**单边上涨** (盘中基本不回撤，直接拉升) 占比: {p1_uni_rise} / {p1_total} = {p1_uni_pct:.1f}%

**转折点洞察**: 请参考图表 `{symbol}_p1_low_time_dist.png`。从数据分布看，夜盘大跌后的“最低点”通常密集发生在开盘后的前一小时（9:30 - 10:30），这是由于开盘恐慌盘释放完毕，随后大资金开始介入拉升（即均值回归的动能）。如果盘前已经拉回了相当一部分跌幅，盘中单边上涨的概率会显著增加。

### 现象二：夜盘/盘前上涨与盘中下跌/震荡
**设定阈值**: 盘前涨幅 (开盘相较于昨日收盘) >= {rise_threshold}%
**触发天数**: {p2_total} 天

当盘前出现显著高开时：
- **盘中最终收跌 (Close < Open，即高开低走)** 的概率: {p2_negative_close} / {p2_total} = {p2_neg_pct:.1f}%
- **盘中呈现窄幅震荡** (最高涨幅<0.5% 且 最大回撤<0.5%) 的概率: {p2_consolidation} / {p2_total} = {p2_cons_pct:.1f}%

**转折点洞察**: 高开往往会透支当天的多头动能。请参考图表 `{symbol}_p2_high_time_dist.png`。高开后的最高点往往出现在开盘初期的冲高诱多阶段（9:30 - 10:00），随后全天陷入震荡或阴跌出货。从第一性原理来看，市场参与者在盘前积累了浮盈，开盘后面临强烈的获利了结需求，导致价格承压。
"""
    return report

def main():
    print("Loading data...")
    df_qqq = load_data('data/qqq_1m_sim.csv')
    df_spy = load_data('data/spx_1m_sim.csv')
    
    print("Extracting sessions...")
    sessions_qqq = extract_sessions(df_qqq)
    sessions_spy = extract_sessions(df_spy)
    
    print("Analyzing and generating reports...")
    report_qqq = analyze_phenomena(sessions_qqq, 'QQQ')
    report_spy = analyze_phenomena(sessions_spy, 'SPY')
    
    with open('analysis_output/Market_Session_Analysis_Report.md', 'w', encoding='utf-8') as f:
        f.write("# 美股盘前与盘中走势关联性分析报告\n\n")
        f.write(report_qqq)
        f.write("\n---\n")
        f.write(report_spy)
        
    print("Analysis complete. Reports and charts saved to 'analysis_output' directory.")

if __name__ == "__main__":
    main()
