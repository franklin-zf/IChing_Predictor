import os
import pytz
import yaml
import pandas as pd
from datetime import datetime, timedelta, date
from longport.openapi import Config, QuoteContext, Period, AdjustType, TradeSessions
from ultimate_predict import ultimate_iching_prediction, BAGUA_NAMES

def get_symbol_config(choice):
    if choice == '1':
        return 'QQQ.US', 'data/qqq_1m_sim.csv', 'QQQ'
    elif choice == '2':
        return '.SPX.US', 'data/spx_1m_sim.csv', 'SPY'
    return None, None, None

def update_data(symbol, csv_path):
    print(f"⏳ 正在连接 Longport 获取 {symbol} 昨夜最新行情数据...")
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        print("❌ 找不到 config.yaml 文件，请确保配置文件存在。")
        return False

    os.environ['LONGPORT_APP_KEY'] = cfg['longport']['app_key']
    os.environ['LONGPORT_APP_SECRET'] = cfg['longport']['app_secret']
    os.environ['LONGPORT_ACCESS_TOKEN'] = cfg['longport']['access_token']
    ctx = QuoteContext(Config.from_env())

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, parse_dates=['datetime'])
        last_date = df['datetime'].max().date()
    else:
        df = pd.DataFrame()
        last_date = date(2024, 1, 1) # 从2024年开始拉取
    
    ny_tz = pytz.timezone('America/New_York')
    ny_now = datetime.now(ny_tz)
    end_date = ny_now.date() + timedelta(days=1)
    
    cur = last_date
    rows = []
    
    while cur <= end_date:
        w_end = min(cur + timedelta(days=2), end_date)
        try:
            bars = ctx.history_candlesticks_by_date(symbol, Period.Min_1, AdjustType.NoAdjust, cur, w_end, TradeSessions.All)
            for b in bars:
                rows.append({
                    'datetime': str(b.timestamp),
                    'open': float(b.open),
                    'high': float(b.high),
                    'low': float(b.low),
                    'close': float(b.close),
                    'volume': float(b.volume)
                })
        except Exception as e:
            pass
        cur = w_end + timedelta(days=1)

    if rows:
        new_df = pd.DataFrame(rows)
        new_df['datetime'] = pd.to_datetime(new_df['datetime']).dt.tz_localize('Asia/Shanghai').dt.tz_convert('America/New_York').dt.tz_localize(None)
        
        if not df.empty:
            combined = pd.concat([df, new_df]).drop_duplicates(subset=['datetime']).sort_values('datetime')
        else:
            combined = new_df.drop_duplicates(subset=['datetime']).sort_values('datetime')
            
        # Ensure dir exists
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        combined.to_csv(csv_path, index=False)
        print(f"✅ 数据更新完成！本地数据库已更新至: {combined['datetime'].max()}")
    else:
        print("✅ 本地数据已经是最新，无需更新。")
    return True

def generate_report(csv_path, symbol_type):
    target_date = datetime.now().date()
    
    if target_date.weekday() == 5: 
        target_date += timedelta(days=2)
    elif target_date.weekday() == 6: 
        target_date += timedelta(days=1)
        
    target_date_str = str(target_date)
    
    print(f"\n❓ 今天 ({target_date_str}) 是否有重大宏观数据发布？")
    print("  [1] 无重大数据 (常态预测)")
    print("  [2] CPI / PCE 发布 (早8:30，波动率极速扩张，双向假突破多)")
    print("  [3] FOMC 美联储决议 (下午2:00，双向洗盘屠杀)")
    print("  [4] NFP 非农就业 (早8:30，趋势转向)")
    
    import sys
    choice = '1'
    if not sys.stdin.isatty():
        try:
            choice = sys.stdin.readline().strip()
        except:
            choice = '1'
    else:
        choice = input("👉 请输入选项 [1-4] (默认1): ").strip()
        
    if not choice: choice = '1'
    
    event_map = {'1': 'none', '2': 'pce', '3': 'fomc', '4': 'nfp'}
    event_type = event_map.get(choice, 'none')
    event_name = 'CPI/PCE' if event_type == 'pce' else event_type.upper() if event_type != 'none' else '无 (常态)'
    
    print(f"\n🔮 正在推演今晚美股 ({target_date_str}) {symbol_type} 的终极时空点位... (外应事件: {event_name})")
    
    try:
        res = ultimate_iching_prediction(csv_path, target_date_str, event_type, symbol_type=symbol_type)
    except Exception as e:
        print(f"❌ 预测算法执行出错: {e}")
        return
    
    if not res:
        print("❌ 历史数据不足，无法生成预测。请检查数据库。")
        return
        
    report_dir = "predictions"
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{symbol_type}_Prediction_{target_date_str}.md")
    
    pts = sorted(res['points'], key=lambda x: x[1], reverse=True)
    
    md_content = f"""# 🔮 {symbol_type} 终极时空决断预测战报
**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (亚洲时间)
**目标交易日**: {target_date_str} (美东时间)
**宏观外应**: {event_name}

---

## ☯️ 易经时空卦象
- **本卦**: 上{BAGUA_NAMES[res['upper']]} 下{BAGUA_NAMES[res['lower']]}
- **动爻**: 第 **{res['moving']}** 爻
- **状态解读**: *请结合最近的新闻面与此卦象推断市场今日的情绪张力。*

## 📊 核心空间基准
- **爆发基准 ATR ({res['multiplier']}x)**: `{res['atr']:.2f} USD` *(代表今日可能爆发的最大空间振幅)*
- **核心太极中枢 (Pivot)**: `{res['pivot']:.2f} USD`
> *注：若开盘价在 Pivot 之上，全天具有偏多抵抗性；若在 Pivot 之下，全天具有偏空承压性。*

## 🎯 盘中演化 6 大关键点位

| 爻位层级 | 绝对价格 (USD) | 空间属性 | 交易战术位 |
| :---: | :---: | :--- | :--- |
"""
    for p in pts:
        point_price = f"**{p[1]:.2f}**" if "动爻" not in p[2] else f"<strong style='color:red;'>{p[1]:.2f}</strong>"
        md_content += f"| 第 {p[0]} 爻 | {point_price} | {p[2]} | |\n"
        
    md_content += """
---
### ⚔️ 实战操作纪律
1. **拥抱极值**：当盘中价格冲击最上方的第5、6爻，或恐慌下探最下方的第1、2爻时，极易发生暴力反转，是绝佳的左侧狙击点（逢高买Put，逢低买Call）。
2. **远离中枢**：太极中枢附近是多空剧烈绞肉区，请放弃在中间地带进行任何期权买方操作。
3. **死盯动爻**：带有【动爻★】标记的点位是今日时空能量爆发的泉眼。一旦价格在此处遇阻或获得强支撑，其反弹/反转的动能将是全天最强的。
"""

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(md_content)
        
    print(f"✅ 预测战报已生成！请查看: {report_path}")

if __name__ == "__main__":
    print("===========================================")
    print("🌅 启动 每日晨间自动化预测任务")
    print("===========================================")
    print("请选择要预测的标的：")
    print("  [1] QQQ (纳斯达克100 ETF)")
    print("  [2] SPY (标普500指数)")
    
    import sys
    symbol_choice = '1'
    if not sys.stdin.isatty():
        try:
            symbol_choice = sys.stdin.readline().strip()
        except:
            symbol_choice = '1'
    else:
        symbol_choice = input("👉 请输入选项 [1-2] (默认1): ").strip()
        
    if not symbol_choice: symbol_choice = '1'
    
    symbol, csv_path, symbol_type = get_symbol_config(symbol_choice)
    
    if symbol:
        if update_data(symbol, csv_path):
            generate_report(csv_path, symbol_type)
    else:
        print("❌ 标的选择无效")
    
    print("===========================================")
    print("任务执行完毕，祝今日交易顺利！")