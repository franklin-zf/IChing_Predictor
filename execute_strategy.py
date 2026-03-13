import os
import yaml
import time
from datetime import datetime, date, timedelta
import pandas as pd
from longport.openapi import Config, QuoteContext, TradeContext, OrderType, OrderSide, TimeInForceType, SubType, PushQuote
from ultimate_predict import ultimate_iching_prediction, load_data

def get_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def run_trading_bot():
    print("===========================================")
    print("🚀 启动 长桥 OpenAPI 易经期权算法交易 (模拟环境准备)")
    print("===========================================")
    
    cfg = get_config()
    
    # 1. 建立长桥连接
    # 注意：模拟账户需要确保在长桥开放平台使用的是模拟环境的凭证，或在APP端设置权限。
    print("⏳ 正在连接长桥服务器...")
    app_key = cfg['longport']['app_key']
    app_secret = cfg['longport']['app_secret']
    access_token = cfg['longport']['access_token']
    
    try:
        # 默认 Config
        longport_config = Config(app_key, app_secret, access_token)
        
        quote_ctx = QuoteContext(longport_config)
        trade_ctx = TradeContext(longport_config)
        
        # 2. 检查账户资产，验证连接
        balances = trade_ctx.account_balance()
        if balances:
            print("✅ 账户连接成功！资产情况如下：")
            for b in balances:
                print(f"   [{b.currency}] 净资产: {b.net_assets}, 总现金: {b.total_cash}, 购买力: {b.buy_power}")
        else:
            print("⚠️ 未获取到资产信息。")
            
    except Exception as e:
        print(f"❌ 长桥API连接失败，请检查 config.yaml 配置或网络情况: {e}")
        return

    # 3. 准备今日预测点位
    symbol = "QQQ.US"
    csv_path = 'data/qqq_1m_sim.csv'
    today_str = str(datetime.now().date())
    
    print(f"\n🔮 获取 {today_str} {symbol} 今日时空卦象预测点位...")
    try:
        res = ultimate_iching_prediction(csv_path, today_str, 'none', symbol_type='QQQ')
        if not res:
            print("❌ 数据不足，无法获取预测点位。请先执行 python daily_task.py 更新数据。")
            return
            
        pivot = res['pivot']
        levels = res['points']
        atr = res['atr']
        
        print(f"太极中枢 (Pivot): {pivot:.2f}")
        for p in sorted(levels, key=lambda x: x[1], reverse=True):
            print(f"第 {p[0]} 爻: {p[1]:.2f} - {p[2]}")
            
    except Exception as e:
        print(f"❌ 获取预测失败: {e}")
        return

    # 4. 实时行情监控与期权交易逻辑准备
    print("\n📡 开始订阅实时行情...")
    
    # 获取近期期权链的示例：
    try:
        dates = quote_ctx.option_chain_expiry_date_list(symbol)
        if dates:
            target_expiry = dates[0] # 获取最近到期的期权 (末日轮)
            print(f"📅 获取到期权链，最近到期日为: {target_expiry}")
    except Exception as e:
        print(f"⚠️ 获取期权链失败，此功能可能需要高级期权行情权限: {e}")

    # 定义行情回调函数
    def on_quote_update(environment, event_data: PushQuote):
        # 此处 event_data 包含了最新的价格
        current_price = float(event_data.last_done)
        now_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{now_time}] {symbol} 最新价: {current_price:.2f}")
        
        # --- 策略核心逻辑 (伪代码) ---
        # 1. 检查当前价格是否触及第 1,2 爻 (做多区) 或 第 5,6 爻 (做空区)
        # 2. 判断买入 Call 还是 Put
        # 3. 查询当前期权链 (option_chain_info_by_date)
        # 4. 挑选平值 (ATM) 或 稍微虚值 (OTM) 期权代码
        # 5. 执行下单：
        #    trade_ctx.submit_order(
        #        symbol=option_symbol,
        #        order_type=OrderType.Market, # 或限价单
        #        side=OrderSide.Buy,
        #        submitted_quantity=1,
        #        time_in_force=TimeInForceType.Day
        #    )
        
    quote_ctx.set_on_quote(on_quote_update)
    
    # 订阅标的实时报价
    try:
        quote_ctx.subscribe([symbol], sub_types=[SubType.Quote])
        print("✅ 实时行情订阅成功！等待数据推送中... (按 Ctrl+C 停止)")
        
        # 保持主线程存活
        print("💡 您可以通过回调函数接收长桥推送的实时报价。由于没有美股期权LV2行情权限，如要测试下单可直接使用基础买卖逻辑。")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 收到中断信号，正在退出...")
    except Exception as e:
        print(f"❌ 订阅失败: {e}")
    finally:
        print("🔌 正在断开连接，清理资源...")
        # quote_ctx.unsubscribe([symbol], sub_types=[SubType.Quote])

if __name__ == "__main__":
    run_trading_bot()
