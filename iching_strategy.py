from __future__ import annotations
import pandas as pd
from typing import Optional, List, Dict
from dataclasses import dataclass

@dataclass
class IChingTrade:
    date: str
    side: str
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    pnl_pct: float
    exit_reason: str
    trigger_level: str

class IChingOptionStrategy:
    def __init__(self, pivot: float, levels: List[tuple], atr: float):
        self.pivot = pivot
        self.levels = sorted(levels, key=lambda x: x[1]) 
        self.atr = atr
        
        self.entry_tolerance = atr * 0.05  
        self.tp_tolerance = atr * 0.10     
        self.hard_stop_atr_ratio = 0.20
        
        self.current_trade = None

    def _get_zone(self, price: float) -> str:
        if price > self.pivot: return "Upper"
        else: return "Lower"

    def run(self, intraday_data: pd.DataFrame) -> Optional[IChingTrade]:
        if intraday_data.empty: return None
        
        # 只打两头：只做极限的第1, 2, 5, 6爻，放弃中间绞肉机第3, 4爻
        res_levels = [l for l in self.levels if l[1] > self.pivot and l[0] in [5, 6]]
        sup_levels = [l for l in self.levels if l[1] < self.pivot and l[0] in [1, 2]]
        
        trades = []
        
        for ts, row in intraday_data.iterrows():
            price = row['close']
            
            if self.current_trade is not None:
                side = self.current_trade['side']
                entry_price = self.current_trade['entry_price']
                target = self.current_trade['target_price']
                stop_loss = self.current_trade['stop_loss']
                
                duration_mins = (ts - self.current_trade['entry_time']).total_seconds() / 60
                
                if side == 'long':
                    pnl = (price - entry_price) / entry_price
                    if price <= stop_loss:
                        self.current_trade['exit'] = (ts, price, pnl, "Hard Stop")
                    elif price >= target - self.tp_tolerance: 
                        self.current_trade['exit'] = (ts, price, pnl, "Take Profit")
                    elif duration_mins >= 30 and pnl < 0.002:
                        self.current_trade['exit'] = (ts, price, pnl, "Time Stop")
                        
                elif side == 'short':
                    pnl = (entry_price - price) / entry_price
                    if price >= stop_loss:
                        self.current_trade['exit'] = (ts, price, pnl, "Hard Stop")
                    elif price <= target + self.tp_tolerance: 
                        self.current_trade['exit'] = (ts, price, pnl, "Take Profit")
                    elif duration_mins >= 30 and pnl < 0.002:
                        self.current_trade['exit'] = (ts, price, pnl, "Time Stop")
                
                if ts.time() >= pd.to_datetime('15:55:00').time() and 'exit' not in self.current_trade:
                    self.current_trade['exit'] = (ts, price, pnl, "EOD")
                
                if 'exit' in self.current_trade:
                    ext = self.current_trade['exit']
                    trades.append(IChingTrade(
                        date=str(ts.date()),
                        side=side,
                        entry_time=self.current_trade['entry_time'],
                        exit_time=ext[0],
                        entry_price=entry_price,
                        exit_price=ext[1],
                        pnl_pct=ext[2],
                        exit_reason=ext[3],
                        trigger_level=self.current_trade['trigger_level']
                    ))
                    self.current_trade = None
                    break 
                
                continue 

            if ts.time() < pd.to_datetime('09:45:00').time():
                continue
                
            for lvl in res_levels:
                lvl_price = lvl[1]
                lvl_name = f"第{lvl[0]}爻({lvl_price:.2f})"
                
                if lvl_price - self.entry_tolerance <= price <= lvl_price + self.entry_tolerance:
                    if row['close'] < row['open'] and row['close'] < (lvl_price - self.entry_tolerance * 0.2): 
                        target_price = min(self.pivot, price - self.atr * 0.5)
                        stop_loss_price = price + (self.atr * self.hard_stop_atr_ratio) 
                        
                        self.current_trade = {
                            'side': 'short',
                            'entry_time': ts,
                            'entry_price': price,
                            'target_price': target_price,
                            'stop_loss': stop_loss_price,
                            'trigger_level': lvl_name
                        }
                        break
            
            if self.current_trade: continue
                
            for lvl in sup_levels:
                lvl_price = lvl[1]
                lvl_name = f"第{lvl[0]}爻({lvl_price:.2f})"
                
                if lvl_price - self.entry_tolerance <= price <= lvl_price + self.entry_tolerance:
                    if row['close'] > row['open'] and row['close'] > (lvl_price + self.entry_tolerance * 0.2): 
                        target_price = max(self.pivot, price + self.atr * 0.5)
                        stop_loss_price = price - (self.atr * self.hard_stop_atr_ratio) 
                        
                        self.current_trade = {
                            'side': 'long',
                            'entry_time': ts,
                            'entry_price': price,
                            'target_price': target_price,
                            'stop_loss': stop_loss_price,
                            'trigger_level': lvl_name
                        }
                        break
                    
        return trades[0] if trades else None