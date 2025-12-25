"""
VN100 SNIPER BACKTEST - PHIÊN BẢN CHUẨN QUẢN TRỊ VỐN 1 TỶ
- Metrics: Total, TP, BE, SL, Winrate, Not Loss Rate, ROI, Net Profit, HoldTime.
- Logic: BE @ 1.2R, TP >= 1.5R, Max Hold 45 Days.
"""
import warnings

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
)
import numpy as np
import pandas as pd


from dataclasses import dataclass
from typing import List
from data import load_data_with_cache 
from smc_core import ensure_smc_columns
from backtest_scanner import scan_symbol_backtest
import warnings



@dataclass
class Trade:
    symbol: str
    side: str
    entry_time: pd.Timestamp
    entry_price: float
    sl_price: float
    tp_price: float

    exit_time: pd.Timestamp = None
    exit_price: float = None
    result: str = "OPEN"

    quantity: int = 0
    pnl: float = 0
    balance_after: float = 0
    score: float = 0.0

    # metadata để thống kê
    model: str = ""
    confirm_tf: str = "D1"
    is_confirm: bool = False

def get_potential_trades(symbol: str, days: int = 300) -> List[Trade]:
    df = load_data_with_cache(symbol, days_to_load=days, timeframe="1D")
    if df is None or df.empty or len(df) < 60: return []
    df = ensure_smc_columns(df)
    trades = []
    
    # Quét 200 ngày gần nhất
    start_idx = max(0, len(df) - 200)
    for i in range(start_idx, len(df) - 4):
        df_slice = df.iloc[:i+1].copy()
        res, reason = scan_symbol_backtest(symbol, df_slice)
        if not res or res['Score'] < 3.5: continue # Ngưỡng Sniper 3.5

        side, entry_p, sl_p = res['Signal'], res['POI'], res['SL']
        risk = abs(entry_p - sl_p)
        if risk == 0: continue

        # --- CHIẾN THUẬT TP & BE MỚI ---
        try:
            tp_list = [float(x.strip().replace(',', '')) for x in str(res['TP']).split('|')]
            # Ưu tiên lấy vùng cản SMC xa hơn để tăng Holdtime & ROI
            tp_p = tp_list[-1] if len(tp_list) > 1 else tp_list[0]
            # Ép RR tối thiểu 1.5
            min_tp = entry_p + (1.5 * risk) if side == "BUY" else entry_p - (1.5 * risk)
            tp_p = max(tp_p, min_tp) if side == "BUY" else min_tp
        except:
            tp_p = entry_p + (2.0 * risk) if side == "BUY" else entry_p - (2.0 * risk)
            
        # Kích hoạt dời hòa vốn sớm tại 1.2R để giảm tỷ lệ SL
        be_trig = entry_p + (1.2 * risk) if side == "BUY" else entry_p - (1.2 * risk)

        # EXIT LOGIC
        exit_p, res_str, exit_idx = entry_p, "OPEN", i + 1
        curr_sl, moved_be = sl_p, False
        
        for j in range(i + 1, len(df)):
            if j < i + 3: continue # T+2.5 quy định VN
            r = df.iloc[j]
            if side == "BUY":
                if not moved_be and r['High'] >= be_trig: curr_sl, moved_be = entry_p, True
                if r['Low'] <= curr_sl:
                    exit_p, res_str, exit_idx = curr_sl, ("BE" if moved_be else "SL"), j
                    break
                elif r['High'] >= tp_p:
                    exit_p, res_str, exit_idx = tp_p, "TP", j
                    break
            else: # SELL
                if not moved_be and r['Low'] <= be_trig: curr_sl, moved_be = entry_p, True
                if r['High'] >= curr_sl:
                    exit_p, res_str, exit_idx = curr_sl, ("BE" if moved_be else "SL"), j
                    break
                elif r['Low'] <= tp_p:
                    exit_p, res_str, exit_idx = tp_p, "TP", j
                    break

            if (j - i) >= 45: # Hold tới 45 ngày để bắt sóng lớn
                exit_p, res_str, exit_idx = r['Close'], "TIME_EXIT", j
                break
        
        trades.append(Trade(
            symbol, side, df.index[i+1], entry_p, sl_p, tp_p,
            df.index[exit_idx], exit_p, res_str,
            score=res['Score'],
            model=res.get("Model",""),
            confirm_tf=res.get("ConfirmTF","D1"),
            is_confirm=bool(res.get("IsConfirm", False)),
        ))

    return trades

if __name__ == "__main__":
    VN100 = [
    # BANK
    "ACB","BID","CTG","HDB","LPB","MBB","MSB","OCB","SHB","STB",
    "TCB","TPB","VCB","VIB","VPB",

    # CHỨNG KHOÁN
    "SSI","HCM","VCI","VND","MBS","FTS","BSI","ORS",

    # BẤT ĐỘNG SẢN
    "VHM","VIC","VRE","NVL","DXG","DIG","KDH","NLG","PDR","KBC",
    "BCM","HDG","IDC","SZC","LHG",

    # DẦU KHÍ – NĂNG LƯỢNG
    "GAS","PLX","POW","PVD","PVS","BSR",

    # THÉP – VẬT LIỆU
    "HPG","HSG","NKG","DGC","DPM","DCM","CSV",

    # TIÊU DÙNG – BÁN LẺ
    "MWG","FRT","DGW","PNJ","VNM","SAB","MSN","VHC","ANV",

    # CÔNG NGHỆ – VIỄN THÔNG
    "FPT","CMG","ELC",

    # HẠ TẦNG – CÔNG NGHIỆP
    "GVR","REE","GEG","PC1","CTD","FCN","HHV",

    # LOGISTICS – CẢNG BIỂN
    "GMD","VSC","HAH","SCS",

    # KHÁC
    "BVH","BWE","MWG","IMP","DHG"
]

    INITIAL_BALANCE = 1_000_000_000
    RISK_PER_TRADE = 0.02 # 2% rủi ro
    current_balance = INITIAL_BALANCE
    all_potential = []

    print(f">>> ĐANG QUÉT SNIPER VN100 ({len(VN100)} MÃ) - 200 NGÀY GẦN NHẤT <<<")
    for s in VN100:
        all_potential.extend(get_potential_trades(s))
    
    all_potential.sort(key=lambda x: x.entry_time)
    completed_trades, active_trades = [], []

    for t in all_potential:
        active_trades = [at for at in active_trades if at.exit_time > t.entry_time]
        if len(active_trades) >= 5: continue # Tối đa 5 vị thế

        risk_p = abs(t.entry_price - t.sl_price)
        qty = (current_balance * RISK_PER_TRADE / risk_p) // 10 * 10
        if (qty * t.entry_price) > (current_balance * 0.25):
            qty = (current_balance * 0.25) // t.entry_price // 10 * 10
        
        if qty >= 10:
            t.quantity = qty
            t.pnl = ((t.exit_price - t.entry_price) * t.quantity if t.side == "BUY" else (t.entry_price - t.exit_price) * t.quantity)
            t.pnl -= (t.entry_price + t.exit_price) * t.quantity * 0.002 # Thuế phí
            current_balance += t.pnl
            completed_trades.append(t)
            active_trades.append(t)

    # --- BÁO CÁO CHI TIẾT ĐÚNG YÊU CẦU ---
    if completed_trades:
        df_res = pd.DataFrame([t.__dict__ for t in completed_trades])
        df_res['hold_time'] = (df_res['exit_time'] - df_res['entry_time']).dt.days
        
        tp_count = len(df_res[df_res['result'] == 'TP'])
        be_count = len(df_res[df_res['result'] == 'BE'])
        sl_count = len(df_res[df_res['result'] == 'SL'])
        net_profit = current_balance - INITIAL_BALANCE
        
        print("\n" + "="*45)
        print(f"💰 TỔNG KẾT TÀI CHÍNH SNIPER")
        print(f"• Vốn ban đầu     : {INITIAL_BALANCE:,.0f} VNĐ")
        print(f"• Lợi nhuận ròng  : {net_profit:,.0f} VNĐ")
        print(f"• ROI             : {(net_profit/INITIAL_BALANCE)*100:.2f}%")
        print("-" * 45)
        print(f"📊 HIỆU SUẤT GIAO DỊCH")
        print(f"• Tổng số lệnh    : {len(df_res)}")
        print(f"• Chi tiết        : {tp_count} TP | {be_count} BE | {sl_count} SL")
        print(f"• Tỷ lệ thắng (Win Rate)  : {(tp_count/len(df_res))*100:.2f}%")
        print(f"• Tỷ lệ không thua (TP+BE): {((tp_count+be_count)/len(df_res))*100:.2f}%")
        print(f"• Hold Time TB    : {df_res['hold_time'].mean():.1f} ngày")
        print("="*45)
        # ===== MODEL STATS =====
        g = df_res.groupby("model").agg(
            trades=("result","count"),
            tp=("result", lambda x: int((x=="TP").sum())),
            be=("result", lambda x: int((x=="BE").sum())),
            sl=("result", lambda x: int((x=="SL").sum())),
            net_pnl=("pnl","sum"),
            avg_score=("score","mean"),
            confirm_rate=("is_confirm","mean"),
        ).reset_index()
        g["winrate"] = (g["tp"]/g["trades"]*100).round(2)
        g["not_loss"] = ((g["tp"]+g["be"])/g["trades"]*100).round(2)
        g = g.sort_values(["net_pnl","trades"], ascending=[False, False])

        print("\n===== MODEL STATS - ALL =====")
        print(g.to_string(index=False))

        df_res.to_csv("backtest_vn100_optimized.csv", index=False)