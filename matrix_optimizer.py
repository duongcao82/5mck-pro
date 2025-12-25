import pandas as pd
from backtest import get_potential_trades

def run_matrix_optimization():
    # Danh sách VN100 tiêu biểu
    VN100 = ["HPG", "SSI", "STB", "VCB", "MBB", "TCB", "CTG", "FPT", "VIC", "VHM"]
    initial_balance = 1_000_000_000
    
    # Ma trận Score cần kiểm tra
    score_thresholds = [3.0, 3.5, 4.0]
    matrix_results = []

    print("--- CHẠY MA TRẬN TỐI ƯU HÓA VN100 (SMC/PA) ---")
    
    for s_min in score_thresholds:
        all_potential = []
        for symbol in VN100:
            trades = get_potential_trades(symbol)
            filtered = [t for t in trades if t.score >= s_min]
            all_potential.extend(filtered)
            
        all_potential.sort(key=lambda x: x.entry_time)
        
        # Giả lập Portfolio
        balance = initial_balance
        for t in all_potential:
            # 1% Risk Management
            qty = (initial_balance * 0.01 / abs(t.entry_price - t.sl_price)) // 10 * 10
            if (qty * t.entry_price) <= balance * 0.2:
                pnl = (t.exit_price - t.entry_price) * qty if t.side == "BUY" else (t.entry_price - t.exit_price) * qty
                balance += pnl

        roi = (balance - initial_balance) / initial_balance * 100
        matrix_results.append({"Min_Score": s_min, "Trades": len(all_potential), "ROI": f"{roi:.2f}%"})

    print(pd.DataFrame(matrix_results))

if __name__ == "__main__": run_matrix_optimization()