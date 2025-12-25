# src/telegram_bot.py
import requests
import time
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

def send_telegram_msg(message):
    """
    Gửi tin nhắn Telegram.
    Tự động chia nhỏ nếu tin nhắn quá dài (>3000 ký tự) để tránh lỗi 400.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Chưa cấu hình TELEGRAM_TOKEN hoặc CHAT_ID.")
        return False
        
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    # Telegram giới hạn 4096 ký tự. Ta cắt ở ngưỡng an toàn 3000 để chừa chỗ cho thẻ HTML.
    MAX_LEN = 3000
    
    messages_to_send = []
    
    # 1. LOGIC CẮT TIN NHẮN
    if len(message) <= MAX_LEN:
        messages_to_send.append(message)
    else:
        # Lặp để cắt dần
        curr_msg = message
        while len(curr_msg) > MAX_LEN:
            # Tìm vị trí xuống dòng (\n) gần nhất trước ngưỡng MAX_LEN
            # Để tránh cắt ngang thẻ HTML như <b>...</b>
            split_idx = curr_msg.rfind('\n', 0, MAX_LEN)
            
            if split_idx == -1: 
                split_idx = MAX_LEN # Nếu không tìm thấy dòng nào thì cắt cứng
            
            # Lấy phần đầu
            chunk = curr_msg[:split_idx]
            messages_to_send.append(chunk)
            
            # Cập nhật phần còn lại (bỏ qua ký tự xuống dòng ở chỗ cắt)
            curr_msg = curr_msg[split_idx:].strip()
            
        if curr_msg:
            messages_to_send.append(curr_msg)

    # 2. GỬI TỪNG PHẦN
    success_count = 0
    total = len(messages_to_send)
    
    for i, msg_chunk in enumerate(messages_to_send):
        # Thêm số trang nếu tin nhắn bị chia nhỏ
        if total > 1:
            header = f"(Phần {i+1}/{total})\n"
            final_text = header + msg_chunk
        else:
            final_text = msg_chunk

        payload = {
            "chat_id": TELEGRAM_CHAT_ID, 
            "text": final_text, 
            "parse_mode": "HTML"
        }
        
        try: 
            resp = requests.post(url, json=payload, timeout=15)
            if resp.status_code == 200:
                success_count += 1
            else:
                print(f"Telegram Error (Part {i+1}): {resp.text}")
                
            # Nghỉ nhẹ 0.5s giữa các tin để tránh bị Spam Block
            time.sleep(0.5)
            
        except Exception as e: 
            print(f"Connection Error: {e}")
            
    # Trả về True nếu gửi được ít nhất 1 phần
    return success_count > 0