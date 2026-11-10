"""
IoT Edge Simulator (物聯網邊緣裝置感測模擬器)
模擬 4 台校園宿舍洗衣機/烘衣機之電流與震動傳感器
透過 HTTP POST 定期傳輸 Telemetry 數據至雲端 IoT Ingestion API
"""

import os
import time
import random
import requests
from datetime import datetime
import threading
import sys

PORT = os.environ.get("PORT", "8000")
API_URL = os.environ.get("API_URL", f"http://127.0.0.1:{PORT}/api/telemetry")

MACHINES = [
    # 男生宿舍 (A棟)
    {"id": "washer-1", "name": "A棟 洗衣機 1號", "type": "washer", "mode": "STANDBY", "timer": 0},
    {"id": "washer-2", "name": "A棟 洗衣機 2號", "type": "washer", "mode": "STANDBY", "timer": 0},
    {"id": "dryer-1",  "name": "A棟 烘衣機 1號", "type": "dryer",  "mode": "STANDBY", "timer": 0},
    {"id": "dryer-2",  "name": "A棟 烘衣機 2號", "type": "dryer",  "mode": "STANDBY", "timer": 0},
    # 女生宿舍 (B棟)
    {"id": "f-washer-1", "name": "B棟 洗衣機 1號", "type": "washer", "mode": "STANDBY", "timer": 0},
    {"id": "f-washer-2", "name": "B棟 洗衣機 2號", "type": "washer", "mode": "STANDBY", "timer": 0},
    {"id": "f-dryer-1",  "name": "B棟 烘衣機 1號", "type": "dryer",  "mode": "STANDBY", "timer": 0},
    {"id": "f-dryer-2",  "name": "B棟 烘衣機 2號", "type": "dryer",  "mode": "STANDBY", "timer": 0},
]

def generate_sensor_values(machine):
    """
    依據機器當前模式生成模擬物理傳感器數值
    模式:
      STANDBY: 待機 (電流 ~0.1A, 震動 0.0)
      WASHING: 洗滌運轉 (電流 8.0~10.0A, 震動 1.5~3.0)
      SPINNING: 高速脫水/強烘 (電流 10.5~12.2A, 震動 5.0~7.5)
      FINISHED: 洗畢驟降 (電流 0.05A, 震動 0.0)
    """
    mode = machine["mode"]
    
    if mode == "STANDBY":
        current = round(random.uniform(0.05, 0.15), 2)
        vibration = round(random.uniform(0.0, 0.15), 2)
    elif mode == "WASHING":
        current = round(random.uniform(7.8, 9.8), 2)
        vibration = round(random.uniform(1.8, 3.4), 2)
    elif mode == "SPINNING":
        current = round(random.uniform(10.2, 12.0), 2)
        vibration = round(random.uniform(4.8, 7.8), 2)
    elif mode == "FINISHED":
        current = round(random.uniform(0.0, 0.1), 2)
        vibration = 0.0
    else:
        current, vibration = 0.0, 0.0

    return current, vibration

def send_telemetry(machine_id, current, vibration):
    payload = {
        "machine_id": machine_id,
        "current_amp": current,
        "vibration_level": vibration,
        "timestamp": datetime.now().isoformat()
    }
    try:
        resp = requests.post(API_URL, json=payload, timeout=2.5)
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False

def simulation_worker():
    """模擬器定時發送工作執行緒"""
    print("=" * 65)
    print(" 🚀 IoT Edge Telemetry Simulator 已啟動 (每 3 秒自動回傳數據)")
    print(f" 📡 目標雲端端點: {API_URL}")
    print("=" * 65)

    step = 0
    while True:
        step += 1
        backend_machines = {}
        try:
            r = requests.get("http://127.0.0.1:8000/api/machines", timeout=1.5)
            if r.status_code == 200:
                for b_m in r.json().get("data", []):
                    backend_machines[b_m["id"]] = b_m
        except Exception:
            pass

        for m in MACHINES:
            b_info = backend_machines.get(m["id"])
            if b_info and b_info.get("status") == "IN_USE":
                current = round(random.uniform(8.2, 9.8), 2)
                vibration = round(random.uniform(1.8, 3.2), 2)
                mode_str = "WASHING"
            else:
                current, vibration = generate_sensor_values(m)
                mode_str = m["mode"]

            success = send_telemetry(m["id"], current, vibration)

        time.sleep(3)

def interactive_cli():
    """互動式 CLI 操作選單，便於展示演示"""
    time.sleep(1)
    while True:
        print("\n" + "-" * 40)
        print(" 【IoT 模擬器手動操作選單】")
        print(" 1. 設定 washer-1 開始運轉 (30秒洗程)")
        print(" 2. 設定 washer-2 開始運轉 (30秒洗程)")
        print(" 3. 設定 dryer-1  開始烘衣 (30秒烘程)")
        print(" 4. 設定 washer-1 立即完成 (電流降至 0A)")
        print(" 5. 重設全部機台為待機狀態 (STANDBY)")
        print(" Q. 退出模擬器")
        print("-" * 40)
        
        try:
            choice = input("請輸入指令編號 > ").strip()
        except EOFError:
            break

        if choice == "1":
            MACHINES[0]["mode"] = "WASHING"
            MACHINES[0]["timer"] = 30
            print(">>> 已啟動 washer-1 洗滌模式 (30s)！")
        elif choice == "2":
            MACHINES[1]["mode"] = "WASHING"
            MACHINES[1]["timer"] = 30
            print(">>> 已啟動 washer-2 洗滌模式 (30s)！")
        elif choice == "3":
            MACHINES[2]["mode"] = "WASHING"
            MACHINES[2]["timer"] = 30
            print(">>> 已啟動 dryer-1 烘衣模式 (30s)！")
        elif choice == "4":
            MACHINES[0]["mode"] = "FINISHED"
            MACHINES[0]["timer"] = 0
            print(">>> washer-1 強制設為 FINISHED！")
        elif choice == "5":
            for m in MACHINES:
                m["mode"] = "STANDBY"
                m["timer"] = 0
            print(">>> 所有機台已重設為待機！")
        elif choice.lower() == "q":
            print(">>> 模擬器已關閉。")
            sys.exit(0)

if __name__ == "__main__":
    t = threading.Thread(target=simulation_worker, daemon=True)
    t.start()
    
    if sys.stdin and sys.stdin.isatty():
        interactive_cli()
    else:
        while True:
            time.sleep(1)
