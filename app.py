import os
import json
import time
import threading
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import init_db, get_connection
from event_engine import EventEngine

# 初始化資料庫
init_db()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 背景自動啟動 IoT 感測模擬線程與倒數歸零即時監控
    def background_monitor():
        time.sleep(2)
        try:
            import simulator
            threading.Thread(target=simulator.simulation_worker, daemon=True).start()
        except Exception as e:
            print("Simulator worker error:", e)

        # 啟動秒級輪詢：倒數歸零立即觸發 LINE 推播與狀態流轉
        while True:
            time.sleep(1)
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT id, cycle_end_time FROM machines WHERE status = 'IN_USE' AND cycle_end_time IS NOT NULL")
                rows = cursor.fetchall()
                conn.close()

                now = datetime.now()
                for r in rows:
                    if r["cycle_end_time"]:
                        try:
                            end_dt = datetime.fromisoformat(r["cycle_end_time"])
                            if now >= end_dt:
                                ev = EventEngine.finish_wash_cycle(r["id"])
                                if ev:
                                    print(f"Cycle finished for {r['id']}, LINE push sent.")
                        except Exception:
                            pass
            except Exception:
                pass

    t = threading.Thread(target=background_monitor, daemon=True)
    t.start()
    yield

app = FastAPI(
    title="Smart Dorm Laundry Cloud IoT Engine",
    description="智慧校園宿舍洗衣機即時監控與排隊流 (雲端運算期中專案 MVP)",
    version="1.0.0",
    lifespan=lifespan
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(BASE_DIR, "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------------------------------
# WebSocket 連線管理員 (支援即時廣播推播)
# ----------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """廣播訊息給所有線上儀表板客戶端"""
        dead_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead_connections.append(connection)
        for dead in dead_connections:
            self.disconnect(dead)

ws_manager = ConnectionManager()

# ----------------------------------------------------
# Pydantic 資料模型
# ----------------------------------------------------
class TelemetryPayload(BaseModel):
    machine_id: str
    current_amp: float
    vibration_level: float
    timestamp: Optional[str] = None

class ReservationPayload(BaseModel):
    machine_id: str
    user_name: str
    user_contact: Optional[str] = "LINE"
    line_user_id: Optional[str] = None

class DemoActionPayload(BaseModel):
    action: str  # "start_cycle", "finish_cycle", "collect_clothes", "reset_all"
    machine_id: Optional[str] = None
    duration: Optional[int] = 35 * 60
    user_name: Optional[str] = None
    line_user_id: Optional[str] = None

class ReportPayload(BaseModel):
    machine_id: str
    issue_type: str
    note: Optional[str] = None
    reporter: str = "同學"

class PokePayload(BaseModel):
    machine_id: str
    action_type: str = "poke"  # "poke" or "basket"
    sender: str = "現場同學"

class DryerExtendPayload(BaseModel):
    machine_id: str
    extra_minutes: int = 10

# ----------------------------------------------------
# 工具輔助函式
# ----------------------------------------------------
def fetch_all_machines_data():
    """獲取 4 台機器最新狀態、倒數計時與排隊人數"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM machines ORDER BY id ASC")
    rows = cursor.fetchall()

    now = datetime.now()
    machines = []
    for r in rows:
        m = dict(r)
        
        # 計算排隊人數
        cursor.execute("SELECT COUNT(*) FROM reservations WHERE machine_id = ? AND status = 'WAITING'", (m["id"],))
        m["queue_count"] = cursor.fetchone()[0]

        # 計算運轉剩餘秒數
        if m["status"] == "IN_USE" and m["cycle_end_time"]:
            try:
                end_dt = datetime.fromisoformat(m["cycle_end_time"])
                remaining = int((end_dt - now).total_seconds())
                m["remaining_seconds"] = max(0, remaining)
            except Exception:
                m["remaining_seconds"] = 0
        else:
            m["remaining_seconds"] = 0

        # 計算超時未取衣物時間 (秒)
        if m["status"] == "FINISHED_UNCOLLECTED" and m["updated_at"]:
            try:
                up_dt = datetime.fromisoformat(m["updated_at"])
                m["uncollected_seconds"] = max(0, int((now - up_dt).total_seconds()))
            except Exception:
                m["uncollected_seconds"] = 0
        else:
            m["uncollected_seconds"] = 0

        # 計算保留剩餘時間 (秒)
        if m["status"] == "RESERVED" and m["reserved_expire_time"]:
            try:
                exp_dt = datetime.fromisoformat(m["reserved_expire_time"])
                m["reserved_remaining_seconds"] = max(0, int((exp_dt - now).total_seconds()))
            except Exception:
                m["reserved_remaining_seconds"] = 0
        else:
            m["reserved_remaining_seconds"] = 0

        machines.append(m)

    conn.close()
    return machines

# ----------------------------------------------------
# REST API 端點
# ----------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def serve_index(response: Response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    template_path = os.path.join(BASE_DIR, "templates", "index.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/machines")
async def get_machines(response: Response):
    """獲取所有機器當前狀態"""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return {"status": "success", "data": fetch_all_machines_data()}

@app.post("/api/telemetry")
async def ingest_telemetry(payload: TelemetryPayload):
    """
    IoT 邊緣數據攝入端點 (模擬 AWS IoT Core Ingestion)
    """
    triggered_events = EventEngine.process_telemetry(
        machine_id=payload.machine_id,
        current_amp=payload.current_amp,
        vibration_level=payload.vibration_level
    )

    # 透過 WebSocket 廣播最新機器數據給前端
    machines_data = fetch_all_machines_data()
    await ws_manager.broadcast({
        "type": "TELEMETRY_UPDATE",
        "telemetry": payload.model_dump(),
        "machines": machines_data,
        "events": triggered_events
    })

    return {
        "status": "success",
        "received": payload.machine_id,
        "events_triggered": len(triggered_events)
    }

@app.post("/api/reserve")
async def create_reservation(payload: ReservationPayload):
    """使用者預約排隊 (綁定 LINE User ID)"""
    res = EventEngine.add_reservation(
        machine_id=payload.machine_id,
        user_name=payload.user_name,
        user_contact=payload.user_contact or "LINE",
        line_user_id=payload.line_user_id
    )
    machines_data = fetch_all_machines_data()
    await ws_manager.broadcast({
        "type": "STATE_CHANGE",
        "action": "RESERVATION_ADDED",
        "machines": machines_data,
        "event": res["event"]
    })
    return {"status": "success", "data": res}

@app.post("/api/demo/action")
async def demo_action(payload: DemoActionPayload):
    """
    Demo 現場操作面板 (供課堂簡報快速展示各種狀態流轉)
    """
    event = None
    if payload.action == "start_cycle":
        if not payload.machine_id:
            raise HTTPException(status_code=400, detail="machine_id is required")
        duration = payload.duration or (35 * 60)
        event = EventEngine.start_wash_cycle(payload.machine_id, duration, payload.user_name, payload.line_user_id)

    elif payload.action == "finish_cycle":
        if not payload.machine_id:
            raise HTTPException(status_code=400, detail="machine_id is required")
        event = EventEngine.force_finish_cycle(payload.machine_id)

    elif payload.action == "collect_clothes":
        if not payload.machine_id:
            raise HTTPException(status_code=400, detail="machine_id is required")
        event = EventEngine.collect_clothes(payload.machine_id)

    elif payload.action == "reset_all":
        event = EventEngine.reset_all_machines()

    else:
        raise HTTPException(status_code=400, detail=f"Unknown action: {payload.action}")

    # 即時同步給所有儀表板
    machines_data = fetch_all_machines_data()
    await ws_manager.broadcast({
        "type": "STATE_CHANGE",
        "action": payload.action,
        "machine_id": payload.machine_id,
        "machines": machines_data,
        "event": event
    })

    return {"status": "success", "action": payload.action, "event": event}

@app.post("/api/machine/report")
async def report_machine(payload: ReportPayload):
    """機台報修反饋"""
    res = EventEngine.report_machine_issue(
        machine_id=payload.machine_id,
        issue_type=payload.issue_type,
        note=payload.note or "",
        reporter=payload.reporter
    )
    machines_data = fetch_all_machines_data()
    await ws_manager.broadcast({
        "type": "STATE_CHANGE",
        "action": "MACHINE_REPORTED",
        "machines": machines_data,
        "event": res["event"]
    })
    return {"status": "success", "data": res}

@app.post("/api/machine/poke")
async def poke_machine(payload: PokePayload):
    """匿名提醒 / 申請移至暫存籃"""
    res = EventEngine.send_anonymous_poke(
        machine_id=payload.machine_id,
        action_type=payload.action_type,
        sender=payload.sender
    )
    await ws_manager.broadcast({
        "type": "ANONYMOUS_POKE",
        "data": res,
        "event": res["event"]
    })
    return {"status": "success", "data": res}

@app.post("/api/dryer/extend")
async def extend_dryer(payload: DryerExtendPayload):
    """烘衣機追加時間"""
    res = EventEngine.extend_dryer_cycle(
        machine_id=payload.machine_id,
        extra_minutes=payload.extra_minutes
    )
    machines_data = fetch_all_machines_data()
    await ws_manager.broadcast({
        "type": "STATE_CHANGE",
        "action": "DRYER_EXTENDED",
        "machines": machines_data,
        "event": res["event"]
    })
    return {"status": "success", "data": res}

@app.get("/api/events")
async def get_events(limit: int = 25):
    """取得最新事件日誌串流"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM event_logs ORDER BY id DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return {"status": "success", "data": [dict(r) for r in rows]}

@app.get("/api/reservations/{machine_id}")
async def get_machine_reservations(machine_id: str):
    """取得特定機器的排隊清單"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, machine_id, user_name, user_contact, status, created_at 
        FROM reservations 
        WHERE machine_id = ? AND status = 'WAITING' 
        ORDER BY created_at ASC
    """, (machine_id,))
    rows = cursor.fetchall()
    conn.close()
    return {"status": "success", "data": [dict(r) for r in rows]}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """即時雙向通信 WebSocket"""
    await ws_manager.connect(websocket)
    try:
        # 連線成功時立即發送一次完整狀態
        await websocket.send_json({
            "type": "INITIAL_STATE",
            "machines": fetch_all_machines_data()
        })
        while True:
            # 保持監聽客戶端 ping/pong 或心跳
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
