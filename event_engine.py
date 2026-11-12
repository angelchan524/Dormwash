import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import os
import requests

LINE_CHANNEL_ACCESS_TOKEN = os.environ.get(
    "LINE_CHANNEL_ACCESS_TOKEN",
    "CHfmnGjiypG4EvILvXtMDK+95F6kAsF69R5iR4LcszuHM+EU5GvsczyY6mJUOUS7frT2Zr0lkmk08ksEo6fR/AuaA0pgpBpQhJxFg9nUQ03SdUUOMGC3VXiY1JhRDkcJB3sfqhGNOTHxM9JyYPR9rwdB04t89/1O/w1cDnyilFU="
)
DEFAULT_TEST_USER_ID = os.environ.get("DEFAULT_TEST_USER_ID", "U1376d41faf186c1405f53951417b88f4")

def send_line_push_message(to_user_id: str, text: str) -> bool:
    """發送 LINE Messaging API 主動推播訊息"""
    target = to_user_id.strip() if to_user_id and to_user_id.strip() else DEFAULT_TEST_USER_ID
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }
    payload = {
        "to": target,
        "messages": [
            {
                "type": "text",
                "text": text
            }
        ]
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=5)
        print(f"[LINE Push] To: {target} | Status: {resp.status_code}")
        return resp.status_code == 200
    except Exception as e:
        print(f"[LINE Push Exception]: {e}")
        return False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "laundry.db")

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

class EventEngine:
    """
    事件驅動核心引擎 (模擬雲端 EventBridge + Serverless Lambda 運作)
    負責接收 IoT 邊緣事件、評估狀態機狀態轉移、推播通知與排隊佇列處理。
    """

    @staticmethod
    def log_event(machine_id: str, event_type: str, source: str, message: str) -> Dict[str, Any]:
        """寫入事件日誌 (模擬 CloudWatch Logs / EventBridge Event Store)"""
        now = datetime.now().isoformat()
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO event_logs (machine_id, event_type, source, message, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (machine_id, event_type, source, message, now))
        event_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return {
            "id": event_id,
            "machine_id": machine_id,
            "event_type": event_type,
            "source": source,
            "message": message,
            "timestamp": now
        }

    @classmethod
    def process_telemetry(cls, machine_id: str, current_amp: float, vibration_level: float) -> List[Dict[str, Any]]:
        """
        處理 IoT 邊緣感測數據並觸發狀態機轉移規則 (Rule Engine Evaluation)
        """
        events_triggered = []
        now = datetime.now()
        now_str = now.isoformat()

        conn = get_db()
        cursor = conn.cursor()

        # 1. 記錄時序感測數據 (Time-series Log)
        cursor.execute("""
            INSERT INTO telemetry_logs (machine_id, current_amp, vibration_level, timestamp)
            VALUES (?, ?, ?, ?)
        """, (machine_id, current_amp, vibration_level, now_str))

        # 2. 獲取當前機器狀態
        cursor.execute("SELECT * FROM machines WHERE id = ?", (machine_id,))
        machine = cursor.fetchone()
        if not machine:
            conn.close()
            return events_triggered

        current_status = machine["status"]
        cycle_end_time_str = machine["cycle_end_time"]
        cycle_end_time = datetime.fromisoformat(cycle_end_time_str) if cycle_end_time_str else None

        # 更新即時感測器數值
        cursor.execute("""
            UPDATE machines 
            SET current_amp = ?, vibration_level = ?, updated_at = ?
            WHERE id = ?
        """, (current_amp, vibration_level, now_str, machine_id))
        conn.commit()

        # 3. 狀態機規則評估 (State Machine Transitions)
        # 規則 A: 機台在運轉中 (IN_USE)，當預計結束時間抵達 (now >= cycle_end_time) 切換為 FINISHED_UNCOLLECTED
        if current_status == "IN_USE":
            is_time_up = (cycle_end_time and now >= cycle_end_time)

            if is_time_up:
                event = cls.finish_wash_cycle(machine_id)
                if event:
                    events_triggered.append(event)

        # 規則 B: 預約保留超時檢查 (若保留超過 10 分鐘未投幣/啟動，釋放給下一位)
        elif current_status == "RESERVED":
            reserved_expire_str = machine["reserved_expire_time"]
            if reserved_expire_str:
                expire_time = datetime.fromisoformat(reserved_expire_str)
                if now >= expire_time:
                    # 保留超時，換下一位或釋放
                    cursor.execute("""
                        UPDATE machines 
                        SET status = 'AVAILABLE',
                            current_user_name = NULL,
                            current_line_user_id = NULL,
                            reserved_user_name = NULL,
                            reserved_contact = NULL,
                            reserved_line_user_id = NULL,
                            reserved_expire_time = NULL,
                            updated_at = ?
                        WHERE id = ?
                    """, (now_str, machine_id))
                    conn.commit()

                    event = cls.log_event(
                        machine_id=machine_id,
                        event_type="RESERVATION_EXPIRED",
                        source="Lambda_QueueWorker",
                        message=f"{machine['name']} 預約保留期（10分鐘）已過，系統已重置機台為閒置狀態。"
                    )
                    events_triggered.append(event)

        conn.close()
        return events_triggered

    @classmethod
    def finish_wash_cycle(cls, machine_id: str, reason: str = "CYCLE_END") -> Optional[Dict[str, Any]]:
        """
        核心場景 1：洗程結束瞬間，切換狀態並調用 LINE Messaging API 發送推播
        """
        now = datetime.now()
        now_str = now.isoformat()
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM machines WHERE id = ?", (machine_id,))
        machine = cursor.fetchone()

        if not machine or machine["status"] != "IN_USE":
            conn.close()
            return None

        cursor.execute("""
            UPDATE machines 
            SET status = 'FINISHED_UNCOLLECTED',
                current_amp = 0.0,
                vibration_level = 0.0,
                updated_at = ?
            WHERE id = ?
        """, (now_str, machine_id))
        conn.commit()
        conn.close()

        clean_name = machine["name"]
        operator = machine["current_user_name"] or "同學"
        target_line_id = (
            machine["current_line_user_id"] 
            if "current_line_user_id" in machine.keys() and machine["current_line_user_id"]
            else DEFAULT_TEST_USER_ID
        )

        event = cls.log_event(
            machine_id=machine_id,
            event_type="WASH_COMPLETED",
            source="Lambda_RuleEngine",
            message=f"【洗程結束】{clean_name} 洗滌完成！已發送 LINE 取衣推播至 [{target_line_id}]。"
        )

        # 規格要求精確推播文案：
        # 「🧺【DormWash 宿舍智慧洗衣房】\n同學你好！你的【{機台名稱}】洗程已結束囉！✨\n⏰ 請於 10 分鐘內前往取出衣物，避免影響其他排隊同學喔～」
        line_msg = (
            f"🧺【DormWash 宿舍智慧洗衣房】\n"
            f"同學你好！你的【{clean_name}】洗程已結束囉！✨\n"
            f"⏰ 請於 10 分鐘內前往取出衣物，避免影響其他排隊同學喔～"
        )
        send_line_push_message(target_line_id, line_msg)

        cls._dispatch_notification(
            user_target=target_line_id,
            title="🧺 洗滌完成通知",
            body=f"您的衣物已在「{clean_name}」洗滌完畢！請於 10 分鐘內前往取出，避免被別人移走喔！"
        )

        return event

    @classmethod
    def start_wash_cycle(cls, machine_id: str, duration_seconds: int = 35 * 60, user_name: Optional[str] = None, line_user_id: Optional[str] = None) -> Dict[str, Any]:
        """啟動洗程 (供手動或學生啟動，支援綁定 LINE 身分)"""
        now = datetime.now()
        end_time = now + timedelta(seconds=duration_seconds)
        now_str = now.isoformat()
        end_time_str = end_time.isoformat()
        operator = user_name.strip() if user_name and user_name.strip() else "同學"
        line_id = line_user_id.strip() if line_user_id and line_user_id.strip() else DEFAULT_TEST_USER_ID

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM machines WHERE id = ?", (machine_id,))
        machine = cursor.fetchone()

        if not machine:
            conn.close()
            raise ValueError(f"Machine {machine_id} not found")

        cursor.execute("""
            UPDATE machines 
            SET status = 'IN_USE',
                current_amp = 8.8,
                vibration_level = 2.4,
                cycle_start_time = ?,
                cycle_end_time = ?,
                current_user_name = ?,
                current_line_user_id = ?,
                reserved_user_name = NULL,
                reserved_contact = NULL,
                reserved_line_user_id = NULL,
                reserved_expire_time = NULL,
                updated_at = ?
            WHERE id = ?
        """, (now_str, end_time_str, operator, line_id, now_str, machine_id))
        conn.commit()
        conn.close()

        clean_name = machine["name"]
        event = cls.log_event(
            machine_id=machine_id,
            event_type="CYCLE_STARTED",
            source="Student_App",
            message=f"{clean_name} 由 [{operator}] 啟動運轉，預計洗程: {duration_seconds} 秒 (綁定 LINE: {line_id})"
        )
        return event

    @classmethod
    def force_finish_cycle(cls, machine_id: str) -> Dict[str, Any]:
        """強制完成洗程 (模擬洗程結束瞬間)"""
        event = cls.finish_wash_cycle(machine_id, reason="FORCE_FINISH")
        if not event:
            return {"status": "machine not running"}
        return event

    @classmethod
    def collect_clothes(cls, machine_id: str) -> Dict[str, Any]:
        """
        同學取走衣服：
        1. 檢查是否有排隊清單
        2. 若有：自動流轉保留給排隊第一位（保留10分鐘緩衝期），並發送 LINE 輪替提醒推播
        3. 若無：機台回歸 AVAILABLE
        """
        now = datetime.now()
        now_str = now.isoformat()

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM machines WHERE id = ?", (machine_id,))
        machine = cursor.fetchone()
        clean_name = machine["name"] if machine else machine_id

        # 檢查排隊清單
        cursor.execute("""
            SELECT * FROM reservations 
            WHERE machine_id = ? AND status = 'WAITING' 
            ORDER BY created_at ASC LIMIT 1
        """, (machine_id,))
        next_reservation = cursor.fetchone()

        if next_reservation:
            # 輪到第一位排隊者，保留 10 分鐘 (600秒)
            expire_time = now + timedelta(minutes=10)
            expire_str = expire_time.isoformat()
            
            # 取得該排隊者的 LINE User ID
            candidate_line_id = (
                next_reservation["line_user_id"]
                if "line_user_id" in next_reservation.keys() and next_reservation["line_user_id"]
                else next_reservation["user_contact"] or DEFAULT_TEST_USER_ID
            )

            # 更新機台狀態為 RESERVED，並記錄保留給誰
            cursor.execute("""
                UPDATE machines 
                SET status = 'RESERVED',
                    current_amp = 0.0,
                    vibration_level = 0.0,
                    current_user_name = NULL,
                    current_line_user_id = NULL,
                    reserved_user_name = ?,
                    reserved_contact = ?,
                    reserved_line_user_id = ?,
                    reserved_expire_time = ?,
                    updated_at = ?
                WHERE id = ?
            """, (next_reservation["user_name"], next_reservation["user_contact"], candidate_line_id, expire_str, now_str, machine_id))

            # 更新預約紀錄狀態
            cursor.execute("""
                UPDATE reservations 
                SET status = 'NOTIFIED', notified_at = ?
                WHERE id = ?
            """, (now_str, next_reservation["id"]))
            conn.commit()
            conn.close()

            event = cls.log_event(
                machine_id=machine_id,
                event_type="QUEUE_HANDOVER",
                source="Lambda_QueueCoordinator",
                message=f"【排隊流轉】衣物已取出！系統已自動將 {clean_name} 保留給候補同學 [{next_reservation['user_name']}]，緩衝保留 10 分鐘 (LINE: {candidate_line_id})。"
            )

            # 核心場景 2：規格要求精確推播文案
            # 「🔔【DormWash 輪替提醒】\n同學你好！你排隊的【{機台名稱}】已經釋出，已為您保留 10 分鐘！請盡速前往洗衣，逾時將自動讓給下一位同學～」
            line_msg = (
                f"🔔【DormWash 輪替提醒】\n"
                f"同學你好！你排隊的【{clean_name}】已經釋出，已為您保留 10 分鐘！請盡速前往洗衣，逾時將自動讓給下一位同學～"
            )
            send_line_push_message(candidate_line_id, line_msg)

            cls._dispatch_notification(
                user_target=candidate_line_id,
                title="🔔 輪到您洗衣服了！",
                body=f"同學 {next_reservation['user_name']} 您好，您預約的 {clean_name} 現在已有空位！系統已為您保留 10 分鐘，請盡速前往洗衣室。"
            )
            return event
        else:
            # 沒有人排隊，回歸閒置狀態
            cursor.execute("""
                UPDATE machines 
                SET status = 'AVAILABLE',
                    current_amp = 0.0,
                    vibration_level = 0.0,
                    current_user_name = NULL,
                    current_line_user_id = NULL,
                    reserved_user_name = NULL,
                    reserved_contact = NULL,
                    reserved_line_user_id = NULL,
                    reserved_expire_time = NULL,
                    cycle_start_time = NULL,
                    cycle_end_time = NULL,
                    updated_at = ?
                WHERE id = ?
            """, (now_str, machine_id))
            conn.commit()
            conn.close()

            event = cls.log_event(
                machine_id=machine_id,
                event_type="CLOTHES_COLLECTED",
                source="IoT_DoorSensor",
                message=f"{clean_name} 衣物已取走，目前無人候補排隊，機台重回「閒置中 (AVAILABLE)」。"
            )
            return event

    @classmethod
    def add_reservation(cls, machine_id: str, user_name: str, user_contact: str, line_user_id: Optional[str] = None) -> Dict[str, Any]:
        """使用者預約排隊 (綁定 LINE User ID)"""
        now = datetime.now().isoformat()
        conn = get_db()
        cursor = conn.cursor()
        line_id = line_user_id.strip() if line_user_id and line_user_id.strip() else DEFAULT_TEST_USER_ID

        cursor.execute("""
            INSERT INTO reservations (machine_id, user_name, user_contact, line_user_id, status, created_at)
            VALUES (?, ?, ?, ?, 'WAITING', ?)
        """, (machine_id, user_name, user_contact, line_id, now))
        reservation_id = cursor.lastrowid

        # 計算目前排在第幾順位
        cursor.execute("""
            SELECT COUNT(*) FROM reservations 
            WHERE machine_id = ? AND status = 'WAITING'
        """, (machine_id,))
        queue_position = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        event = cls.log_event(
            machine_id=machine_id,
            event_type="RESERVATION_QUEUED",
            source="User_Portal",
            message=f"同學 [{user_name}] 登記預約排隊 {machine_id} (目前候補順位第 {queue_position} 位，綁定 LINE: {line_id})"
        )
        return {
            "reservation_id": reservation_id,
            "machine_id": machine_id,
            "user_name": user_name,
            "queue_position": queue_position,
            "line_user_id": line_id,
            "event": event
        }

    @classmethod
    def extend_dryer_cycle(cls, machine_id: str, extra_minutes: int = 10) -> Dict[str, Any]:
        """烘衣機中途追加時間"""
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM machines WHERE id = ?", (machine_id,))
        machine = cursor.fetchone()
        if not machine or machine["status"] != "IN_USE":
            conn.close()
            raise ValueError(f"機台 {machine_id} 非運轉中，無法追加烘衣時間")

        now = datetime.now()
        current_end_str = machine["cycle_end_time"]
        if current_end_str:
            base_time = max(now, datetime.fromisoformat(current_end_str))
        else:
            base_time = now

        new_end_time = base_time + timedelta(minutes=extra_minutes)
        now_str = now.isoformat()
        new_end_str = new_end_time.isoformat()

        cursor.execute("""
            UPDATE machines 
            SET cycle_end_time = ?, updated_at = ?
            WHERE id = ?
        """, (new_end_str, now_str, machine_id))
        conn.commit()
        conn.close()

        event = cls.log_event(
            machine_id=machine_id,
            event_type="CYCLE_EXTENDED",
            source="Student_App",
            message=f"{machine['name']} 已成功延長烘衣 {extra_minutes} 分鐘！"
        )
        return {
            "machine_id": machine_id,
            "new_end_time": new_end_str,
            "extra_minutes": extra_minutes,
            "event": event
        }

    @classmethod
    def report_machine_issue(cls, machine_id: str, issue_type: str, note: str, reporter: str) -> Dict[str, Any]:
        """機台報修 / 異常反饋"""
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM machines WHERE id = ?", (machine_id,))
        machine = cursor.fetchone()
        if not machine:
            conn.close()
            raise ValueError(f"機台 {machine_id} 不存在")

        current_reports = (machine["fault_reports_count"] or 0) + 1
        fault_reason = f"{issue_type}（{note}）" if note else issue_type
        now_str = datetime.now().isoformat()

        # 超過 1 次報修 (即累計 2 次以上) 鎖定為 MAINTENANCE
        new_status = machine["status"]
        is_locked = False
        if current_reports >= 2 and machine["status"] != "MAINTENANCE":
            new_status = "MAINTENANCE"
            is_locked = True

        cursor.execute("""
            UPDATE machines 
            SET fault_reports_count = ?, fault_reason = ?, status = ?, updated_at = ?
            WHERE id = ?
        """, (current_reports, fault_reason, new_status, now_str, machine_id))
        conn.commit()
        conn.close()

        msg = f"同學 [{reporter}] 回報 {machine['name']} 異常：{fault_reason}。"
        if is_locked:
            msg += " ⚠️ 因累計多次報修，系統已自動鎖定為「維修保養中」，並通報宿舍管理處！"

        event = cls.log_event(
            machine_id=machine_id,
            event_type="MACHINE_FAULT_REPORTED",
            source="Dorm_Maintenance",
            message=msg
        )
        return {
            "machine_id": machine_id,
            "fault_reports_count": current_reports,
            "fault_reason": fault_reason,
            "new_status": new_status,
            "event": event
        }

    @classmethod
    def send_anonymous_poke(cls, machine_id: str, action_type: str, sender: str) -> Dict[str, Any]:
        """匿名提醒 / 申請移至乾淨暫存籃"""
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM machines WHERE id = ?", (machine_id,))
        machine = cursor.fetchone()
        conn.close()

        target_user = machine["current_user_name"] or "機台主人"
        if action_type == "basket":
            content = f"已有同學在現場等待，您的衣物已洗好超過 10 分鐘，若不克前往，同學將協助將衣物放進乾淨暫存籃喔！"
            title = "🧺 申請移至暫存籃提醒"
        else:
            content = f"已有同學在洗衣房等候，您的衣物已洗滌完畢，請盡速前往取出以免影響他人！"
            title = "🔔 匿名溫馨提醒"

        event = cls.log_event(
            machine_id=machine_id,
            event_type="ANONYMOUS_POKE",
            source="Friendly_Reminder",
            message=f"【{title}】向 {machine['name']} 主人 [{target_user}] 發送通知：{content}"
        )

        cls._dispatch_notification(
            user_target=target_user,
            title=title,
            body=content
        )

        return {
            "machine_id": machine_id,
            "target_user": target_user,
            "title": title,
            "content": content,
            "event": event
        }

    @classmethod
    def reset_all_machines(cls) -> Dict[str, Any]:
        """重置所有機台為初始狀態 (展示用)"""
        now = datetime.now().isoformat()
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE machines 
            SET status = 'AVAILABLE',
                current_amp = 0.0,
                vibration_level = 0.0,
                current_user_name = NULL,
                cycle_start_time = NULL,
                cycle_end_time = NULL,
                reserved_user_name = NULL,
                reserved_contact = NULL,
                reserved_expire_time = NULL,
                updated_at = ?
        """, (now,))
        cursor.execute("UPDATE reservations SET status = 'CANCELLED' WHERE status = 'WAITING'")
        conn.commit()
        conn.close()

        event = cls.log_event(
            machine_id="ALL",
            event_type="SYSTEM_RESET",
            source="Demo_Control",
            message="所有洗衣機與烘衣機狀態已全部重置為 AVAILABLE 閒置狀態。"
        )
        return event

    @staticmethod
    def _dispatch_notification(user_target: str, title: str, body: str):
        """
        模擬雲端推播發送 (AWS SNS / Firebase FCM / LINE Notify Webhook)
        在日誌中記錄，並同時能輸出至主控台與 WebSocket
        """
        print(f"\n[🚀 雲端推播模擬 (Cloud Notification Dispatch)]")
        print(f"  Target: {user_target}")
        print(f"  Title : {title}")
        print(f"  Body  : {body}\n")
