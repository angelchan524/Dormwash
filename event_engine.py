import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "laundry.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
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
        # 規則 A: 機台在運轉中 (IN_USE)，必須當預計結束時間抵達 (now >= cycle_end_time) 才轉為 FINISHED_UNCOLLECTED
        # 避免剛啟動時因模擬器剛好在待機低電流造成誤判立即完成
        if current_status == "IN_USE":
            is_time_up = (cycle_end_time and now >= cycle_end_time)

            if is_time_up:
                # 觸發「洗滌完成」事件
                cursor.execute("""
                    UPDATE machines 
                    SET status = 'FINISHED_UNCOLLECTED',
                        current_amp = 0.0,
                        vibration_level = 0.0,
                        updated_at = ?
                    WHERE id = ?
                """, (now_str, machine_id))
                conn.commit()

                event = cls.log_event(
                    machine_id=machine_id,
                    event_type="WASH_COMPLETED",
                    source="Lambda_RuleEngine",
                    message=f"【洗程結束】{machine['name']} 洗滌完成！請房號 [{machine['current_user_name'] or '同學'}] 於 10 分鐘內前往取出。"
                )
                events_triggered.append(event)

                # 模擬第三方通知發送 (例如 LINE Notify / Webhook / SMS)
                cls._dispatch_notification(
                    user_target=machine["current_user_name"] or "當前使用者",
                    title="🧺 洗滌完成通知",
                    body=f"您的衣物已在「{machine['name']}」洗滌完畢！請於 10 分鐘內前往取出，避免被別人移走喔！"
                )

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
                            reserved_user_name = NULL,
                            reserved_contact = NULL,
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
    def start_wash_cycle(cls, machine_id: str, duration_seconds: int = 30, user_name: Optional[str] = None) -> Dict[str, Any]:
        """啟動洗程 (供手動或學生啟動)"""
        now = datetime.now()
        end_time = now + timedelta(seconds=duration_seconds)
        now_str = now.isoformat()
        end_time_str = end_time.isoformat()
        operator = user_name.strip() if user_name and user_name.strip() else "同學"

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
                reserved_user_name = NULL,
                reserved_contact = NULL,
                reserved_expire_time = NULL,
                updated_at = ?
            WHERE id = ?
        """, (now_str, end_time_str, operator, now_str, machine_id))
        conn.commit()
        conn.close()

        event = cls.log_event(
            machine_id=machine_id,
            event_type="CYCLE_STARTED",
            source="Student_App",
            message=f"{machine['name']} 由 [{operator}] 啟動運轉，預計洗程: {duration_seconds} 秒"
        )
        return event

    @classmethod
    def force_finish_cycle(cls, machine_id: str) -> Dict[str, Any]:
        """強制完成洗程 (模擬洗程結束瞬間)"""
        now = datetime.now().isoformat()
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE machines 
            SET status = 'FINISHED_UNCOLLECTED',
                current_amp = 0.0,
                vibration_level = 0.0,
                updated_at = ?
            WHERE id = ?
        """, (now, machine_id))
        conn.commit()
        conn.close()

        event = cls.log_event(
            machine_id=machine_id,
            event_type="WASH_COMPLETED",
            source="Demo_Control",
            message=f"【手動展示觸發】{machine_id} 洗滌完成，進入「待取出 (FINISHED_UNCOLLECTED)」狀態。"
        )
        cls._dispatch_notification(
            user_target="當前洗滌同學",
            title="🧺 洗程已完成",
            body=f"{machine_id} 已洗完，請立即前往洗衣間取出衣物！"
        )
        return event

    @classmethod
    def collect_clothes(cls, machine_id: str) -> Dict[str, Any]:
        """
        同學取走衣服：
        1. 檢查是否有排隊清單
        2. 若有：自動流轉保留給排隊第一位（保留10分鐘緩衝期）
        3. 若無：機台回歸 AVAILABLE
        """
        now = datetime.now()
        now_str = now.isoformat()

        conn = get_db()
        cursor = conn.cursor()

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

            # 更新機台狀態為 RESERVED
            cursor.execute("""
                UPDATE machines 
                SET status = 'RESERVED',
                    current_amp = 0.0,
                    vibration_level = 0.0,
                    reserved_user_name = ?,
                    reserved_contact = ?,
                    reserved_expire_time = ?,
                    updated_at = ?
                WHERE id = ?
            """, (next_reservation["user_name"], next_reservation["user_contact"], expire_str, now_str, machine_id))

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
                message=f"【排隊流轉】衣物已取出！系統已自動將 {machine_id} 保留給候補同學 [{next_reservation['user_name']}]，緩衝保留 10 分鐘。"
            )

            cls._dispatch_notification(
                user_target=next_reservation["user_contact"],
                title="🔔 輪到您洗衣服了！",
                body=f"同學 {next_reservation['user_name']} 您好，您預約的 {machine_id} 現在已有空位！系統已為您保留 10 分鐘，請盡速前往洗衣室。"
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
                    reserved_user_name = NULL,
                    reserved_contact = NULL,
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
                message=f"{machine_id} 衣物已取走，目前無人候補排隊，機台重回「閒置中 (AVAILABLE)」。"
            )
            return event

    @classmethod
    def add_reservation(cls, machine_id: str, user_name: str, user_contact: str) -> Dict[str, Any]:
        """使用者預約排隊"""
        now = datetime.now().isoformat()
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO reservations (machine_id, user_name, user_contact, status, created_at)
            VALUES (?, ?, ?, 'WAITING', ?)
        """, (machine_id, user_name, user_contact, now))
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
            message=f"同學 [{user_name}] 登記預約排隊 {machine_id} (目前候補順位第 {queue_position} 位)"
        )
        return {
            "reservation_id": reservation_id,
            "machine_id": machine_id,
            "user_name": user_name,
            "queue_position": queue_position,
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
