import os
import sys
import time
import subprocess

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from confluent_kafka import Consumer, TopicPartition, KafkaException
from config.settings import load_settings

def find_target_pids():
    try:
        # Use powershell to find python processes running remote_to_local_kafka.py
        cmd = 'Get-CimInstance Win32_Process -Filter "Name like \'%python%\'" | Where-Object { $_.CommandLine -like "*remote_to_local_kafka.py*" } | Select-Object -ExpandProperty ProcessId'
        res = subprocess.run(["powershell", "-NoProfile", "-Command", cmd], capture_output=True, text=True, check=True)
        my_pid = os.getpid()
        pids = []
        for line in res.stdout.strip().splitlines():
            line = line.strip()
            if line.isdigit():
                pid = int(line)
                if pid != my_pid:
                    pids.append(pid)
        return pids
    except Exception as e:
        return []

def kill_targets(pids):
    for pid in pids:
        try:
            print(f"[ACTION] Dừng tiến trình PID {pid}...")
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
        except Exception:
            pass

def main():
    config = load_settings("config/config.yml")
    src_conf = config.source.to_client_config()
    src_conf['group.id'] = 'remote_to_local_kafka'
    src_conf['enable.auto.commit'] = False
    
    topic = "product_view"
    
    print("=" * 60, flush=True)
    print("BẮT ĐẦU THEO DÕI TIẾN TRÌNH REMOTE -> LOCAL KAFKA", flush=True)
    print("=" * 60, flush=True)

    try:
        consumer = Consumer(src_conf)
    except Exception as e:
        print(f"[LỖI KHỞI TẠO KAFKA]: {e}", flush=True)
        targets = find_target_pids()
        kill_targets(targets)
        sys.exit(1)

    last_time = time.time()
    last_committed = None
    stall_count = 0

    while True:
        now = time.time()
        targets = find_target_pids()
        
        # Check if process died
        if not targets:
            print("[CẢNH BÁO/LỖI]: Không tìm thấy tiến trình remote_to_local_kafka.py đang chạy!")
            break

        try:
            tps = [TopicPartition(topic, p) for p in [0, 1, 2]]
            committed_list = consumer.committed(tps, timeout=10)
            
            total_lag = 0
            total_high = 0
            total_committed = 0
            partition_stats = []

            for tp in committed_list:
                low, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)
                committed_val = tp.offset if tp.offset >= 0 else low
                lag = max(0, high - committed_val)
                total_lag += lag
                total_high += (high - low)
                total_committed += max(0, committed_val - low)
                partition_stats.append((tp.partition, committed_val, high, lag))

            # Calculate speed
            dt = now - last_time
            rate_str = "Đang tính..."
            eta_str = "Đang tính..."
            if last_committed is not None and dt > 0:
                delta = total_committed - last_committed
                rate = delta / dt
                if rate > 0:
                    rate_str = f"{rate:,.0f} msgs/s"
                    eta_sec = total_lag / rate
                    eta_str = f"{eta_sec:.1f}s ({eta_sec/60:.1f} phút)"
                else:
                    rate_str = "0 msgs/s"
                    eta_str = "N/A"
                    if total_lag > 100:
                        stall_count += 1
                    else:
                        stall_count = 0
            
            last_committed = total_committed
            last_time = now

            pct = (total_committed / total_high * 100) if total_high > 0 else 0
            
            print(f"[{time.strftime('%H:%M:%S')}] Tiến độ: {total_committed:,}/{total_high:,} ({pct:.2f}%) | "
                  f"Còn lại: {total_lag:,} msgs | Tốc độ: {rate_str} | ETA: {eta_str}", flush=True)
            for p, c, h, l in partition_stats:
                print(f"   - Part {p}: offset={c:,}/{h:,} (lag={l:,})", flush=True)

            # Check stall or error
            if stall_count >= 6:  # stalled for ~30 seconds while having lag
                print(f"[LỖI]: Tiến trình bị nghẽn (stall), không có message mới được xử lý trong 30s! Dừng phiên...", flush=True)
                kill_targets(targets)
                sys.exit(2)

            # Check completion
            if total_lag <= 50:
                print("\n[HOÀN THÀNH]: Tiến trình đã đồng bộ hết toàn bộ dữ liệu từ remote về local kafka!", flush=True)
                break

        except KafkaException as ke:
            print(f"\n[LỖI KAFKA]: {ke}", flush=True)
            print("[ACTION]: Dừng tiến trình remote_to_local_kafka...", flush=True)
            kill_targets(targets)
            sys.exit(1)
        except Exception as e:
            print(f"\n[LỖI HỆ THỐNG]: {e}", flush=True)
            print("[ACTION]: Dừng tiến trình remote_to_local_kafka...", flush=True)
            kill_targets(targets)
            sys.exit(1)

        time.sleep(5)

    consumer.close()

if __name__ == "__main__":
    main()
