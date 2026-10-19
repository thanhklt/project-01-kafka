# Kết quả xác minh sample — 2026-10-19

## Kết quả

- Offline suite: 31 tests PASS, 1 integration test được skip mặc định vì opt-in.
- Integration chạy riêng: PASS trên hai Kafka cluster local, tổng 39 source records. Đã xác minh 3 consumer cùng group, theo dõi record phát sinh mới, restart sau một transaction payload/checkpoint chưa commit và mỗi origin đúng một lần.
- Nguồn thật → Docker canary: hai lần chạy `--canary --max-records 50`, mỗi lần commit đúng 50 record rồi dừng.
- Verifier: `canary_records=100`, `unique_origins=100`, `source_records_compared=100`, `payload_headers_timestamp=match`, `status=verified`. Bao gồm key, value, headers, timestamp, mapping partition và thứ tự offset trong từng partition.

| Partition nguồn | Offset bắt đầu sample | Checkpoint cuối (next offset) | Records đã chuyển |
|---|---:|---:|---:|
| 0 | 32970612 | 32970625 | 13 |
| 1 | 32971818 | 32971869 | 51 |
| 2 | 32971149 | 32971185 | 36 |
| Tổng | | | 100 |

Số record ở bảng được xác minh bằng các origin đã đọc, không suy ra chung từ watermark; sample này không có offset gaps trong khoảng tương ứng.

## Trạng thái bàn giao

- `config/mirror.yml` là file cấu hình local đang dùng, bị Git ignore; `config/config.yml` đã xóa sau migration thành công.
- Topic canary: `product_view_mirror_canary`; checkpoint: `_mirror_product_view_canary_state_v1`. Dữ liệu/state được giữ để có thể tiếp tục sample.
- Topic mirror chính `product_view` chưa được ghi hoặc khởi tạo bởi phiên kiểm thử này. Không có tiến trình mirror chạy liên tục sau sample.
- Dừng riêng Kafka fixture `tests/integration/compose.source.yml` sau kiểm thử. Giữ các broker đích chính và dữ liệu Docker hiện có.
- Hai deployment thật trùng cluster ID mẫu; đã xác minh namespace/broker membership khác nhau, ghim source topic ID và yêu cầu bằng chứng local riêng trong preflight. Không sửa cluster IDs hoặc xóa volume.
- Không commit/push, không tạo worktree, không xóa hoặc khôi phục các thay đổi có sẵn của người dùng.

Đây là bằng chứng cho sample đã chạy, không phải chứng nhận mọi lỗi mạng, retention hoặc thay đổi partition đều đã được kiểm thử. Xem [runbook](kafka-mirror-runbook.md) để chạy đầy đủ khi cần.
