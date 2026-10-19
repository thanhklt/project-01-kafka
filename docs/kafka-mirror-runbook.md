# Kafka mirror: vận hành

Pipeline B đọc dữ liệu nguồn còn lưu từ earliest rồi tiếp tục theo dõi record mới. Một tiến trình Python chạy 3 consumer threads trong cùng group; mỗi partition có producer giao dịch và checkpoint riêng. Consumer đích phải dùng `isolation.level=read_committed`.

## Cấu hình

Chỉ dùng `config/mirror.yml`, chứa `source`, `destination`, `mirror`. File này bị Git ignore. Cấu hình cũ `config/config.yml` đã được migrate, xác minh tương đương rồi xóa. `count_messages.py` và `consume_sample.py` mặc định đọc phần `source` của file mới; không cần cấu hình nguồn thứ hai.

Đích local dùng cổng 9094, 9194, 9294 và SASL khớp JAAS. Nguồn cần quyền đọc topic và tham gia consumer group; pipeline không commit group offset nguồn. Đích cần quyền tạo/describe topic, đọc/ghi state và sử dụng transactional IDs.

Hai deployment hiện dùng chung cluster ID mẫu. File local ghim `mirror.expected_source_topic_id` và bật `allow_shared_cluster_id` sau khi xác minh nguồn remote có broker 0, đích loopback có brokers 0/1/2 và namespace topic khác nhau. Khi ID trùng, preflight yêu cầu tất cả bằng chứng này; trỏ hai endpoint về cùng topic/cluster vẫn bị từ chối. Không đổi cluster ID hay xóa volume để giải quyết trường hợp này. Với deployment có cluster ID riêng, giữ `allow_shared_cluster_id=false` như mặc định.

## Chạy

```powershell
# Chỉ kiểm tra metadata/state, không tạo topic hoặc join group/fence producer.
poetry run python mirror_kafka.py --check

# Sample hữu hạn vào topic riêng. Mỗi lần chạy giới hạn tổng commit của cả 3 consumer.
poetry run python mirror_kafka.py --canary --max-records 50

# Tiếp tục canary từ checkpoint đã commit, không chạy lại 50 record trước.
poetry run python mirror_kafka.py --canary --max-records 50

# Đối soát đúng 100 record canary với các source offsets tương ứng.
poetry run python verify_mirror_sample.py --expected-records 100

# Chạy đầy đủ liên tục khi cần. Lệnh này sẽ đọc toàn bộ lịch sử còn lưu.
poetry run python mirror_kafka.py
```

Theo yêu cầu kiểm thử bằng sample, phiên triển khai chỉ chạy canary 100 record và fixture local 39 record. Chưa khởi chạy lệnh đầy đủ liên tục.

Canary dùng topic `<destination.topic>_mirror_canary`, checkpoint riêng và group/transactional IDs riêng. Topic chính chưa được ghi chỉ vì đã chạy canary. `--max-records` giới hạn số record commit trong **lần chạy**, không phải snapshot hoặc mốc tổng của toàn luồng; lần chạy sau tiếp tục các record kế tiếp.

## Checkpoint và recovery

- Mặc định mỗi transaction gồm một message và checkpoint `next_offset=source_offset+1` trên cluster đích. ACK của produce riêng lẻ chưa chứng minh dữ liệu đã commit.
- Trước restore lúc startup, supervisor giải quyết producer epochs của mọi source partition để transaction cũ không chặn state reader. Mỗi assignment lại fencing partition producer trước khi đọc checkpoint và đặt offset.
- Khi consumer rời group, Kafka phân lại partition. Consumer mới dùng transactional ID ổn định theo partition; producer của partition khác không bị ảnh hưởng.
- Commit timeout retriable được retry tại bước commit. Nếu kết quả vẫn chưa rõ, worker dừng và giữ reservation; chạy lại cùng cấu hình để fence/restore, không tự replay mù.
- Ctrl+C dừng nhận dữ liệu và đóng group members. Chạy lại cùng config để tiếp tục. Không xóa/reset checkpoint hoặc đổi tên state topic/group/transactional IDs để xử lý lỗi.
- Lỗi mất/hỏng state, topic ID đổi hoặc checkpoint ngoài vùng source còn lưu làm worker dừng. Không tự nhảy earliest/latest; cần điều tra nguyên nhân và phạm vi dữ liệu mất trước khi tạo luồng mới.
- Không chạy hai tiến trình cùng luồng để tăng tốc. Control producer của tiến trình mới sẽ fence tiến trình cũ; heartbeat control phát hiện fencing trong chu kỳ metadata (mặc định 30 giây).

## Theo dõi và giới hạn

Log chỉ in member/partition assignments và progress: số record đã commit trong lần chạy, bytes, throughput, checkpoint, khoảng cách offset quan sát và catching_up/following. Khoảng cách offset là ước lượng; không phải số message chính xác.

Topic dữ liệu đích giữ `cleanup.policy=delete`, nhưng `retention.ms=-1` và `retention.bytes=-1` để lưu toàn bộ record đã chuyển. Cần theo dõi dung lượng Docker disk; nguồn vẫn áp dụng retention riêng. Topic checkpoint được compact và không có TTL. Không có writer khác ghi vào topic dữ liệu/state thuộc luồng.

Ghi transaction cho từng message có chi phí round trip/commit cao. `batch_max_records` có thể tăng để xử lý batch sau khi đo tốc độ; một transaction vẫn chỉ chứa một partition, checkpoint chỉ tiến sau khi cả batch commit. Giữ `batch_max_bytes` để giới hạn kích thước batch; record lớn hơn giới hạn được thử riêng và không bị bỏ qua nếu broker từ chối.

Dedupe chỉ theo origin Kafka, không theo nội dung. Record khác offset vẫn được giữ khi payload giống nhau. Giữ binary/null/headers lặp/timestamp nguồn; thêm header `__mirror_origin_v1`. Offset đích khác offset nguồn. Kafka có thể giữ aborted records trong log vật lý, vì vậy phải đối soát bằng consumer `read_committed`, không lấy chênh lệch watermark làm số message chính xác. Ứng dụng downstream tự đọc lại record vẫn cần cơ chế xử lý idempotent của nó.

## Kiểm thử sample tái lập

```powershell
poetry run python -m unittest discover -s tests -v
docker compose -f tests/integration/compose.source.yml up -d --wait
$env:MIRROR_INTEGRATION = '1'
poetry run python -m unittest tests.integration.test_mirror_e2e -v
Remove-Item Env:MIRROR_INTEGRATION
# Dừng riêng Kafka fixture sau kiểm thử; không chạm các broker chính.
docker compose -f tests/integration/compose.source.yml stop
```

Fixture nguồn chỉ dùng `127.0.0.1:19094`, cluster ID riêng, topic prefix `mirror-it-`. Sample gồm 30 record đầu, 6 record phát sinh khi đang theo dõi và 3 record sau restart; kiểm tra binary/null/duplicate payload/header/order, mỗi origin đúng một bản và transaction cũ bị abort khi recovery. Đây không phải kiểm thử toàn bộ dữ liệu production hoặc mọi kiểu sự cố mạng.
