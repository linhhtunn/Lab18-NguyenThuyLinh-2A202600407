Trong các anti-pattern ở slide §5, lỗi mà team chúng tôi dễ vướng phải nhất là **"Small-File Problem" (Vấn đề vô số file nhỏ)**.

**Lý do dễ mắc phải:**

Thứ nhất, team thường có xu hướng ưu tiên tốc độ nạp dữ liệu (ingestion) để có tính real-time (ví dụ: streaming log từ ứng dụng hệ thống hoặc call API về Data Lake liên tục mỗi vài giây/phút). Việc liên tục append các batch dữ liệu rất nhỏ này một cách tự nhiên sẽ sinh ra hàng ngàn file Parquet li ti trên ổ cứng.

Thứ hai, trong giai đoạn đầu xây dựng pipeline, mọi người thường chỉ tập trung làm sao để luồng Bronze nhận được data thành công mà hay "quên" thiết lập các quy trình bảo trì định kỳ (routine maintenance) như chạy lệnh `OPTIMIZE` hay `Z-ORDER`.

Hậu quả là khi dữ liệu phình to, hệ thống sẽ tốn quá nhiều thời gian chỉ để load metadata và mở các file nhỏ lẻ (I/O bottleneck), khiến cho tốc độ truy vấn ở lớp Silver và Gold bị chậm đi nghiêm trọng. Bài Lab số 2 đã minh chứng rõ việc không gộp file sẽ làm giảm hiệu suất truy vấn, do đó việc lập lịch chạy `OPTIMIZE` định kỳ là tiêu chuẩn bắt buộc khi vận hành Data Lakehouse.
