# Class7-Customer360(LogSearch)
## Mô tả bài toán:

Phân tích tập dữ liệu log search của tháng 6 và tháng 7 giúp business có
góc nhìn toàn diện về user story

Log search là tập dữ liệu log được thu thập từ chính việc user sử dụng
dịch vụ của business, dataset này khác với log content ở chỗ đó là dữ
liệu này là user chủ động sinh ra cụ thể từ hoạt động tìm kiếm

### Những insights cần extract được từ tập dữ liệu:

**+) Keyword được tìm kiếm nhiều nhất của từng tháng

+) Keyword đó thuộc phân loại nào

+) Phân loại nào đang nắm giữ lượt tìm kiếm nhiều nhất

+) Liệu phân loại của các keyword có thay đổi hay không và thay đổi như
thế nào**

**Hình ảnh về nguồn dữ liệu ban đầu:**
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/firstdataset.png)

**Các bước thực hiện:**

Phase 1:
+) Lọc ra các category == quit Người dùng nhập vào ô tìm kiếm nhưng
không thực hiện tìm kiếm <br>
+) Bỏ cột category và actions <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/step1_output.jpg) <br> <br>
+) Thêm cột date và month, lọc ra các giá trị month không đúng(outlier) <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/step34_output.png) <br><br>

+) Normalize search keyword(Xử lý đơn giản các keyword mà ng dùng tìm
kiếm, chuyển hết thành lowercase hoặc uppercase) <br>
NOTE: Chưa xử lý toàn vẹn được những trường hợp điển hình như: liên minh
công lý -- lien minh cong ly <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/step5_output.png)
<br><br>
+) Đếm số lượt tìm kiếm group by ngày tháng và keyword, sắp xếp số lượng
từ lớn đến bé
<br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/step6_output.png) <br><br>

+) Cộng lượt tìm kiếm theo ngày và rename lại cột để phân biệt giữa 2
datasets giúp dễ dàng khi join ở phase 2 <br>
Output:<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/t6.png)
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/t7.png)<br><br>

Phase 2: <br>
+) Thêm cột đánh số index cho cả 2 dataset để sử dụng join <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/indexed_t6_t7.png) <br><br>

+) Join 2 datasets lại -\> ta có được 1 tập dữ liệu theo chiều ngang,
drop cột index<br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/combined.png)<br><br>

Phase 3: <br>
+) Thêm cột trending type, cột này sẽ theo dõi sự thay đổi của nhóm
keyword searched theo từng tháng <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/trending_type.png) <br><br>

+) Thêm cột changes, cột này sẽ theo dõi cụ thể nhóm keyword searched sẽ
thay đổi từ nhóm nào sang nhóm nào <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/changes.png) <br><br>

Kết quả cuối cùng: <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/final.png) <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/final_log.png) <br><br>

Kết quả sau khi save vào database: <br>
Output: <br>
![Logo dự án](https://github.com/hkhanhdev/Customer360-LogSearch/blob/main/presentations/database.png) <br>


