# Big Data Recommendation System Dashboard Design

## Mục tiêu trình diễn đồ án

1. Mô phỏng những ứng dụng mà hệ thống cung cấp 

2. Chứng minh khả năng xử lý dữ liệu lớn và phân tích dữ liệu realtime của hệ thống.

Dashboard phục vụ hai nhóm người dùng (Hai Dashboard):

- **Customer**
  - 1 Theo dõi hành vi người dùng.
  - 2 Hiển thị gợi ý sản phẩm realtime.
  - 3 Hiển thị sản phẩm đang hot.

- **Seller**
  - 1 Tìm kiếm khách hàng tiềm năng cho từng sản phẩm.
  - 2 Theo dõi xu hướng sản phẩm realtime (của 1 sản phẩm làm đó)
  - 3 Phân tích hành vi mua sắm của khách hàng.

Toàn bộ dữ liệu hiển thị được lấy từ Serving Layer thông qua các REST API.

---

# 1. Customer Dashboard

## Mục tiêu

Cho phép mô phỏng hành vi người dùng và quan sát khả năng gợi ý sản phẩm theo thời gian thực của hệ thống.

## Hiển thị

- Có khu vực chọn/nhập user_id, khi chọn/nhập user_id thì tất cả thông sau còn lại trong dashboard sẽ là thông tin xoay quanh user này

- Các khu vực bên dưới sẽ thực hiện chức năng của hệ thống
---

## 1.1. Khu vực 1: User Behavior

### Mục đích

Mô phỏng hành vi người dùng trên hệ thống.

Người dùng có thể thực hiện:

- View Product
- Add To Cart
- Purchase Product
  
Các hành vi này sẽ được gửi vào Kafka thông qua Serving Layer để hệ thống xử lý realtime.

--- 

### Hiển thị

- Hiển thị danh sách product (id, name, category) và nút view, add, purchase

## APIs

### Gửi hành vi người dùng
- Khi chọn product sau đó nhấn view, add, purchase và ấn xác nhận thì client sẽ gọi API 

```text
POST /api/events

Request

{
    "user_id": "U1",
    "product_id": "P15",
    "product_name": "abc",
    "category" : clothing,
    "event_type": "view" // event_type = view | cart | purchase
}

```

---
## 1.2. Khu vực 2: Realtime Recommendation

### Mục đích

- Hiển thị danh sách sản phẩm mà hệ thống đang gợi ý cho người dùng.
- Giao diện sẽ cập nhật sản phẩm được gợi ý cho khách hàng này sau mỗi thao tác view, purchase, cart và theo chu kỳ 5 phút (cập nhật sản phẩm hot trend)

Recommendation được xây dựng từ:

- Batch Recommendation
- Realtime Interest

---

### Hiển thị

Recommended For U1

- danh sách product (id, name, category)
1. P15 (id, name, category, score)
2. P8
3. P20
4. P3
5. P7

---

### APIs

GET /api/recommendations/{user_id}

Example

```http
GET /api/recommendations/U1
```

Response

```json
{
    "user_id": "U1",
    "segment_name": Frequent Shoppers | Risky Frequent Buyers | Low Frequency | Bad Customer,

    "recommendations": [
        {
            "product_id": "P15",
            "product_name": "Fami",
            "category": "vitamin",
            "score": 98.3,
            "recommendation_type": ABC
        },
        {
            "product_id": "P8",
            "product_name": "iphone 11 pro max",
            "category": "smartphone",
            "score": 95,
            "recommendation_type": ABC
        }
    ]
}
```

---

## 1.3. Khu vực 3: Trending Products
### Mục đích

Hiển thị các sản phẩm đang hot theo thời gian thực.

Nguồn dữ liệu: Trending Product Pipeline

Cấu hình xử lý:

- Window Size: 5 minutes
- Slide Duration: 1 minute

---

### Hiển thị

Top Trending Products (có sắp xếp )


| Rank | Product ID | Product Name | Category | Trend Score |
|------|------------|--------------|----------|-------------|
| 1 | P15 | Nimbus Stay | Toys | 92.0 |
| 2 | P8 | Orion Head | Beauty | 91.2 |
| 3 | P3 | Nimbus Whose | Clothing | 91.1 |
| 4 | P20 | Atlas Pro | Electronics | 81.2 |

---

## APIs

GET /api/trending

Response


```json
{
    "window_end": "10:05", // thời điểm thông tin trending product mới nhất
    "products": [
        {
            "product_id": "P15",
            "product_name": "Nimbus Stay",
            "category": "Toys",
            "trend_score": 250
        },
        {
            "product_id": "P8",
            "product_name": "Orion Head",
            "category": "Beauty",
            "trend_score": 220
        }
    ]
}
```

---

# 2. Seller Dashboard

## Mục đích

- Hỗ trợ nhà bán hàng phân tích dữ liệu kinh doanh và khai thác thông tin từ hệ thống recommendation. (Với dashboard này, không cần thông tin của đơn vị kinh doanh)
- Giao diện có phần để nhập/chọn product id (hiển thị id, name. category)
- Có nút xác nhận để tiến hành gọi API để lấy, hiển thị các kết quả bên dưới


---

## 2.1. Khu vực 1: Potential Customers

### Mục đích

Hiển thị danh sách khách hàng có khả năng cao sẽ mua một sản phẩm.

Nguồn dữ liệu: Batch Recommendation

Ý nghĩa: Hỗ trợ nhà bán hàng xác định khách hàng tiềm năng cho từng sản phẩm.

---


### Hiển thị

Selected Product_id: P15

Potential Customers (có sắp xếp )

| User | Interest Score | Type |
|--------|----------------|-------------|
| U1 | 98 | consumption |
| U3 | 95 | similar |
| U5 | 90 | complementary |
| U7 | 85 | similar |

---

### APIs

GET /api/products/{product_id}/potential-customers

```http
GET /api/products/P15/potential-customers
```

Response

```json
{
    "product_id": "P15",
    "customers": [
        {
            "user_id": "U1",
            "interest_score": 98.0,
            "type": "consumption"
        },
        {
            "user_id": "U3",
            "interest_score": 95.0,
            "type": "consumption"
        },
        {
            "user_id": "U5",
            "interest_score": 90.2,
            "type": "similar"
        }
    ]
}
```

---

## 2.2. Khu vực 2: Product Trend Analytics

### Mục đích

Theo dõi sự thay đổi xu hướng của sản phẩm theo thời gian.

Khác với Customer Dashboard:

- Customer Dashboard chỉ hiển thị sản phẩm đang hot.
- Seller Dashboard phân tích diễn biến trend score của một sản phẩm cụ thể

Giúp nhà bán hàng:

- Theo dõi hiệu quả marketing.
- Theo dõi sản phẩm đang tăng trưởng.
- Phát hiện sản phẩm sắp trở thành xu hướng.

---
### Hiển thị
- **Loại biểu đồ:** Line Chart (biểu đồ đường)
#### Trục dữ liệu:

- **X-Axis (Trục thời gian):**
  - Time Window (ví dụ: 10:00–10:01, 10:01–10:02,...)
  
- **Y-Axis (Trục giá trị):**
  - Trend Score (điểm xu hướng của sản phẩm)



#### Ví dụ sản phẩm: `P15` (mô tả này viết dạng bảng còn hiển thị là dạng đồ thị có chiều dài 10 phút theo trục Ox)

| Time Window | Trend Score |
|-------------|------------|
| 10:00       | 2.0        |
| 10:01       | 16.0        |
| 10:02       | 2.5        |
| 10:03       | 3.8        |
| 10:04       | 40.2        |

---

## APIs

GET /api/trending/{product_id}/history

Example

```http
GET /api/trending/P15/history
```

Response

```json
[
    {
        "window": "10:00",
        "score": 12.0
    },
    {
        "window": "10:01",
        "score": 16.2
    },
    {
        "window": "10:02",
        "score": 25.3
    }
]
```

---


# 3. Danh sách API của hệ thống

## Customer Dashboard

POST /api/events

GET /api/recommendations/{user_id}

GET /api/trending

---

## Seller Dashboard

GET /api/products/{product_id}/potential-customers

GET /api/trending/{product_id}/history

---
