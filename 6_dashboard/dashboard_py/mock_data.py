# Du lieu mau dung tam khi Serving Layer (phan 5) chua co API that.
# Cau truc cua moi ham bam sat dung response JSON mo ta trong README.md,
# nho vay khi API that san sang chi can goi API, khong phai sua giao dien.

# Danh sach san pham co dinh, dung lam nguon cho ca hai dashboard.
PRODUCTS = [
    {"product_id": "P3", "product_name": "Nimbus Whose", "category": "Clothing", "price": 320000},
    {"product_id": "P7", "product_name": "Atlas Mini", "category": "Electronics", "price": 1500000},
    {"product_id": "P8", "product_name": "Orion Head", "category": "Beauty", "price": 210000},
    {"product_id": "P15", "product_name": "Nimbus Stay", "category": "Toys", "price": 90000},
    {"product_id": "P20", "product_name": "Atlas Pro", "category": "Electronics", "price": 5200000},
]


def mock_products():
    # Tra ve ban sao de noi goi khong lo sua nham du lieu goc.
    return [dict(p) for p in PRODUCTS]


def mock_recommendations(user_id):
    # Gia lap goi y san pham cho mot user. Score giam dan theo thu hang.
    recs = [
        {"product_id": "P15", "product_name": "Nimbus Stay", "category": "Toys", "score": 98.3},
        {"product_id": "P8", "product_name": "Orion Head", "category": "Beauty", "score": 95.0},
        {"product_id": "P20", "product_name": "Atlas Pro", "category": "Electronics", "score": 90.4},
        {"product_id": "P3", "product_name": "Nimbus Whose", "category": "Clothing", "score": 85.1},
        {"product_id": "P7", "product_name": "Atlas Mini", "category": "Electronics", "score": 80.7},
    ]
    return {"user_id": user_id, "recommendations": recs}


def mock_trending():
    # Top san pham dang hot, kem moc thoi gian cua window moi nhat.
    products = [
        {"product_id": "P15", "product_name": "Nimbus Stay", "category": "Toys", "trend_score": 250},
        {"product_id": "P8", "product_name": "Orion Head", "category": "Beauty", "trend_score": 220},
        {"product_id": "P3", "product_name": "Nimbus Whose", "category": "Clothing", "trend_score": 180},
        {"product_id": "P20", "product_name": "Atlas Pro", "category": "Electronics", "trend_score": 140},
    ]
    return {"window_end": "10:05", "products": products}


def mock_potential_customers(product_id):
    # Danh sach khach hang tiem nang cho mot san pham (nguon: Batch Recommendation).
    customers = [
        {"user_id": "U1", "interest_score": 98.0, "type": "consumption"},
        {"user_id": "U3", "interest_score": 95.0, "type": "similar"},
        {"user_id": "U5", "interest_score": 90.2, "type": "complementary"},
        {"user_id": "U7", "interest_score": 85.4, "type": "similar"},
    ]
    return {"product_id": product_id, "customers": customers}


def mock_trend_history(product_id):
    # Dien bien trend score cua mot san pham trong 10 window gan nhat.
    windows = ["10:00", "10:01", "10:02", "10:03", "10:04",
               "10:05", "10:06", "10:07", "10:08", "10:09"]
    scores = [2.0, 16.0, 2.5, 3.8, 40.2, 35.1, 28.7, 31.0, 45.6, 52.3]
    return [{"window": w, "score": s} for w, s in zip(windows, scores)]
