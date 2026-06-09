import pandas as pd
import requests
import streamlit as st

import mock_data

API_BASE_URL = "http://localhost:8000"
REQUEST_TIMEOUT = 5

st.set_page_config(
    page_title="Seller Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Luu product_id da chon de giu nguyen ket qua khi giao dien chay lai.
if "selected_pid" not in st.session_state:
    st.session_state.selected_pid = None


def api_get(path):
    try:
        r = requests.get(f"{API_BASE_URL}{path}", timeout=REQUEST_TIMEOUT)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_products():
    # Goi API that truoc, neu khong co thi dung danh sach san pham mau.
    data = api_get("/api/products")
    if isinstance(data, list) and data:
        return data
    return mock_data.mock_products()


def fetch_potential_customers(product_id):
    data = api_get(f"/api/products/{product_id}/potential-customers")
    if data and isinstance(data, dict):
        return data.get("customers", [])
    # Khong co API: dung khach hang tiem nang mau.
    return mock_data.mock_potential_customers(product_id).get("customers", [])


def fetch_trend_history(product_id):
    data = api_get(f"/api/trending/{product_id}/history")
    if isinstance(data, list) and data:
        return data
    # Khong co API: dung dien bien trend mau.
    return mock_data.mock_trend_history(product_id)


# ── Header: chon san pham va xac nhan ─────────────────────────────────────────
st.title("Seller Dashboard")
st.caption("Tìm khách hàng tiềm năng và phân tích xu hướng sản phẩm")

products = load_products()

with st.container(border=True):
    col_pick, col_confirm = st.columns([5, 1], vertical_alignment="bottom")

    with col_pick:
        if products:
            options = {
                f"{p['product_id']} — {p.get('product_name', '')} ({p.get('category', '')})": p
                for p in products
            }
            chosen_label = st.selectbox("Chọn sản phẩm", list(options.keys()))
            chosen = options[chosen_label]
        else:
            chosen = None
            st.info("Không có dữ liệu sản phẩm.")

    with col_confirm:
        # Chi goi API va hien ket qua khi nguoi dung bam xac nhan.
        if st.button("Xác nhận", use_container_width=True, type="primary") and chosen:
            st.session_state.selected_pid = chosen["product_id"]

# Neu chua chon san pham nao thi dung lai, khong goi API.
if not st.session_state.selected_pid:
    st.info("Chọn một sản phẩm rồi bấm Xác nhận để xem khách hàng tiềm năng và diễn biến xu hướng.")
    st.stop()

pid = st.session_state.selected_pid

# ── Hai cot: khach hang tiem nang | dien bien xu huong ────────────────────────
col_left, col_right = st.columns([2, 3], gap="medium")

# ── Khu vuc 1: Potential Customers ────────────────────────────────────────────
with col_left:
    with st.container(border=True):
        st.subheader("Khách hàng tiềm năng")
        st.caption(f"Sản phẩm: {pid}")

        customers = fetch_potential_customers(pid)

        if not customers:
            st.info("Chưa có dữ liệu khách hàng tiềm năng.")
        else:
            df = pd.DataFrame(customers)
            # Sap xep theo diem quan tam giam dan.
            if "interest_score" in df.columns:
                df = df.sort_values("interest_score", ascending=False)

            c1, c2 = st.columns(2)
            c1.metric("Số khách tiềm năng", len(df))
            if "interest_score" in df.columns:
                c2.metric("Điểm cao nhất", f"{df['interest_score'].max():.0f}")

            col_map = {"user_id": "User", "interest_score": "Interest", "type": "Type"}
            display_cols = [c for c in col_map if c in df.columns]
            df_show = df[display_cols].rename(columns=col_map)
            st.dataframe(
                df_show,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Interest": st.column_config.ProgressColumn(
                        "Interest", format="%.1f", min_value=0, max_value=100
                    ),
                },
            )

# ── Khu vuc 2: Product Trend Analytics ────────────────────────────────────────
with col_right:
    with st.container(border=True):
        st.subheader("Diễn biến xu hướng")
        st.caption(f"Trend Score của {pid} theo từng time window")

        history = fetch_trend_history(pid)

        if not history:
            st.info("Chưa có dữ liệu lịch sử xu hướng.")
        else:
            df = pd.DataFrame(history)
            # Truc X la time window, truc Y la trend score.
            if "window" in df.columns and "score" in df.columns:
                latest = df["score"].iloc[-1]
                peak = df["score"].max()
                m1, m2 = st.columns(2)
                m1.metric("Score hiện tại", f"{latest:.1f}")
                m2.metric("Score cao nhất", f"{peak:.1f}")

                chart_df = df.set_index("window")[["score"]].rename(columns={"score": "Trend Score"})
                st.area_chart(chart_df, height=300)
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
