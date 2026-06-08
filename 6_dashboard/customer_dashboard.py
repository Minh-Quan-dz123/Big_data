import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import mock_data

API_BASE_URL = "http://localhost:8000"
REQUEST_TIMEOUT = 5

st.set_page_config(
    page_title="Customer Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st_autorefresh(interval=5 * 60 * 1000, key="auto_refresh")

if "user_id" not in st.session_state:
    st.session_state.user_id = "U1"
if "last_action" not in st.session_state:
    st.session_state.last_action = None


def api_get(path):
    try:
        r = requests.get(f"{API_BASE_URL}{path}", timeout=REQUEST_TIMEOUT)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def api_post(path, payload):
    try:
        r = requests.post(f"{API_BASE_URL}{path}", json=payload, timeout=REQUEST_TIMEOUT)
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


def send_event(user_id, product_id, event_type):
    # Phan 5 chua co endpoint nhan event, nen neu API loi thi coi nhu gui thanh cong
    # de luong demo (view/cart/purchase) van chay duoc.
    result = api_post("/api/events", {
        "user_id": user_id,
        "product_id": product_id,
        "event_type": event_type,
    })
    if result is not None:
        return result.get("status") == "ok"
    return True


def fetch_recommendations(user_id):
    data = api_get(f"/api/recommendations/{user_id}")
    if data and isinstance(data, dict):
        return data.get("recommendations", [])
    # Khong co API: dung goi y mau.
    return mock_data.mock_recommendations(user_id).get("recommendations", [])


def fetch_trending():
    data = api_get("/api/trending")
    if data and isinstance(data, dict):
        return data.get("window_end"), data.get("products", [])
    # Khong co API: dung trending mau.
    mock = mock_data.mock_trending()
    return mock.get("window_end"), mock.get("products", [])


# ── Header ───────────────────────────────────────────────────────────────────
col_title, col_uid, col_refresh = st.columns([5, 2, 1], vertical_alignment="center")

with col_title:
    st.title("Customer Dashboard")
    st.caption("Theo dõi hành vi và gợi ý sản phẩm realtime")

with col_uid:
    new_uid = st.text_input(
        "User ID",
        value=st.session_state.user_id,
        placeholder="User ID (vd: U1)",
    )
    if new_uid != st.session_state.user_id:
        st.session_state.user_id = new_uid
        st.rerun()

with col_refresh:
    st.write("")
    if st.button("Làm mới", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

user_id = st.session_state.user_id
st.divider()

# ── 3 cột chính ──────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3, gap="medium")

# ── Cột 1: User Behavior ─────────────────────────────────────────────────────
with col1:
    with st.container(border=True):
        st.subheader("Hành vi người dùng")

        products = load_products()

        if not products:
            st.info("Không có dữ liệu sản phẩm.")
        else:
            options = {
                f"{p['product_id']} — {p.get('product_name', '')} ({p.get('category', '')})": p
                for p in products
            }
            chosen_label = st.selectbox("Chọn sản phẩm", list(options.keys()))
            chosen = options[chosen_label]
            pid = chosen["product_id"]

            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("View", use_container_width=True, key="btn_view"):
                    if send_event(user_id, pid, "view"):
                        st.session_state.last_action = ("view", pid)
                        st.rerun()
            with b2:
                if st.button("Add Cart", use_container_width=True, key="btn_cart"):
                    if send_event(user_id, pid, "cart"):
                        st.session_state.last_action = ("cart", pid)
                        st.rerun()
            with b3:
                if st.button("Purchase", use_container_width=True, type="primary", key="btn_purchase"):
                    if send_event(user_id, pid, "purchase"):
                        st.session_state.last_action = ("purchase", pid)
                        st.rerun()

            if st.session_state.last_action:
                act, apid = st.session_state.last_action
                st.success(f"Đã gửi: {act} → {apid}")

            st.markdown("**Danh sách sản phẩm**")
            df_prod = pd.DataFrame(products)
            cols_show = [c for c in ["product_id", "product_name", "category", "price"] if c in df_prod.columns]
            st.dataframe(
                df_prod[cols_show],
                use_container_width=True,
                height=300,
                hide_index=True,
                column_config={
                    "product_id": "ID",
                    "product_name": "Sản phẩm",
                    "category": "Danh mục",
                    "price": st.column_config.NumberColumn("Giá", format="%d đ"),
                },
            )


# ── Cột 2: Recommendations ───────────────────────────────────────────────────
with col2:
    with st.container(border=True):
        st.subheader("Gợi ý cho bạn")
        st.caption(f"User: {user_id}")

        recs = fetch_recommendations(user_id)

        if not recs:
            st.info("Chưa có gợi ý. Thực hiện hành vi ở cột bên trái.")
        else:
            df = pd.DataFrame(recs)
            col_map = {"product_id": "ID", "product_name": "Sản phẩm", "category": "Danh mục", "score": "Score", "type": "Loại"}
            display_cols = [c for c in col_map if c in df.columns]
            df_show = df[display_cols].rename(columns=col_map)
            df_show.insert(0, "#", range(1, len(df_show) + 1))
            # ProgressColumn bien score thanh thanh tien trinh truc quan thay vi so kho.
            st.dataframe(
                df_show,
                use_container_width=True,
                height=430,
                hide_index=True,
                column_config={
                    "Score": st.column_config.ProgressColumn(
                        "Score", format="%.1f", min_value=0, max_value=100
                    ),
                },
            )
            st.caption(f"{len(recs)} gợi ý — tự động cập nhật mỗi 5 phút")


# ── Cột 3: Trending ──────────────────────────────────────────────────────────
with col3:
    with st.container(border=True):
        window_end, trending = fetch_trending()
        st.subheader("Sản phẩm hot")
        st.caption(f"Window mới nhất: {window_end}" if window_end else "Window 5 phút / slide 1 phút")

        if not trending:
            st.info("Chưa có dữ liệu trending.")
        else:
            df = pd.DataFrame(trending)
            col_map = {"product_id": "ID", "product_name": "Sản phẩm", "category": "Danh mục", "trend_score": "Score"}
            display_cols = [c for c in col_map if c in df.columns]
            df_show = df[display_cols].rename(columns=col_map)
            df_show.insert(0, "Rank", range(1, len(df_show) + 1))

            top = df_show.iloc[0]
            st.metric(f"#1 {top['Sản phẩm']}", f"{top['Score']:.0f}", help="Trend score cao nhất hiện tại")

            max_score = max(df_show["Score"].max(), 1)
            st.dataframe(
                df_show,
                use_container_width=True,
                height=330,
                hide_index=True,
                column_config={
                    "Score": st.column_config.ProgressColumn(
                        "Trend", format="%.0f", min_value=0, max_value=float(max_score)
                    ),
                },
            )
