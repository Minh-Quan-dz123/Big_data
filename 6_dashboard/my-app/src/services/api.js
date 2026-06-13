import axios from "axios";

const API_BASE_URL = "http://localhost:30070";


const logError = (apiName, error) => {
  if (error.response) {
    // Server phản hồi với mã lỗi khác 2xx
    console.error(`[${apiName}] Error ${error.response.status}:`, error.response.data);
  } else if (error.request) {
    // Request đã gửi nhưng không nhận được phản hồi
    console.error(`[${apiName}] Network Error: No response received from server.`);
  } else {
    // Lỗi xảy ra khi thiết lập request
    console.error(`[${apiName}] Setup Error:`, error.message);
  }
};

// 1 hàm async post event
export async function sendEvent({ userId, productId, productName, category, eventType }) {
  try {
    await axios.post(`${API_BASE_URL}/api/events`, {
      user_id: userId,
      product_id: productId,
      product_name: productName,
      category: category,
      event_type: eventType,
    });
    return true;
  } catch (error) {
    logError("sendEvent", error);
    return false;
  }
}


// 2 lấy sản phẩm được gợi ý cho user (batch view)
export async function getRecommendations(userId) {
  try {
    const res = await axios.get(`${API_BASE_URL}/api/recommendations/${userId}`);
    return res.data;
  } catch (error) {
    logError("getRecommendations", error);
    // Trả về mảng rỗng để UI báo "No recommendations" thay vì sập web
    return { user_id: userId, recommendations: [] }; 
  }
}

// 3 lấy sản phẩm được gợi ý cho user (realtimes)
export async function getRealtimeRecommendations(userId) {
  try {
    const res = await axios.get(`${API_BASE_URL}/api/recommendations_realtime/${userId}`);
    return res.data;
  } catch (error) {
    logError("getRealtimeRecommendations", error);
    return { user_id: userId, recommendations: [] };
  }
}

// 4 lấy sản phẩm đang trending hiện tại
export async function getTrending() {
  try {
    const res = await axios.get(`${API_BASE_URL}/api/trending`);
    return res.data;
  } catch (error) {
    logError("getTrending", error);
    // Trả về format chuẩn của API với mảng rỗng
    return { count: 0, products: [] }; 
  }
}