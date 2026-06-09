import axios from "axios";
import { mockProducts, mockRecommendations, mockTrending } from "../data/mockData";

const API_BASE_URL = "http://localhost:30070";


// 1 hàm async post event
export async function sendEvent(userId, productId, productName, category, eventType) {
  try {
    await axios.post(`${API_BASE_URL}/api/events`, {
      user_id: userId,
      product_id: productId,
      product_name: productName,
      category: category,
      event_type: eventType,
    });
    return true;
  } 
  catch (e) {
    return true; // để ko bị crash
  }
}


// 2 lấy sản phẩm được gợi ý cho user (batch view)
export async function getRecommendations(userId) {
  try {
    const res = await axios.get(
      `${API_BASE_URL}/api/recommendations/${userId}`
    );

    return res.data;
  } catch {
    return mockRecommendations(userId);
  }
}

// 3 lấy sản phẩm được gợi ý cho user (realtimes)
export async function getRealtimeRecommendations(userId) {
  try {
    const res = await axios.get(
      `${API_BASE_URL}/api/recommendations_realtime/${userId}`
    );

    return res.data;
  } catch {
    return {
      user_id: userId,
      recommendations: [],
    };
  }
}

// 4 lấy sản phẩm đang trending hiện tại
export async function getTrending() {
  try {
    const res = await axios.get(
      `${API_BASE_URL}/api/trending`
    );

    return res.data;
  } catch {
    return mockTrending();
  }
}