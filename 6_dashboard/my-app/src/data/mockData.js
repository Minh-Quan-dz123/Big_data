import { products } from "./products";

export const PRODUCTS = products;
export function mockProducts() {
  return PRODUCTS;
}

export function mockRecommendations(userId) {
  return {
    user_id: userId,
    recommendations: [
      { product_id: "P15", product_name: "Nimbus Stay", category: "Toys", score: 98.3 },
      { product_id: "P8", product_name: "Orion Head", category: "Beauty", score: 95.0 },
      { product_id: "P20", product_name: "Atlas Pro", category: "Electronics", score: 90.4 },
      { product_id: "P3", product_name: "Nimbus Whose", category: "Clothing", score: 85.1 },
      { product_id: "P7", product_name: "Atlas Mini", category: "Electronics", score: 80.7 },
    ],
  };
}

export function mockTrending() {
  return {
    window_end: "10:05",
    products: [
      { product_id: "P15", product_name: "Nimbus Stay", category: "Toys", trend_score: 250 },
      { product_id: "P8", product_name: "Orion Head", category: "Beauty", trend_score: 220 },
      { product_id: "P3", product_name: "Nimbus Whose", category: "Clothing", trend_score: 180 },
      { product_id: "P20", product_name: "Atlas Pro", category: "Electronics", trend_score: 140 },
    ],
  };
}

export function mockPotentialCustomers(productId) {
  return {
    product_id: productId,
    customers: [
      { user_id: "U1", interest_score: 98.0, type: "consumption" },
      { user_id: "U3", interest_score: 95.0, type: "similar" },
      { user_id: "U5", interest_score: 90.2, type: "complementary" },
      { user_id: "U7", interest_score: 85.4, type: "similar" },
    ],
  };
}