// Mock implementation of Seller Dashboard APIs

const MOCK_CUSTOMERS = {
  P15: [
    { user_id: 'U1', interest_score: 98.0, type: 'recent_interest' },
    { user_id: 'U3', interest_score: 95.0, type: 'frequently_bought_together' },
    { user_id: 'U5', interest_score: 90.2, type: 'similar_product' },
    { user_id: 'U7', interest_score: 85.0, type: 'complementary_product' }
  ],
  P8: [
    { user_id: 'U2', interest_score: 99.1, type: 'trending' },
    { user_id: 'U8', interest_score: 88.5, type: 'similar_product' }
  ]
};

const MOCK_TRENDS = {
  P15: [
    { window: '10:00', score: 2.0, total_views: 10, total_carts: 1, total_purchases: 0, growth_rate: 1.0 },
    { window: '10:01', score: 16.0, total_views: 45, total_carts: 5, total_purchases: 1, growth_rate: 1.5 },
    { window: '10:02', score: 2.5, total_views: 8, total_carts: 0, total_purchases: 0, growth_rate: 0.8 },
    { window: '10:03', score: 3.8, total_views: 12, total_carts: 2, total_purchases: 0, growth_rate: 1.1 },
    { window: '10:04', score: 40.2, total_views: 150, total_carts: 20, total_purchases: 5, growth_rate: 2.5 },
    { window: '10:05', score: 35.0, total_views: 120, total_carts: 15, total_purchases: 3, growth_rate: 0.9 },
    { window: '10:06', score: 55.4, total_views: 200, total_carts: 35, total_purchases: 8, growth_rate: 1.8 },
    { window: '10:07', score: 62.1, total_views: 220, total_carts: 40, total_purchases: 10, growth_rate: 1.2 },
    { window: '10:08', score: 60.5, total_views: 210, total_carts: 38, total_purchases: 9, growth_rate: 0.95 },
    { window: '10:09', score: 78.9, total_views: 300, total_carts: 50, total_purchases: 15, growth_rate: 1.4 }
  ],
  P8: [
    { window: '10:00', score: 50.0 },
    { window: '10:01', score: 45.0 },
    { window: '10:02', score: 42.5 },
    { window: '10:03', score: 55.8 },
    { window: '10:04', score: 60.2 },
    { window: '10:05', score: 58.0 },
    { window: '10:06', score: 65.4 },
    { window: '10:07', score: 72.1 },
    { window: '10:08', score: 70.5 },
    { window: '10:09', score: 88.9 }
  ]
};

const generateRandomCustomers = () => {
  const count = Math.floor(Math.random() * 5) + 3; // 3 to 7
  const types = ['trending', 'similar_product', 'recent_interest', 'complementary_product', 'frequently_bought_together'];
  const res = [];
  for (let i = 0; i < count; i++) {
    res.push({
      user_id: `U${Math.floor(Math.random() * 100) + 10}`,
      interest_score: parseFloat((Math.random() * 40 + 60).toFixed(1)),
      type: types[Math.floor(Math.random() * types.length)]
    });
  }
  return res.sort((a, b) => b.interest_score - a.interest_score);
};

const generateRandomTrends = () => {
  const res = [];
  let currentScore = Math.random() * 20 + 10;
  for (let i = 0; i < 10; i++) {
    currentScore += (Math.random() * 20 - 5); // mostly trending up
    if (currentScore < 0) currentScore = 0;
    res.push({
      window: `10:0${i}`,
      score: parseFloat(currentScore.toFixed(1)),
      total_views: Math.floor(currentScore * 3),
      total_carts: Math.floor(currentScore * 0.5),
      total_purchases: Math.floor(currentScore * 0.1),
      growth_rate: parseFloat((Math.random() * 1.5 + 0.5).toFixed(2))
    });
  }
  return res;
};

// Simulate network delay
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export const getPotentialCustomers = async (productId) => {
  await delay(500); // 500ms delay
  const data = MOCK_CUSTOMERS[productId] || generateRandomCustomers();
  return {
    product_id: productId,
    customers: data
  };
};

export const getProductTrendHistory = async (productId) => {
  await delay(600); // 600ms delay
  return MOCK_TRENDS[productId] || generateRandomTrends();
};

const MOCK_PRODUCTS = {
  P15: { id: 'P15', name: 'Nimbus Stay', category: 'Toys' },
  P8: { id: 'P8', name: 'Orion Head', category: 'Beauty' },
  P3: { id: 'P3', name: 'Nimbus Whose', category: 'Clothing' },
  P20: { id: 'P20', name: 'Atlas Pro', category: 'Electronics' }
};

export const getProductInfo = async (productId) => {
  await delay(100);
  return MOCK_PRODUCTS[productId] || { id: productId, name: 'Unknown Product', category: 'General' };
};
