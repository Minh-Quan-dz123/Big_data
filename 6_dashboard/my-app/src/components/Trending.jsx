import { useEffect, useState } from "react";
import { getTrending } from "../services/api";

export default function Trending() {
  const [data, setData] = useState(null);

  useEffect(() => {
    // Gọi lần đầu khi trang vừa load
    getTrending().then(setData);

    // Đặt bộ đếm tự động gọi lại mỗi 5 giây (5000ms)
    const id = setInterval(() => {
      getTrending().then(setData);
    }, 5000);

    // Dọn dẹp bộ đếm khi tắt component
    return () => clearInterval(id);
  }, []);

  if (!data) {
    return (
      <div className="text-sm text-gray-500 animate-pulse">
        Loading trending data...
      </div>
    );
  }

  const top = data.products?.[0];

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-gray-800">
          Live Trending Products
        </h2>
        <p className="text-xs text-gray-500">
          Auto-updating every 5s
        </p>
      </div>

      {/* Top product (Chỉ hiện nếu có dữ liệu) */}
      {top && (
        <div className="bg-gradient-to-r from-indigo-50 to-blue-50 border border-indigo-100 rounded-lg p-4 mb-4 shadow-sm">
          <div className="text-xs text-indigo-500 mb-1 font-semibold tracking-wider uppercase">
            🔥 #1 Trending Now
          </div>

          <div className="font-bold text-gray-800 text-lg">
            {top.product_name}
          </div>

          <div className="text-sm text-gray-500 mb-2">
            {top.category}
          </div>

          <div className="text-indigo-600 font-bold bg-white inline-block px-2 py-1 rounded text-sm mb-2 shadow-sm">
            Trend Score: {top.trend_score.toFixed(2)}
          </div>

          <div className="text-xs text-gray-600 flex gap-4 font-medium mt-1">
            <span className="bg-white px-2 py-1 rounded">👁 {top.view_count} Views</span>
            <span className="bg-white px-2 py-1 rounded">🛒 {top.cart_count} Carts</span>
            <span className="bg-white px-2 py-1 rounded">💳 {top.purchase_count} Sold</span>
          </div>
        </div>
      )}

      {/* Product list */}
      <div className="flex-1 overflow-auto space-y-2">
        {data.products.map((p, index) => (
          <div
            key={p.product_id}
            className="border rounded-lg p-3 hover:bg-gray-50 transition"
          >
            <div className="flex justify-between items-start">
              <div>
                <div className="font-medium text-gray-800">
                  <span className="text-gray-400 mr-1">#{index + 2}</span> 
                  {p.product_name}
                </div>

                <div className="text-xs text-gray-500">
                  {p.product_id} • {p.category}
                </div>
              </div>

              <div className="text-indigo-600 font-semibold">
                {p.trend_score}
              </div>
            </div>

            <div className="mt-2 flex gap-4 text-xs text-gray-600">
              <span>👁 Views: {p.view_count}</span>
              <span>🛒 Carts: {p.cart_count}</span>
              <span>💳 Purchases: {p.purchase_count}</span>
            </div>
          </div>
        ))}

        {data.products.length === 0 && (
          <div className="text-sm text-gray-500">
            Waiting for real-time user events...
          </div>
        )}
      </div>
    </div>
  );
}