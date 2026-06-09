import { useEffect, useState } from "react";
import { getTrending } from "../services/api";

export default function Trending() {
  const [data, setData] = useState(null);

  useEffect(() => {
    getTrending().then(setData);
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
          Trending Products
        </h2>
        <p className="text-xs text-gray-500">
          Updated at {data.window_end}
        </p>
      </div>

      {/* Top product */}
      {top && (
        <div className="bg-gradient-to-r from-indigo-50 to-blue-50 border rounded-lg p-4 mb-4">
          <div className="text-xs text-gray-500 mb-1">
            🔥 Top Trending Product
          </div>

          <div className="font-semibold text-gray-800 text-lg">
            {top.product_name}
          </div>

          <div className="text-sm text-gray-500 mb-2">
            {top.category}
          </div>

          <div className="text-indigo-600 font-semibold">
            Trend Score: {top.trend_score}
          </div>

          <div className="mt-2 text-xs text-gray-600 flex gap-4">
            <span>👁 {top.view_count}</span>
            <span>🛒 {top.cart_count}</span>
            <span>💳 {top.purchase_count}</span>
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
                  #{index + 1} {p.product_name}
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
            No trending products available.
          </div>
        )}
      </div>
    </div>
  );
}