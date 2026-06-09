import { useState } from "react";
import { sendEvent } from "../services/api";
import { PRODUCTS } from "../data/mockData";


export default function UserBehavior({ userId }) 
{
  const [selected, setSelected] = useState(null);
  const [sending, setSending] = useState(false);


  const handle = async (type) => {
    if (!selected) return;

    setSending(true);

    try {
      await sendEvent(
        userId,
        selected.product_id,
        selected.product_name,
        selected.category,
        type
      );
    } finally {
      setSending(false);
    }
  };


  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-gray-800">
          User Behavior
        </h2>
        <p className="text-xs text-gray-500">
          Simulate real-time user events
        </p>
      </div>

      {/* Select */}
      <select
        className="w-full border rounded-lg px-3 py-2 text-sm mb-4 focus:outline-none focus:ring-2 focus:ring-indigo-400"
        onChange={(e) =>
          setSelected(
            PRODUCTS.find(
              (p) => p.product_id === e.target.value
            )
          )

        }
      >
        <option value="">Select product</option>
        {PRODUCTS.map(p => (
          <option key={p.product_id} value={p.product_id}>
            {p.product_name}
          </option>
        ))}
      </select>

      {/* Actions */}
      {selected && (
        <div className="border rounded-lg p-3 mb-4 bg-gray-50">
          <div className="font-medium">
            {selected.product_name}
          </div>

          <div className="text-xs text-gray-500">
            {selected.product_id} • {selected.category}
          </div>

          <div className="text-sm text-indigo-600 mt-1">
            ${selected.price.toFixed(2)}
          </div>
        </div>
      )}

      {/* Action buttons */}
      <div className="grid grid-cols-3 gap-2 mt-auto">
        <button
          disabled={!selected || sending}
          onClick={() => handle("view")}
          className="bg-blue-500 hover:bg-blue-600 disabled:bg-gray-300 text-white text-sm py-2 rounded-lg"
        >
          View
        </button>

        <button
          disabled={!selected || sending}
          onClick={() => handle("cart")}
          className="bg-yellow-500 hover:bg-yellow-600 disabled:bg-gray-300 text-white text-sm py-2 rounded-lg"
        >
          Cart
        </button>

        <button
          disabled={!selected || sending}
          onClick={() => handle("purchase")}
          className="bg-green-500 hover:bg-green-600 disabled:bg-gray-300 text-white text-sm py-2 rounded-lg"
        >
          Buy
        </button>
      </div>
    </div>
  );
}