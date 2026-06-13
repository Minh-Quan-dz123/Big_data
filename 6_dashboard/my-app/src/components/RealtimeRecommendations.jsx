import { useEffect, useState } from "react";
import { getRealtimeRecommendations } from "../services/api";

export default function RealtimeRecommendations({ userId }) {
  const [data, setData] = useState(null);

  useEffect(() => {
    getRealtimeRecommendations(userId).then(setData);

    const id = setInterval(() => {
      getRealtimeRecommendations(userId).then(setData);
    }, 3000);

    return () => clearInterval(id);
  }, [userId]);

  if (!data) {
    return (
      <div className="flex items-center justify-center h-full text-sm text-gray-500 animate-pulse">
        Loading real-time insights...
      </div>
    );
  }

  const recs = data.recommendations || [];

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
          Real-time Recommendations
          <span className="relative flex h-3 w-3">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
          </span>
        </h2>
        <p className="text-xs text-gray-500 mt-1">
          Instant updates based on live events for {userId}
        </p>
      </div>

      {/* Product List Scrollable */}
      <div className="flex-1 overflow-y-auto pr-2 space-y-3">
        {recs.length === 0 ? (
          <div className="flex items-center justify-center h-full text-sm text-gray-500 italic">
            Waiting for user activity...
          </div>
        ) : (
          recs.map((r) => (
            <div
              key={r.product_id}
              className="border border-gray-100 bg-white rounded-lg p-3 hover:shadow-md transition duration-200 flex justify-between items-center"
            >
              <div>
                <div className="font-medium text-gray-800">{r.product_name}</div>
                <div className="text-xs text-gray-500 mt-1 flex items-center gap-2">
                  <span className="bg-gray-100 px-2 py-0.5 rounded text-gray-600">
                    {r.category}
                  </span>
                  <span className="bg-blue-50 text-blue-600 px-2 py-0.5 rounded border border-blue-100">
                    Trigger: {r.event_type}
                  </span>
                </div>
              </div>

              <div className="text-right">
                <div className="text-xs text-gray-500 mb-1">Match Score</div>
                <div className="font-bold text-green-600">
                  {Number(r.score).toFixed(2)}
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}