import { useEffect, useState } from "react";
import { getRealtimeRecommendations } from "../services/api";

export default function RealtimeRecommendations({ userId }) {
  const [data, setData] = useState(null);

  // 1 gọi api mỗi 5s
  useEffect(() => {
    getRealtimeRecommendations(userId).then(setData);

    const id = setInterval(() => {
      getRealtimeRecommendations(userId).then(setData);
    }, 5000);

    return () => clearInterval(id);
  }, [userId]);

  if (!data)
    return <div>Loading...</div>;

  return (
    <div>
      <h2 className="text-lg font-semibold">
        Realtime Recommendations
      </h2>

      <div className="space-y-2 mt-3">
        {data.recommendations.map(r => (
          <div
            key={r.product_id}
            className="border rounded p-2"
          >
            <div>{r.product_name}</div>

            <div className="text-xs text-gray-500">
              {r.category}
            </div>

            <div className="text-xs">
              Event: {r.event_type}
            </div>

            <div>
              Score: {r.score}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}