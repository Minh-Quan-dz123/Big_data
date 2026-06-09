import { useEffect, useState } from "react";
import { getRecommendations } from "../services/api";

export default function Recommendations({ userId }) {
  const [data, setData] = useState(null);

  useEffect(() => {
    getRecommendations(userId).then(setData);
  }, [userId]);

  const recs = data?.recommendations || [];

  //#	Product	Category	Type	Score
  //1	Fami	Vitamin	ABC	98.3

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-gray-800">
          Recommendations
        </h2>
        <p className="text-xs text-gray-500">
          Personalized ranking for user {userId}
        </p>
      </div>

      {/* Table */}
      <div className="overflow-hidden rounded-lg border border-gray-200">
        <div className="grid grid-cols-12 bg-gray-100 px-3 py-2 text-xs font-semibold text-gray-600">
          <div className="col-span-1">#</div>
          <div className="col-span-4">Product</div>
          <div className="col-span-3">Category</div>
          <div className="col-span-4 text-right">Score</div>
        </div>


        <div className="divide-y">
          {recs.length === 0 ? (
            <div className="p-4 text-center text-sm text-gray-500">
              No recommendations
            </div>
          ) : (
            recs.map((r, i) => (
              <div
                key={r.product_id}
                className="grid grid-cols-12 px-3 py-2 text-sm hover:bg-gray-50"
              >
                <div className="col-span-1 text-gray-500">
                  {i + 1}
                </div>

                <div className="col-span-4 font-medium text-gray-800">
                  {r.product_name}
                </div>

                <div className="col-span-3 text-gray-600">
                  {r.category}
                </div>

                <div className="col-span-4 text-right font-semibold text-indigo-600">
                  {r.score.toFixed(1)}
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}