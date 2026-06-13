import { useEffect, useState } from "react";
import { getRecommendations } from "../services/api";

export default function Recommendations({ userId }) {
  const [data, setData] = useState(null);

  useEffect(() => {
    getRecommendations(userId).then(setData);
  }, [userId]);

  const recs = data?.recommendations || [];
  const segmentName = data?.segment_name;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
          Batch Recommendations
          {segmentName && segmentName !== "Unknown" && (
            <span className="inline-block px-3 py-1 text-xs font-semibold bg-purple-100 text-purple-700 rounded-full border border-purple-200 shadow-sm">
              {segmentName}
            </span>
          )}
        </h2>
        <p className="text-xs text-gray-500 mt-1">
          Historical data processing for {userId}
        </p>
      </div>

      {/* Table Wrapper for Scrolling */}
      <div className="flex-1 overflow-y-auto rounded-lg border border-gray-200">
        <div className="min-w-full">
          {/* Table Header - Sticky */}
          <div className="sticky top-0 z-10 grid grid-cols-12 bg-gray-50 border-b border-gray-200 px-3 py-2 text-xs font-bold text-gray-600 uppercase tracking-wider">
            <div className="col-span-1">#</div>
            <div className="col-span-5">Product</div>
            <div className="col-span-3">Category</div>
            <div className="col-span-3 text-right">Score</div>
          </div>

          {/* Table Body */}
          <div className="divide-y divide-gray-100 bg-white">
            {recs.length === 0 ? (
              <div className="p-8 text-center text-sm text-gray-500">
                No recommendations found.
              </div>
            ) : (
              recs.map((r, i) => (
                <div
                  key={r.product_id}
                  className="grid grid-cols-12 px-3 py-3 text-sm hover:bg-indigo-50 transition"
                >
                  <div className="col-span-1 text-gray-400 font-medium">{i + 1}</div>
                  <div className="col-span-5 font-medium text-gray-800 truncate pr-2">
                    {r.product_name}
                  </div>
                  <div className="col-span-3 text-gray-500 truncate pr-2">
                    {r.category}
                  </div>
                  <div className="col-span-3 text-right font-semibold text-indigo-600">
                    {Number(r.score).toFixed(1)}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}