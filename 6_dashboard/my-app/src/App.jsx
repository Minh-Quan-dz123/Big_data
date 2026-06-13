import { useState } from "react";
import UserBehavior from "./components/UserBehavior";
import Recommendations from "./components/Recommendations";
import Trending from "./components/Trending";
import RealtimeRecommendations from "./components/RealtimeRecommendations";
import { USERS } from "./data/users";

export default function App() {
  // Cập nhật giá trị khởi tạo thành chuẩn 6 số
  const [userId, setUserId] = useState("U000001");
  const [inputUserId, setInputUserId] = useState("U000001");

  const handleConfirm = () => {
    setUserId(inputUserId);
  };

  return (
    <div className="min-h-screen bg-slate-50 p-6 font-sans text-slate-800">
      
      {/* HEADER */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-5 flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">
            Customer Analytics Dashboard
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            Real-time recommendation & behavior tracking system
          </p>
        </div>

        <div className="flex items-center gap-3 bg-slate-50 p-2 rounded-lg border border-slate-200">
          <span className="text-sm font-medium text-gray-600 pl-2">Target User:</span>
          <input
            list="users-list"
            className="border-slate-300 rounded-md px-3 py-1.5 text-sm w-36 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition"
            value={inputUserId}
            onChange={(e) => setInputUserId(e.target.value)}
            placeholder="Search ID..."
          />

          <datalist id="users-list">
            {USERS.map((user) => (
              <option key={user} value={user} />
            ))}
          </datalist>

          <button
            onClick={handleConfirm}
            className="px-4 py-1.5 bg-indigo-600 text-white text-sm font-medium rounded-md hover:bg-indigo-700 transition shadow-sm"
          >
            Monitor
          </button>
        </div>
      </div>

      {/* DASHBOARD GRID */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 mt-6">
        
        {/* ROW 1: Behavior (1/3) & Trending (2/3) */}
        <div className="lg:col-span-4 bg-white rounded-xl shadow-sm border border-slate-200 p-5 h-[420px]">
          <UserBehavior userId={userId} />
        </div>

        <div className="lg:col-span-8 bg-white rounded-xl shadow-sm border border-slate-200 p-5 h-[420px] flex flex-col">
          <Trending />
        </div>

        {/* ROW 2: Batch Recs (1/2) & Realtime Recs (1/2) */}
        <div className="lg:col-span-6 bg-white rounded-xl shadow-sm border border-slate-200 p-5 h-[450px] flex flex-col">
          <Recommendations userId={userId} />
        </div>

        <div className="lg:col-span-6 bg-white rounded-xl shadow-sm border border-slate-200 p-5 h-[450px] flex flex-col">
          <RealtimeRecommendations userId={userId}/>
        </div>

      </div>
    </div>
  );
}