import { useState } from "react";
import UserBehavior from "./components/UserBehavior";
import Recommendations from "./components/Recommendations";
import Trending from "./components/Trending";
import RealtimeRecommendations from "./components/RealtimeRecommendations";

export default function App() {
  const [userId, setUserId] = useState("U1");
  const [inputUserId, setInputUserId] = useState("U1");

  const handleConfirm = () => {
    setUserId(inputUserId);
  };

  return (
    <div className="min-h-screen bg-gray-100 p-6">
      
      {/* HEADER */}
      <div className="bg-white rounded-xl shadow-sm p-5 flex justify-between items-center">
        <div>
          <h1 className="text-xl font-bold text-gray-800">
            Customer Analytics Dashboard
          </h1>
          <p className="text-sm text-gray-500">
            Real-time recommendation & behavior tracking system
          </p>
        </div>

        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">User:</span>
          <input
            className="border rounded-lg px-3 py-2 text-sm w-32"
            value={inputUserId}
            onChange={(e) => setInputUserId(e.target.value)}
          />

          <button
            onClick={handleConfirm}
            className="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700"
          >
            Confirm
          </button>
        </div>
      </div>

      {/* GRID */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
        
        <div className="bg-white rounded-xl shadow-sm p-4">
          <UserBehavior userId={userId} />
        </div>

        <div className="bg-white rounded-xl shadow-sm p-4">
          <Recommendations userId={userId} />
        </div>

        <div className="bg-white rounded-xl shadow-sm p-4">
          <Trending />
        </div>

        <div className="bg-white rounded-xl shadow-sm p-4">
          <RealtimeRecommendations userId={userId}/>
        </div>

      </div>
    </div>
  );
}