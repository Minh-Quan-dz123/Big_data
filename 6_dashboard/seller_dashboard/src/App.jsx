import { useState, useEffect } from 'react';
import { Activity } from 'lucide-react';
import ProductSearch from './components/ProductSearch';
import PotentialCustomers from './components/PotentialCustomers';
import ProductTrendChart from './components/ProductTrendChart';
import { getPotentialCustomers, getProductTrendHistory, getProductInfo } from './services/api';

function App() {
  const [currentProduct, setCurrentProduct] = useState(null);
  const [customersData, setCustomersData] = useState(null);
  const [trendData, setTrendData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  const fetchDashboardData = async (productId) => {
    setIsLoading(true);
    try {
      const [productInfo, customers, trends] = await Promise.all([
        getProductInfo(productId),
        getPotentialCustomers(productId),
        getProductTrendHistory(productId)
      ]);
      setCurrentProduct(productInfo);
      setCustomersData(customers);
      setTrendData(trends);
    } catch (error) {
      console.error("Failed to fetch dashboard data:", error);
    } finally {
      setIsLoading(false);
    }
  };

  // Initial fetch for a default product
  useEffect(() => {
    fetchDashboardData('P15');
  }, []);

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <div>
          <h1 className="dashboard-title">Seller Dashboard</h1>
          <p style={{ color: 'var(--text-secondary)', marginTop: '0.5rem' }}>
            Analyze business data and customer potential from the Recommendation Engine.
          </p>
        </div>
        <div style={{ background: 'rgba(59, 130, 246, 0.1)', padding: '0.75rem', borderRadius: '50%', color: 'var(--accent-primary)' }}>
          <Activity size={28} />
        </div>
      </header>

      <ProductSearch onSearch={fetchDashboardData} isLoading={isLoading} />

      {currentProduct && (
        <div style={{ marginBottom: '1rem', background: 'var(--glass-bg)', padding: '1rem', borderRadius: 'var(--radius-md)', border: 'var(--glass-border)' }}>
          <h2 style={{ fontSize: '1.25rem', color: 'var(--text-secondary)', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            Selected Product: 
            <span style={{ color: 'var(--text-primary)', fontWeight: '600' }}>
              {currentProduct.id} - {currentProduct.name}
            </span>
            <span className="badge" style={{ backgroundColor: 'rgba(255,255,255,0.1)', color: 'var(--text-secondary)' }}>
              {currentProduct.category}
            </span>
          </h2>
        </div>
      )}

      <div className="grid-2">
        <PotentialCustomers data={customersData} isLoading={isLoading} />
        <ProductTrendChart data={trendData} isLoading={isLoading} />
      </div>
    </div>
  );
}

export default App;
