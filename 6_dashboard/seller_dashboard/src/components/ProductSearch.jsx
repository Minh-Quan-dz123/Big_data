import { useState } from 'react';
import { Search } from 'lucide-react';

export default function ProductSearch({ onSearch, isLoading }) {
  const [productId, setProductId] = useState('P15');

  const handleSubmit = (e) => {
    e.preventDefault();
    if (productId.trim()) {
      onSearch(productId.trim());
    }
  };

  return (
    <div className="glass-panel" style={{ marginBottom: '2rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
      <div>
        <h2 style={{ fontSize: '1.25rem', marginBottom: '0.25rem', color: 'var(--text-primary)' }}>Select Product</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>Enter a product ID to analyze trends and potential customers.</p>
      </div>
      
      <form onSubmit={handleSubmit} className="input-group">
        <input
          type="text"
          value={productId}
          onChange={(e) => setProductId(e.target.value)}
          placeholder="e.g. P15"
          className="styled-input"
          disabled={isLoading}
        />
        <button type="submit" className="btn-primary" disabled={isLoading || !productId.trim()}>
          <Search size={18} />
          {isLoading ? 'Analyzing...' : 'Analyze'}
        </button>
      </form>
    </div>
  );
}
