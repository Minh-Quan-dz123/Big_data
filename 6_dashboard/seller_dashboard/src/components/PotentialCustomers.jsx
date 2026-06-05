import { Users } from 'lucide-react';

export default function PotentialCustomers({ data, isLoading }) {
  if (isLoading) {
    return (
      <div className="glass-panel flex-center" style={{ minHeight: '400px' }}>
        <div className="loading-spinner">Loading potential customers...</div>
      </div>
    );
  }

  if (!data || !data.customers || data.customers.length === 0) {
    return (
      <div className="glass-panel flex-center" style={{ minHeight: '400px' }}>
        <p style={{ color: 'var(--text-secondary)' }}>No potential customers found for this product.</p>
      </div>
    );
  }

  const getScoreClass = (score) => {
    if (score >= 90) return 'score-high';
    if (score >= 70) return 'score-medium';
    return 'score-low';
  };

  return (
    <div className="glass-panel">
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1.5rem' }}>
        <Users size={20} color="var(--accent-primary)" />
        <h3 style={{ fontSize: '1.25rem' }}>Potential Customers</h3>
      </div>
      
      <div style={{ overflowX: 'auto' }}>
        <table className="styled-table">
          <thead>
            <tr>
              <th>User ID</th>
              <th>Interest Score</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            {data.customers.map((customer, idx) => (
              <tr key={`${customer.user_id}-${idx}`}>
                <td style={{ fontWeight: '500' }}>{customer.user_id}</td>
                <td className={getScoreClass(customer.interest_score)}>
                  {customer.interest_score.toFixed(1)}
                </td>
                <td>
                  <span className={`badge badge-${customer.type}`}>
                    {customer.type}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
