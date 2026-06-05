import { TrendingUp } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function ProductTrendChart({ data, isLoading }) {
  if (isLoading) {
    return (
      <div className="glass-panel flex-center" style={{ minHeight: '400px' }}>
        <div className="loading-spinner">Loading trend data...</div>
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div className="glass-panel flex-center" style={{ minHeight: '400px' }}>
        <p style={{ color: 'var(--text-secondary)' }}>No trend data available for this product.</p>
      </div>
    );
  }

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div style={{ 
          background: 'rgba(15, 23, 42, 0.95)', 
          border: '1px solid rgba(255,255,255,0.1)',
          padding: '1rem',
          borderRadius: '8px',
          boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
          minWidth: '180px'
        }}>
          <p style={{ color: 'var(--text-secondary)', marginBottom: '0.5rem', fontSize: '0.875rem', borderBottom: '1px solid rgba(255,255,255,0.1)', paddingBottom: '0.5rem' }}>
            Time Window: <span style={{color: 'white'}}>{label}</span>
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: '0.5rem', fontSize: '0.875rem' }}>
            <span style={{ color: '#94a3b8' }}>Trend Score:</span>
            <span style={{ color: '#4ade80', fontWeight: '600' }}>{data.score.toFixed(1)}</span>
            
            <span style={{ color: '#94a3b8' }}>Total Views:</span>
            <span style={{ color: 'white', fontWeight: '500' }}>{data.total_views || 0}</span>
            
            <span style={{ color: '#94a3b8' }}>Total Carts:</span>
            <span style={{ color: 'white', fontWeight: '500' }}>{data.total_carts || 0}</span>
            
            <span style={{ color: '#94a3b8' }}>Purchases:</span>
            <span style={{ color: 'white', fontWeight: '500' }}>{data.total_purchases || 0}</span>

            <span style={{ color: '#94a3b8' }}>Growth:</span>
            <span style={{ color: data.growth_rate >= 1 ? '#4ade80' : '#f87171', fontWeight: '500' }}>
              {(data.growth_rate * 100).toFixed(0)}%
            </span>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="glass-panel">
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1.5rem' }}>
        <TrendingUp size={20} color="var(--accent-secondary)" />
        <h3 style={{ fontSize: '1.25rem' }}>Product Trend Analytics</h3>
      </div>
      
      <div className="chart-container">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={data}
            margin={{ top: 10, right: 10, left: -20, bottom: 0 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
            <XAxis 
              dataKey="window" 
              stroke="var(--text-secondary)" 
              fontSize={12}
              tickLine={false}
              axisLine={false}
            />
            <YAxis 
              stroke="var(--text-secondary)" 
              fontSize={12}
              tickLine={false}
              axisLine={false}
            />
            <Tooltip content={<CustomTooltip />} />
            <Line 
              type="monotone" 
              dataKey="score" 
              stroke="url(#colorGradient)" 
              strokeWidth={3}
              dot={{ r: 4, fill: 'var(--bg-surface)', stroke: 'var(--accent-secondary)', strokeWidth: 2 }}
              activeDot={{ r: 6, fill: 'var(--accent-primary)', stroke: '#fff' }}
            />
            <defs>
              <linearGradient id="colorGradient" x1="0" y1="0" x2="1" y2="0">
                <stop offset="0%" stopColor="var(--accent-primary)" />
                <stop offset="100%" stopColor="var(--accent-secondary)" />
              </linearGradient>
            </defs>
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
