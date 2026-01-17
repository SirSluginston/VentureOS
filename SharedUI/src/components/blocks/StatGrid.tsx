import React from 'react';
import type { EntityStats } from '../../types';

interface StatGridProps {
  stats: EntityStats;
  columns?: 2 | 3 | 4;
}

// SVG Icons for stats
const Icons = {
  violations: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--warning-color)" strokeWidth="2">
      <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
      <line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
    </svg>
  ),
  injuries: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--info-color)" strokeWidth="2">
      <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
    </svg>
  ),
  fatalities: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--danger-color)" strokeWidth="2">
      <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
    </svg>
  ),
  fines: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--success-color)" strokeWidth="2">
      <line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>
    </svg>
  ),
  avgFine: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--secondary-color)" strokeWidth="2">
      <line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>
    </svg>
  ),
  trendUp: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--danger-color)" strokeWidth="2">
      <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>
    </svg>
  ),
  trendDown: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--success-color)" strokeWidth="2">
      <polyline points="23 18 13.5 8.5 8.5 13.5 1 6"/><polyline points="17 18 23 18 23 12"/>
    </svg>
  ),
};

export const StatGrid: React.FC<StatGridProps> = ({ 
  stats, 
  columns = 4 
}) => {
  const statItems = [
    { label: 'Total Incidents', value: stats.totalViolations, icon: Icons.violations },
    stats.totalInjuries !== undefined && { label: 'Injuries', value: stats.totalInjuries, icon: Icons.injuries },
    stats.totalFatalities !== undefined && { label: 'Fatalities', value: stats.totalFatalities, icon: Icons.fatalities },
    stats.totalFines !== undefined && { label: 'Total Fines', value: formatCurrency(stats.totalFines), icon: Icons.fines },
    stats.avgFines !== undefined && { label: 'Avg Fine', value: formatCurrency(stats.avgFines), icon: Icons.avgFine },
    stats.trend && { 
      label: 'YoY Change', 
      value: `${stats.trend.direction === 'up' ? '↑' : stats.trend.direction === 'down' ? '↓' : '→'} ${stats.trend.percentage}%`,
      icon: stats.trend.direction === 'down' ? Icons.trendDown : Icons.trendUp
    },
  ].filter(Boolean) as { label: string; value: string | number; icon: React.ReactNode }[];

  return (
    <section style={{ marginBottom: 'var(--space-lg)' }}>
      <div 
        className="stat-grid"
        style={{
          display: 'grid',
          gridTemplateColumns: `repeat(${columns}, 1fr)`,
          gap: 'var(--space-md)',
        }}
      >
        {statItems.map((stat) => (
          <StatCard key={stat.label} {...stat} />
        ))}
      </div>
    </section>
  );
};

const StatCard: React.FC<{ label: string; value: string | number; icon: React.ReactNode }> = ({ 
  label, 
  value, 
  icon 
}) => (
  <div style={{
    padding: 'var(--space-lg)',
    borderRadius: 'var(--radius-md)',
    backgroundColor: 'var(--bg-color)',
    border: '2px solid var(--border-color)',
    display: 'flex',
    alignItems: 'center',
    gap: 'var(--space-md)',
    transition: 'transform var(--transition-fast), box-shadow var(--transition-fast)',
    cursor: 'default',
  }}
  onMouseEnter={(e) => {
    e.currentTarget.style.transform = 'translateY(-2px)';
    e.currentTarget.style.boxShadow = 'var(--shadow-md)';
  }}
  onMouseLeave={(e) => {
    e.currentTarget.style.transform = 'translateY(0)';
    e.currentTarget.style.boxShadow = 'none';
  }}
  >
    <span style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>{icon}</span>
    <div>
      <div style={{
        fontFamily: 'var(--font-sans)',
        fontWeight: 700,
        fontSize: '1.5rem',
        color: 'var(--primary-color)',
      }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
      </div>
      <div style={{
        fontFamily: 'var(--font-sans)',
        fontSize: '0.9rem',
        color: 'var(--text-color)',
        opacity: 0.8,
      }}>
        {label}
      </div>
    </div>
  </div>
);

function formatCurrency(value: number): string {
  if (value >= 1_000_000) {
    return `$${(value / 1_000_000).toFixed(1)}M`;
  }
  if (value >= 1_000) {
    return `$${(value / 1_000).toFixed(0)}K`;
  }
  return `$${value.toLocaleString()}`;
}

