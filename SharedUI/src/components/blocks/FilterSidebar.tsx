import React from 'react';
import { AdSlot } from './AdSlot';

interface FilterSidebarProps {
  showAd?: boolean;
}

export const FilterSidebar: React.FC<FilterSidebarProps> = ({
  showAd = true,
}) => {
  return (
    <aside className="page-filters" style={{
      display: 'flex',
      flexDirection: 'column',
      gap: 'var(--space-md)',
    }}>
      {/* Filters Card */}
      <div style={{
        padding: 'var(--space-md)',
        backgroundColor: 'var(--bg-color)',
        border: '2px solid var(--border-color)',
        borderRadius: 'var(--radius-md)',
      }}>
        <h3 style={{
          margin: '0 0 var(--space-md)',
          fontFamily: 'var(--font-serif)',
          fontSize: '1.1rem',
          color: 'var(--text-color)',
        }}>
          Filters
        </h3>

        {/* Date Range */}
        <div style={{ marginBottom: 'var(--space-md)' }}>
          <label style={{
            display: 'block',
            fontSize: '0.85rem',
            fontWeight: 500,
            marginBottom: 'var(--space-xs)',
            color: 'var(--text-color)',
          }}>
            Date Range
          </label>
          <select style={{
            width: '100%',
            padding: 'var(--space-sm)',
            borderRadius: 'var(--radius-sm)',
            border: '1px solid var(--border-color)',
            backgroundColor: 'var(--bg-color)',
            color: 'var(--text-color)',
            fontSize: '0.9rem',
          }}>
            <option>All Time</option>
            <option>Last Year</option>
            <option>Last 5 Years</option>
            <option>Last 10 Years</option>
          </select>
        </div>

        {/* Severity */}
        <div style={{ marginBottom: 'var(--space-md)' }}>
          <label style={{
            display: 'block',
            fontSize: '0.85rem',
            fontWeight: 500,
            marginBottom: 'var(--space-xs)',
            color: 'var(--text-color)',
          }}>
            Severity
          </label>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-xs)' }}>
            {['All', 'Fatalities', 'Hospitalizations', 'Amputations'].map((option) => (
              <label key={option} style={{
                display: 'flex',
                alignItems: 'center',
                gap: 'var(--space-xs)',
                fontSize: '0.85rem',
                cursor: 'pointer',
              }}>
                <input 
                  type="checkbox" 
                  defaultChecked={option === 'All'}
                  style={{ accentColor: 'var(--primary-color)' }}
                />
                {option}
              </label>
            ))}
          </div>
        </div>

        {/* Sort By */}
        <div>
          <label style={{
            display: 'block',
            fontSize: '0.85rem',
            fontWeight: 500,
            marginBottom: 'var(--space-xs)',
            color: 'var(--text-color)',
          }}>
            Sort By
          </label>
          <select style={{
            width: '100%',
            padding: 'var(--space-sm)',
            borderRadius: 'var(--radius-sm)',
            border: '1px solid var(--border-color)',
            backgroundColor: 'var(--bg-color)',
            color: 'var(--text-color)',
            fontSize: '0.9rem',
          }}>
            <option>Most Incidents</option>
            <option>Most Recent</option>
            <option>Highest Fines</option>
            <option>Alphabetical</option>
          </select>
        </div>
      </div>

      {/* Ad Slot */}
      {showAd && <AdSlot position="left-sidebar" />}
    </aside>
  );
};


