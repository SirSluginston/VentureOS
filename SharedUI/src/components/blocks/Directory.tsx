import React, { useState } from 'react';
import type { DirectoryItem } from '../../types';

interface DirectoryProps {
  title: string;
  items: DirectoryItem[];
  basePath: string;           // e.g., "/tn" for cities in Tennessee
  emptyMessage?: string;
  showSearch?: boolean;
  initialLimit?: number;      // Show this many initially
}

export const Directory: React.FC<DirectoryProps> = ({
  title,
  items,
  basePath,
  emptyMessage = 'No items found',
  showSearch = true,
  initialLimit = 30,
}) => {
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState(false);
  
  const filteredItems = items.filter(item =>
    item.name.toLowerCase().includes(search.toLowerCase())
  );
  
  const showAll = expanded || search.length > 0; // Always show all when searching
  const displayItems = showAll ? filteredItems : filteredItems.slice(0, initialLimit);
  const hasMore = filteredItems.length > initialLimit && !showAll;

  return (
    <section style={{
      marginBottom: 'var(--space-lg)',
    }}>
      <div>
        {/* Header */}
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 'var(--space-md)',
          flexWrap: 'wrap',
          gap: 'var(--space-sm)',
        }}>
          <h2 style={{
            margin: 0,
            fontFamily: 'var(--font-serif)',
            fontSize: '1.5rem',
            color: 'var(--text-color)',
          }}>
            {title}
            <span style={{
              marginLeft: 'var(--space-sm)',
              fontSize: '1rem',
              opacity: 0.6,
            }}>
              ({items.length})
            </span>
          </h2>

          {showSearch && items.length > 10 && (
            <input
              type="text"
              placeholder="Search..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              style={{
                padding: 'var(--space-sm) var(--space-md)',
                borderRadius: 'var(--radius-md)',
                border: '2px solid var(--border-color)',
                fontFamily: 'var(--font-sans)',
                fontSize: '0.9rem',
                backgroundColor: 'var(--bg-color)',
                color: 'var(--text-color)',
                width: 200,
              }}
            />
          )}
        </div>

        {/* Grid of compact tags */}
        <div style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: 'var(--space-sm)',
          padding: 'var(--space-md)',
          border: '2px solid var(--border-color)',
          borderRadius: 'var(--radius-md)',
        }}>
          {filteredItems.length === 0 ? (
            <p style={{
              width: '100%',
              textAlign: 'center',
              color: 'var(--text-color)',
              opacity: 0.6,
              margin: 0,
              padding: 'var(--space-lg)',
            }}>
              {emptyMessage}
            </p>
          ) : (
            <>
              {displayItems.map((item) => (
                <a
                  key={item.slug}
                  href={`${basePath}/${item.slug}`}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 'var(--space-xs)',
                    padding: 'var(--space-xs) var(--space-sm)',
                    backgroundColor: 'var(--bg-color)',
                    border: '1px solid var(--border-color)',
                    borderRadius: 'var(--radius-sm)',
                    textDecoration: 'none',
                    color: 'var(--text-color)',
                    fontFamily: 'var(--font-sans)',
                    fontSize: '0.85rem',
                    transition: 'all var(--transition-fast)',
                    whiteSpace: 'nowrap',
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.backgroundColor = 'var(--primary-color)';
                    e.currentTarget.style.color = 'var(--surface-light)';
                    e.currentTarget.style.borderColor = 'var(--primary-color)';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.backgroundColor = 'var(--bg-color)';
                    e.currentTarget.style.color = 'var(--text-color)';
                    e.currentTarget.style.borderColor = 'var(--border-color)';
                  }}
                >
                  <span style={{ fontWeight: 500 }}>{item.name}</span>
                  <span 
                    title={`${item.count.toLocaleString()} incidents`}
                    style={{
                      backgroundColor: 'var(--secondary-color)',
                      color: 'var(--surface-light)',
                      padding: '2px 6px',
                      borderRadius: 'var(--radius-full)',
                      fontSize: '0.75rem',
                      fontWeight: 600,
                    }}
                  >
                    {item.count.toLocaleString()}
                  </span>
                </a>
              ))}
              
              {/* Show more / less toggle */}
              {(hasMore || expanded) && (
                <button
                  onClick={() => setExpanded(!expanded)}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 'var(--space-xs)',
                    padding: 'var(--space-xs) var(--space-sm)',
                    backgroundColor: 'var(--accent-color)',
                    border: 'none',
                    borderRadius: 'var(--radius-sm)',
                    color: 'var(--surface-light)',
                    fontFamily: 'var(--font-sans)',
                    fontSize: '0.85rem',
                    fontWeight: 500,
                    cursor: 'pointer',
                    transition: 'all var(--transition-fast)',
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.opacity = '0.8';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.opacity = '1';
                  }}
                >
                  {expanded 
                    ? 'Show less' 
                    : `+${filteredItems.length - initialLimit} more`
                  }
                </button>
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
};

