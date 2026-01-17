import React from 'react';

interface Column<T> {
  key: keyof T | string;
  header: string;
  render?: (item: T) => React.ReactNode;
  width?: string | number;
  align?: 'left' | 'center' | 'right';
}

interface TableProps<T> {
  columns: Column<T>[];
  data: T[];
  keyExtractor: (item: T) => string;
  onRowClick?: (item: T) => void;
  emptyMessage?: string;
  striped?: boolean;
}

export function Table<T>({
  columns,
  data,
  keyExtractor,
  onRowClick,
  emptyMessage = 'No data available',
  striped = true,
}: TableProps<T>) {
  return (
    <div style={{
      overflowX: 'auto',
      border: '2px solid var(--border-color)',
      borderRadius: 'var(--radius-md)',
    }}>
      <table style={{
        width: '100%',
        borderCollapse: 'collapse',
        fontFamily: 'var(--font-sans)',
      }}>
        <thead>
          <tr style={{ backgroundColor: 'var(--secondary-color)' }}>
            {columns.map((col) => (
              <th
                key={String(col.key)}
                style={{
                  padding: 'var(--space-md)',
                  textAlign: col.align || 'left',
                  fontWeight: 600,
                  fontSize: '0.9rem',
                  color: 'var(--surface-light)',
                  width: col.width,
                  whiteSpace: 'nowrap',
                }}
              >
                {col.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.length === 0 ? (
            <tr>
              <td
                colSpan={columns.length}
                style={{
                  padding: 'var(--space-xl)',
                  textAlign: 'center',
                  color: 'var(--text-color)',
                  opacity: 0.6,
                }}
              >
                {emptyMessage}
              </td>
            </tr>
          ) : (
            data.map((item, index) => (
              <tr
                key={keyExtractor(item)}
                onClick={() => onRowClick?.(item)}
                style={{
                  backgroundColor: striped && index % 2 === 1 ? 'var(--secondary-color)' : 'transparent',
                  cursor: onRowClick ? 'pointer' : 'default',
                  transition: 'background-color var(--transition-fast)',
                }}
                onMouseEnter={(e) => {
                  if (onRowClick) {
                    e.currentTarget.style.backgroundColor = 'var(--accent-color)';
                  }
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = striped && index % 2 === 1 ? 'var(--secondary-color)' : 'transparent';
                }}
              >
                {columns.map((col) => (
                  <td
                    key={String(col.key)}
                    style={{
                      padding: 'var(--space-md)',
                      textAlign: col.align || 'left',
                      color: 'var(--text-color)',
                      borderBottom: '1px solid var(--border-color)',
                    }}
                  >
                    {col.render ? col.render(item) : String((item as Record<string, unknown>)[col.key as string] ?? '')}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}


