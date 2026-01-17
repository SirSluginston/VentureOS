import React from 'react';
import { AdSlot } from './AdSlot';
import type { EntityMeta } from '../../types';

interface InfoSidebarProps {
  meta?: EntityMeta;
  showAd?: boolean;
}

export const InfoSidebar: React.FC<InfoSidebarProps> = ({
  meta,
  showAd = true,
}) => {
  if (!meta && !showAd) return null;

  const infoItems = meta ? [
    meta.population && { label: 'Population', value: meta.population.toLocaleString() },
    meta.industry && { label: 'Industry', value: meta.industry },
    meta.founded && { label: 'Founded', value: meta.founded },
    meta.headquarters && { label: 'HQ', value: meta.headquarters },
  ].filter(Boolean) as { label: string; value: string | number }[] : [];

  return (
    <aside className="page-sidebar" style={{
      display: 'flex',
      flexDirection: 'column',
      gap: 'var(--space-md)',
    }}>
      {/* Entity Info Card */}
      {infoItems.length > 0 && (
        <div style={{
          padding: 'var(--space-lg)',
          backgroundColor: 'var(--bg-color)',
          border: '2px solid var(--border-color)',
          borderRadius: 'var(--radius-md)',
        }}>
          {/* Logo if available */}
          {meta?.logoUrl && (
            <div style={{
              width: 80,
              height: 80,
              margin: '0 auto var(--space-md)',
              borderRadius: 'var(--radius-md)',
              overflow: 'hidden',
              backgroundColor: 'white',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}>
              <img
                src={meta.logoUrl}
                alt=""
                style={{ maxWidth: '100%', maxHeight: '100%' }}
              />
            </div>
          )}

          {/* Flag if available */}
          {meta?.flagUrl && (
            <div style={{
              width: '100%',
              margin: '0 auto var(--space-md)',
              borderRadius: 'var(--radius-sm)',
              overflow: 'hidden',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              border: '1px solid var(--border-color)',
            }}>
              <img
                src={meta.flagUrl}
                alt=""
                style={{ width: '100%', height: 'auto', display: 'block' }}
              />
            </div>
          )}

          {/* Info Items - stacked layout */}
          <dl style={{ margin: 0 }}>
            {infoItems.map(({ label, value }) => (
              <div
                key={label}
                style={{
                  padding: 'var(--space-sm) 0',
                  borderBottom: '1px solid var(--border-color)',
                }}
              >
                <dt style={{
                  fontFamily: 'var(--font-sans)',
                  fontSize: '0.75rem',
                  color: 'var(--text-color)',
                  opacity: 0.6,
                  textTransform: 'uppercase',
                  letterSpacing: '0.5px',
                  marginBottom: '2px',
                }}>
                  {label}
                </dt>
                <dd style={{
                  margin: 0,
                  fontFamily: 'var(--font-sans)',
                  fontSize: '0.9rem',
                  fontWeight: 600,
                  color: 'var(--text-color)',
                }}>
                  {value}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      )}

      {/* Ad Slot */}
      {showAd && <AdSlot position="sidebar" />}
    </aside>
  );
};

