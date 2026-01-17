import React from 'react';

interface HeroProps {
  title: string;
  subtitle?: string;
  breadcrumbs?: { label: string; path?: string }[];
  score?: {
    value: number;
    label: string;
    betaDisclaimer?: boolean;
  };
}

export const Hero: React.FC<HeroProps> = ({
  title,
  subtitle,
  breadcrumbs,
  score,
}) => {
  return (
    <section style={{
      backgroundColor: 'var(--secondary-color)',
      padding: 'var(--space-lg) var(--space-lg)',
      textAlign: 'center',
      borderBottom: '4px solid var(--accent-color)',
    }}>
      {/* Breadcrumbs */}
      {breadcrumbs && breadcrumbs.length > 0 && (
        <nav style={{
          marginBottom: 'var(--space-md)',
          display: 'flex',
          justifyContent: 'center',
          gap: 'var(--space-sm)',
          fontFamily: 'var(--font-sans)',
          fontSize: '0.9rem',
          fontWeight: 600,
        }}>
          {breadcrumbs.map((crumb, index) => (
            <React.Fragment key={crumb.label}>
              {index > 0 && (
                <span style={{ color: 'var(--surface-light)', opacity: 0.4 }}>
                  /
                </span>
              )}
              {crumb.path ? (
                <a
                  href={crumb.path}
                  style={{
                    color: 'var(--accent-color)',
                    textDecoration: 'none',
                  }}
                >
                  {crumb.label}
                </a>
              ) : (
                <span style={{ color: 'var(--surface-light)', opacity: 0.8 }}>
                  {crumb.label}
                </span>
              )}
            </React.Fragment>
          ))}
        </nav>
      )}

      {/* Title */}
      <h1 style={{
        margin: 0,
        fontFamily: 'var(--font-serif)',
        fontSize: 'clamp(2rem, 5vw, 3.5rem)',
        fontWeight: 700,
        color: 'var(--text-dark)',
        letterSpacing: '0.02em',
      }}>
        {title}
      </h1>

      {/* Subtitle */}
      {subtitle && (
        <p style={{
          margin: 'var(--space-sm) 0 0',
          fontFamily: 'var(--font-sans)',
          fontSize: '1.1rem',
          color: 'var(--text-dark)',
          opacity: 0.8,
        }}>
          {subtitle}
        </p>
      )}

      {/* Score Badge */}
      {score && (
        <div style={{
          marginTop: 'var(--space-md)',
          display: 'inline-flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 'var(--space-xs)',
        }}>
          <div style={{
            backgroundColor: 'var(--accent-color)',
            color: 'white',
            padding: 'var(--space-sm) var(--space-lg)',
            borderRadius: 'var(--radius-full)',
            fontFamily: 'var(--font-sans)',
            fontWeight: 700,
            fontSize: '1.5rem',
            display: 'flex',
            alignItems: 'center',
            gap: 'var(--space-sm)',
            boxShadow: 'var(--shadow-md)',
          }}>
            <span>{score.value}</span>
            <span style={{ fontSize: '0.9rem', fontWeight: 500 }}>
              {score.label}
            </span>
          </div>
          {score.betaDisclaimer && (
            <span style={{
              fontFamily: 'var(--font-sans)',
              fontSize: '0.75rem',
              color: 'var(--text-dark)',
              opacity: 0.6,
              fontStyle: 'italic',
            }}>
              Score algorithm is a work in progress
            </span>
          )}
        </div>
      )}
    </section>
  );
};

