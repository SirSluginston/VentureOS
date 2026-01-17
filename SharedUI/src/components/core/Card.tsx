import React from 'react';

interface CardProps {
  title?: string;
  subtitle?: string;
  children: React.ReactNode;
  hoverable?: boolean;
  padding?: 'none' | 'sm' | 'md' | 'lg';
  className?: string;
  style?: React.CSSProperties;
  onClick?: () => void;
}

export const Card: React.FC<CardProps> = ({
  title,
  subtitle,
  children,
  hoverable = false,
  padding = 'lg',
  className,
  style,
  onClick,
}) => {
  const paddingMap = {
    none: '0',
    sm: 'var(--space-sm)',
    md: 'var(--space-md)',
    lg: 'var(--space-lg)',
  };

  return (
    <div
      className={className}
      onClick={onClick}
      style={{
        backgroundColor: 'var(--bg-color)',
        borderRadius: 'var(--radius-md)',
        padding: paddingMap[padding],
        border: '2px solid var(--border-color)',
        display: 'flex',
        flexDirection: 'column',
        gap: 'var(--space-md)',
        transition: 'transform var(--transition-fast), box-shadow var(--transition-fast)',
        cursor: onClick ? 'pointer' : 'default',
        ...style,
      }}
      onMouseEnter={(e) => {
        if (hoverable) {
          e.currentTarget.style.transform = 'translateY(-4px)';
          e.currentTarget.style.boxShadow = 'var(--shadow-lg)';
        }
      }}
      onMouseLeave={(e) => {
        if (hoverable) {
          e.currentTarget.style.transform = 'translateY(0)';
          e.currentTarget.style.boxShadow = 'none';
        }
      }}
    >
      {(title || subtitle) && (
        <div style={{ borderBottom: '1px solid var(--border-color)', paddingBottom: 'var(--space-sm)' }}>
          {title && (
            <h3 style={{
              margin: 0,
              fontFamily: 'var(--font-serif)',
              fontSize: '1.25rem',
              fontWeight: 700,
              color: 'var(--primary-color)',
            }}>
              {title}
            </h3>
          )}
          {subtitle && (
            <p style={{
              margin: 'var(--space-xs) 0 0',
              fontFamily: 'var(--font-sans)',
              fontSize: '0.9rem',
              color: 'var(--text-color)',
              opacity: 0.7,
            }}>
              {subtitle}
            </p>
          )}
        </div>
      )}
      <div style={{ fontFamily: 'var(--font-sans)', lineHeight: 1.6 }}>
        {children}
      </div>
    </div>
  );
};


