import React from 'react';

interface AlertProps {
  children: React.ReactNode;
  variant?: 'info' | 'success' | 'warning' | 'danger';
  title?: string;
  dismissible?: boolean;
  onDismiss?: () => void;
}

export const Alert: React.FC<AlertProps> = ({
  children,
  variant = 'info',
  title,
  dismissible = false,
  onDismiss,
}) => {
  const variantStyles = {
    info: {
      backgroundColor: 'rgba(49, 130, 206, 0.1)',
      borderColor: 'var(--info-color)',
      iconColor: 'var(--info-color)',
      icon: 'ℹ️',
    },
    success: {
      backgroundColor: 'rgba(56, 161, 105, 0.1)',
      borderColor: 'var(--success-color)',
      iconColor: 'var(--success-color)',
      icon: '✓',
    },
    warning: {
      backgroundColor: 'rgba(214, 158, 46, 0.1)',
      borderColor: 'var(--warning-color)',
      iconColor: 'var(--warning-color)',
      icon: '⚠',
    },
    danger: {
      backgroundColor: 'rgba(229, 62, 62, 0.1)',
      borderColor: 'var(--danger-color)',
      iconColor: 'var(--danger-color)',
      icon: '✕',
    },
  };

  const style = variantStyles[variant];

  return (
    <div
      role="alert"
      style={{
        backgroundColor: style.backgroundColor,
        borderLeft: `4px solid ${style.borderColor}`,
        borderRadius: 'var(--radius-md)',
        padding: 'var(--space-md) var(--space-lg)',
        display: 'flex',
        alignItems: 'flex-start',
        gap: 'var(--space-md)',
        margin: 'var(--space-md) 0',
      }}
    >
      <span style={{
        fontSize: '1.2rem',
        lineHeight: 1,
        flexShrink: 0,
      }}>
        {style.icon}
      </span>
      
      <div style={{ flex: 1 }}>
        {title && (
          <h4 style={{
            margin: '0 0 var(--space-xs)',
            fontFamily: 'var(--font-sans)',
            fontWeight: 600,
            fontSize: '1rem',
            color: 'var(--text-color)',
          }}>
            {title}
          </h4>
        )}
        <div style={{
          fontFamily: 'var(--font-sans)',
          fontSize: '0.95rem',
          color: 'var(--text-color)',
          lineHeight: 1.5,
        }}>
          {children}
        </div>
      </div>

      {dismissible && (
        <button
          onClick={onDismiss}
          aria-label="Dismiss"
          style={{
            background: 'none',
            border: 'none',
            cursor: 'pointer',
            padding: 'var(--space-xs)',
            color: 'var(--text-color)',
            opacity: 0.5,
            transition: 'opacity var(--transition-fast)',
            flexShrink: 0,
          }}
          onMouseEnter={(e) => e.currentTarget.style.opacity = '1'}
          onMouseLeave={(e) => e.currentTarget.style.opacity = '0.5'}
        >
          ✕
        </button>
      )}
    </div>
  );
};


