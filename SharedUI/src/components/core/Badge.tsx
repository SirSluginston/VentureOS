import React from 'react';

interface BadgeProps {
  children: React.ReactNode;
  variant?: 'default' | 'primary' | 'secondary' | 'success' | 'warning' | 'danger' | 'outline';
  size?: 'sm' | 'md';
}

export const Badge: React.FC<BadgeProps> = ({
  children,
  variant = 'default',
  size = 'md',
}) => {
  const sizeStyles = {
    sm: { padding: '2px var(--space-xs)', fontSize: '0.7rem' },
    md: { padding: 'var(--space-xs) var(--space-sm)', fontSize: '0.8rem' },
  };

  const variantStyles: Record<string, React.CSSProperties> = {
    default: {
      backgroundColor: 'var(--secondary-color)',
      color: 'var(--surface-light)',
    },
    primary: {
      backgroundColor: 'var(--primary-color)',
      color: 'var(--surface-light)',
    },
    secondary: {
      backgroundColor: 'var(--accent-color)',
      color: 'var(--primary-color)',
    },
    success: {
      backgroundColor: 'var(--success-color)',
      color: 'var(--surface-light)',
    },
    warning: {
      backgroundColor: 'var(--warning-color)',
      color: 'var(--surface-dark)',
    },
    danger: {
      backgroundColor: 'var(--danger-color)',
      color: 'var(--surface-light)',
    },
    outline: {
      backgroundColor: 'transparent',
      color: 'var(--primary-color)',
      border: '1px solid var(--primary-color)',
    },
  };

  return (
    <span
      style={{
        ...sizeStyles[size],
        ...variantStyles[variant],
        borderRadius: 'var(--radius-full)',
        fontFamily: 'var(--font-sans)',
        fontWeight: 600,
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        whiteSpace: 'nowrap',
        lineHeight: 1,
        textTransform: 'uppercase',
        letterSpacing: '0.03em',
      }}
    >
      {children}
    </span>
  );
};


