import React from 'react';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'outline' | 'ghost' | 'danger';
  size?: 'sm' | 'md' | 'lg';
  loading?: boolean;
  icon?: React.ReactNode;
  children: React.ReactNode;
}

export const Button: React.FC<ButtonProps> = ({
  variant = 'primary',
  size = 'md',
  loading = false,
  icon,
  children,
  disabled,
  ...props
}) => {
  const sizeStyles = {
    sm: { padding: 'var(--space-xs) var(--space-sm)', fontSize: '0.85rem' },
    md: { padding: 'var(--space-sm) var(--space-md)', fontSize: '1rem' },
    lg: { padding: 'var(--space-md) var(--space-lg)', fontSize: '1.1rem' },
  };

  const variantStyles = {
    primary: {
      backgroundColor: 'var(--primary-color)',
      color: 'var(--surface-light)',
      border: '2px solid var(--primary-color)',
      boxShadow: '2px 2px 0 var(--secondary-color)',
    },
    secondary: {
      backgroundColor: 'var(--secondary-color)',
      color: 'var(--surface-light)',
      border: '2px solid var(--secondary-color)',
      boxShadow: '2px 2px 0 var(--primary-color)',
    },
    outline: {
      backgroundColor: 'transparent',
      color: 'var(--primary-color)',
      border: '2px solid var(--primary-color)',
      boxShadow: 'none',
    },
    ghost: {
      backgroundColor: 'transparent',
      color: 'var(--text-color)',
      border: '2px solid transparent',
      boxShadow: 'none',
    },
    danger: {
      backgroundColor: 'var(--danger-color)',
      color: 'var(--surface-light)',
      border: '2px solid var(--danger-color)',
      boxShadow: '2px 2px 0 rgba(0,0,0,0.2)',
    },
  };

  const baseStyle: React.CSSProperties = {
    ...sizeStyles[size],
    ...variantStyles[variant],
    borderRadius: 'var(--radius-md)',
    fontFamily: 'var(--font-sans)',
    fontWeight: 600,
    cursor: disabled || loading ? 'not-allowed' : 'pointer',
    opacity: disabled ? 0.5 : 1,
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 'var(--space-sm)',
    transition: 'transform var(--transition-fast), box-shadow var(--transition-fast)',
    outline: 'none',
  };

  return (
    <button
      {...props}
      disabled={disabled || loading}
      style={{ ...baseStyle, ...props.style }}
      onMouseDown={(e) => {
        if (!disabled && !loading) {
          e.currentTarget.style.transform = 'translate(2px, 2px)';
          e.currentTarget.style.boxShadow = 'none';
        }
        props.onMouseDown?.(e);
      }}
      onMouseUp={(e) => {
        e.currentTarget.style.transform = 'translate(0, 0)';
        e.currentTarget.style.boxShadow = variantStyles[variant].boxShadow || 'none';
        props.onMouseUp?.(e);
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.transform = 'translate(0, 0)';
        e.currentTarget.style.boxShadow = variantStyles[variant].boxShadow || 'none';
        props.onMouseLeave?.(e);
      }}
    >
      {loading ? <Spinner size={16} /> : icon}
      {children}
    </button>
  );
};

const Spinner: React.FC<{ size: number }> = ({ size }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" style={{ animation: 'spin 0.8s linear infinite' }}>
    <circle
      cx="12" cy="12" r="10"
      fill="none"
      stroke="currentColor"
      strokeWidth="3"
      strokeDasharray="31.4 31.4"
      strokeLinecap="round"
    />
    <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
  </svg>
);


