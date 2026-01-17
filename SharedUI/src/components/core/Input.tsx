import React from 'react';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
  hint?: string;
  icon?: React.ReactNode;
}

export const Input: React.FC<InputProps> = ({
  label,
  error,
  hint,
  icon,
  ...props
}) => {
  const hasError = !!error;

  return (
    <div style={{ marginBottom: 'var(--space-md)', width: '100%' }}>
      {label && (
        <label style={{
          display: 'block',
          marginBottom: 'var(--space-xs)',
          fontFamily: 'var(--font-sans)',
          fontSize: '0.9rem',
          fontWeight: 600,
          color: 'var(--text-color)',
        }}>
          {label}
        </label>
      )}
      
      <div style={{ position: 'relative' }}>
        {icon && (
          <span style={{
            position: 'absolute',
            left: 'var(--space-sm)',
            top: '50%',
            transform: 'translateY(-50%)',
            color: 'var(--text-color)',
            opacity: 0.5,
          }}>
            {icon}
          </span>
        )}
        
        <input
          {...props}
          style={{
            width: '100%',
            padding: icon ? 'var(--space-sm) var(--space-sm) var(--space-sm) var(--space-xl)' : 'var(--space-sm) var(--space-md)',
            borderRadius: 'var(--radius-md)',
            border: `2px solid ${hasError ? 'var(--danger-color)' : 'var(--border-color)'}`,
            fontFamily: 'var(--font-sans)',
            fontSize: '1rem',
            backgroundColor: 'var(--bg-color)',
            color: 'var(--text-color)',
            boxSizing: 'border-box',
            transition: 'border-color var(--transition-fast), box-shadow var(--transition-fast)',
            outline: 'none',
            ...props.style,
          }}
          onFocus={(e) => {
            e.currentTarget.style.borderColor = hasError ? 'var(--danger-color)' : 'var(--primary-color)';
            e.currentTarget.style.boxShadow = `0 0 0 3px ${hasError ? 'rgba(229, 62, 62, 0.2)' : 'rgba(26, 54, 93, 0.2)'}`;
            props.onFocus?.(e);
          }}
          onBlur={(e) => {
            e.currentTarget.style.borderColor = hasError ? 'var(--danger-color)' : 'var(--border-color)';
            e.currentTarget.style.boxShadow = 'none';
            props.onBlur?.(e);
          }}
        />
      </div>

      {(error || hint) && (
        <p style={{
          margin: 'var(--space-xs) 0 0',
          fontFamily: 'var(--font-sans)',
          fontSize: '0.8rem',
          color: error ? 'var(--danger-color)' : 'var(--text-color)',
          opacity: error ? 1 : 0.6,
        }}>
          {error || hint}
        </p>
      )}
    </div>
  );
};


