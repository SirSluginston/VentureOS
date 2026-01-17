import React from 'react';

interface SpinnerProps {
  size?: 'sm' | 'md' | 'lg';
  color?: string;
}

export const Spinner: React.FC<SpinnerProps> = ({
  size = 'md',
  color = 'var(--primary-color)',
}) => {
  const sizeMap = {
    sm: 20,
    md: 32,
    lg: 48,
  };

  const pixelSize = sizeMap[size];

  return (
    <svg
      width={pixelSize}
      height={pixelSize}
      viewBox="0 0 50 50"
      style={{ animation: 'spinnerRotate 0.8s linear infinite' }}
    >
      <circle
        cx="25"
        cy="25"
        r="20"
        fill="none"
        stroke={color}
        strokeWidth="4"
        strokeDasharray="80 40"
        strokeLinecap="round"
      />
      <style>{`
        @keyframes spinnerRotate {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </svg>
  );
};

// Full page loading overlay
interface LoadingOverlayProps {
  message?: string;
}

export const LoadingOverlay: React.FC<LoadingOverlayProps> = ({
  message = 'Loading...',
}) => (
  <div style={{
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 'var(--space-md)',
    zIndex: 1000,
  }}>
    <Spinner size="lg" color="var(--surface-light)" />
    <span style={{
      fontFamily: 'var(--font-sans)',
      fontSize: '1rem',
      color: 'var(--surface-light)',
    }}>
      {message}
    </span>
  </div>
);


