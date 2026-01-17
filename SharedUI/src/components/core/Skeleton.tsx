import React from 'react';

interface SkeletonProps {
  width?: string | number;
  height?: string | number;
  variant?: 'text' | 'rect' | 'circle';
  animation?: 'pulse' | 'wave' | 'none';
}

export const Skeleton: React.FC<SkeletonProps> = ({
  width = '100%',
  height = 20,
  variant = 'text',
  animation = 'wave',
}) => {
  const variantStyles: Record<string, React.CSSProperties> = {
    text: { borderRadius: 'var(--radius-sm)' },
    rect: { borderRadius: 'var(--radius-md)' },
    circle: { borderRadius: '50%' },
  };

  const animationStyles = {
    pulse: {
      animation: 'skeletonPulse 1.5s ease-in-out infinite',
    },
    wave: {
      backgroundImage: 'linear-gradient(90deg, var(--secondary-color) 0%, var(--bg-color) 50%, var(--secondary-color) 100%)',
      backgroundSize: '200% 100%',
      animation: 'skeletonWave 1.5s ease-in-out infinite',
    },
    none: {},
  };

  return (
    <div
      style={{
        width,
        height,
        backgroundColor: animation === 'wave' ? undefined : 'var(--secondary-color)',
        ...variantStyles[variant],
        ...animationStyles[animation],
      }}
    >
      <style>{`
        @keyframes skeletonPulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
        @keyframes skeletonWave {
          0% { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }
      `}</style>
    </div>
  );
};

// Preset skeleton patterns
export const SkeletonText: React.FC<{ lines?: number }> = ({ lines = 3 }) => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-sm)' }}>
    {Array.from({ length: lines }).map((_, i) => (
      <Skeleton
        key={i}
        width={i === lines - 1 ? '60%' : '100%'}
        height={16}
      />
    ))}
  </div>
);

export const SkeletonCard: React.FC = () => (
  <div style={{
    padding: 'var(--space-lg)',
    border: '2px solid var(--border-color)',
    borderRadius: 'var(--radius-md)',
    display: 'flex',
    flexDirection: 'column',
    gap: 'var(--space-md)',
  }}>
    <Skeleton height={24} width="50%" />
    <SkeletonText lines={3} />
  </div>
);


