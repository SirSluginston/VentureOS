import React from 'react';

interface AdSlotProps {
  position: 'sidebar' | 'left-sidebar' | 'banner' | 'inline';
  width?: number | string;
  height?: number | string;
}

export const AdSlot: React.FC<AdSlotProps> = ({
  position,
  width,
  height,
}) => {
  const dimensions: Record<string, { width: number | string; height: number | string }> = {
    'sidebar': { width: width ?? 280, height: height ?? 250 },
    'left-sidebar': { width: width ?? '100%', height: height ?? 200 },
    'banner': { width: width ?? '100%', height: height ?? 90 },
    'inline': { width: width ?? '100%', height: height ?? 120 },
  };

  const { width: w, height: h } = dimensions[position];

  return (
    <div
      data-ad-slot={position}
      style={{
        width: w,
        height: h,
        backgroundColor: 'var(--secondary-color)',
        border: '2px dashed var(--border-color)',
        borderRadius: 'var(--radius-md)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontFamily: 'var(--font-sans)',
        fontSize: '0.85rem',
        color: 'var(--text-color)',
        opacity: 0.5,
      }}
    >
      Ad Placeholder ({position})
    </div>
  );
};

