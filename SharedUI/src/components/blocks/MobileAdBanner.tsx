import React from 'react';

export const MobileAdBanner: React.FC = () => {
  return (
    <>
      <style>{`
        .mobile-ad-banner {
          display: none;
        }
        @media (max-width: 1200px) {
          .mobile-ad-banner {
            display: block;
            position: sticky;
            bottom: 0;
            left: 0;
            right: 0;
            padding: 8px;
            background-color: var(--bg-color);
            border-top: 2px solid var(--border-color);
            text-align: center;
            margin-top: auto;
          }
        }
      `}</style>
      <div className="mobile-ad-banner">
        <div
          data-ad-slot="mobile-banner"
          style={{
            width: '100%',
            maxWidth: 468,
            height: 60,
            margin: '0 auto',
            backgroundColor: 'var(--secondary-color)',
            border: '2px dashed var(--border-color)',
            borderRadius: '4px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontFamily: 'var(--font-sans)',
            fontSize: '0.8rem',
            color: 'var(--text-color)',
            opacity: 0.6,
          }}
        >
          Ad (468×60)
        </div>
      </div>
    </>
  );
};

