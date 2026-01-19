import React from 'react';

interface FooterProps {
  copyrightBrand: string;
  yearCreated: number;
  poweredBy?: string;
}

export const Footer: React.FC<FooterProps> = ({
  copyrightBrand,
  yearCreated,
  poweredBy,
}) => {
  const currentYear = new Date().getFullYear();
  const yearRange = yearCreated === currentYear 
    ? currentYear.toString() 
    : `${yearCreated}-${currentYear}`;

  return (
    <footer style={{
      backgroundColor: 'var(--primary-color)',
      color: 'var(--text-dark)',
      padding: 'var(--space-lg) var(--space-xl)',
      borderTop: '4px solid var(--accent-color)',
    }}>
      <div style={{
        maxWidth: 1200,
        margin: '0 auto',
        display: 'flex',
        flexDirection: 'column',
        gap: 'var(--space-md)',
        alignItems: 'center',
        textAlign: 'center',
      }}>

        {/* Copyright */}
        <p style={{
          margin: 0,
          fontFamily: 'var(--font-sans)',
          fontSize: '0.85rem',
          opacity: 0.8,
        }}>
          © {yearRange} {copyrightBrand}. All rights reserved.
        </p>

        {/* Powered By */}
        {poweredBy && (
          <p style={{
            margin: 0,
            fontFamily: 'var(--font-sans)',
            fontSize: '0.75rem',
            opacity: 0.6,
          }}>
            Powered by{' '}
            <span style={{ color: 'var(--org-color)', fontWeight: 600 }}>
              {poweredBy}
            </span>
          </p>
        )}
      </div>
    </footer>
  );
};

