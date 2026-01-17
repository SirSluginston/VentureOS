import React from 'react';
import type { FooterLink } from '../../types';

interface FooterProps {
  copyrightBrand: string;
  yearCreated: number;
  poweredBy?: string;
  links?: FooterLink[];
}

export const Footer: React.FC<FooterProps> = ({
  copyrightBrand,
  yearCreated,
  poweredBy,
  links,
}) => {
  const currentYear = new Date().getFullYear();
  const yearRange = yearCreated === currentYear 
    ? currentYear.toString() 
    : `${yearCreated}-${currentYear}`;

  return (
    <footer style={{
      backgroundColor: 'var(--primary-color)',
      color: 'var(--bg-color)',
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
        {/* Links */}
        {links && links.length > 0 && (
          <nav style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: 'var(--space-md)',
            justifyContent: 'center',
          }}>
            {links.map((link) => (
              <a
                key={link.label}
                href={link.url}
                style={{
                  color: 'var(--bg-color)',
                  textDecoration: 'none',
                  fontFamily: 'var(--font-sans)',
                  fontSize: '0.9rem',
                  opacity: 0.9,
                  transition: 'opacity var(--transition-fast)',
                }}
                onMouseEnter={(e) => e.currentTarget.style.opacity = '1'}
                onMouseLeave={(e) => e.currentTarget.style.opacity = '0.9'}
              >
                {link.label}
              </a>
            ))}
          </nav>
        )}

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

