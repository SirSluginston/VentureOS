import React from 'react';
import { Header } from './Header';
import { Footer } from './Footer';
import { useTheme } from '../../hooks/useTheme';
import type { BrandConfig } from '../../types';

interface ShellProps {
  brand: BrandConfig;
  children: React.ReactNode;
}

export const Shell: React.FC<ShellProps> = ({
  brand,
  children,
}) => {
  const { darkMode, toggleDarkMode } = useTheme(brand);

  return (
    <div className="shell" style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <Header
        brand={brand}
        darkMode={darkMode}
        onThemeToggle={toggleDarkMode}
      />
      
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        <main style={{
          flex: 1,
          backgroundColor: 'var(--bg-color)',
        }}>
          {children}
        </main>
      </div>
      
      <Footer
        copyrightBrand={brand.footer.copyrightBrand}
        yearCreated={brand.footer.yearCreated}
        poweredBy={brand.footer.poweredBy}
      />
    </div>
  );
};
