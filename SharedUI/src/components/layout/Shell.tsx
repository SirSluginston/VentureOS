import React from 'react';
import { Header } from './Header';
import { Footer } from './Footer';
import { MobileAdBanner } from '../blocks/MobileAdBanner';
import { useTheme } from '../../hooks/useTheme';
import type { BrandConfig } from '../../types';

interface ShellProps {
  brand: BrandConfig;
  children: React.ReactNode;
  onAccountClick?: () => void;
  onNotificationClick?: () => void;
}

export const Shell: React.FC<ShellProps> = ({
  brand,
  children,
  onAccountClick,
  onNotificationClick,
}) => {
  const { darkMode, toggleDarkMode } = useTheme(brand);

  return (
    <div className="shell" style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <Header
        brand={brand}
        darkMode={darkMode}
        onThemeToggle={toggleDarkMode}
        onAccountClick={onAccountClick}
        onNotificationClick={onNotificationClick}
      />
      
      {/* Content wrapper - ad banner sticks to bottom of THIS, not viewport */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        <main style={{
          flex: 1,
          backgroundColor: 'var(--bg-color)',
        }}>
          {children}
        </main>
        
        {/* Mobile-only sticky ad banner - inside content wrapper so it stops at footer */}
        <MobileAdBanner />
      </div>
      
      <Footer
        copyrightBrand={brand.footer.copyrightBrand}
        yearCreated={brand.footer.yearCreated}
        poweredBy={brand.footer.poweredBy}
        links={brand.footer.links}
      />
    </div>
  );
};

