import React, { useState, useEffect } from 'react';
import type { BrandConfig, NavItem } from '../../types';

interface HeaderProps {
  brand: BrandConfig;
  darkMode: boolean;
  onThemeToggle: () => void;
  onAccountClick?: () => void;
  onNotificationClick?: () => void;
}

export const Header: React.FC<HeaderProps> = ({
  brand,
  darkMode,
  onThemeToggle,
  onAccountClick,
  onNotificationClick,
}) => {
  const [isMobile, setIsMobile] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [activeDropdown, setActiveDropdown] = useState<string | null>(null);

  useEffect(() => {
    const checkMobile = () => setIsMobile(window.innerWidth < 768);
    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  const visibleNavItems = brand.navItems.filter(item => item.inNavbar !== false);

  return (
    <header style={{
      backgroundColor: 'var(--primary-color)',
      borderBottom: '4px solid var(--accent-color)',
      position: 'sticky',
      top: 0,
      zIndex: 100,
    }}>
      {/* Single Row Header */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        padding: 'var(--space-sm) var(--space-lg)',
        gap: 'var(--space-md)',
        maxWidth: 1400,
        margin: '0 auto',
      }}>
        {/* Left: Logo */}
        {brand.logoUrl && (
          <div style={{
            width: 40,
            height: 40,
            borderRadius: 'var(--radius-md)',
            overflow: 'hidden',
            backgroundColor: 'var(--bg-color)',
            border: '2px solid var(--accent-color)',
            flexShrink: 0,
          }}>
            <img 
              src={brand.logoUrl} 
              alt={brand.name} 
              style={{ width: '100%', height: '100%', objectFit: 'cover' }}
            />
          </div>
        )}

        {/* Title */}
        <h1 style={{
          margin: 0,
          fontFamily: 'var(--font-serif)',
          fontSize: isMobile ? '1.1rem' : '1.4rem',
          fontWeight: 700,
          color: 'var(--surface-light)',
          whiteSpace: 'nowrap',
          flexShrink: 0,
        }}>
          {brand.name}
        </h1>

        {/* Center: Navigation (desktop) */}
        {!isMobile && (
          <nav style={{
            display: 'flex',
            alignItems: 'center',
            gap: 'var(--space-sm)',
            flex: 1,
            justifyContent: 'center',
          }}>
            {visibleNavItems.map((item) => (
              <NavItemButton
                key={item.label}
                item={item}
                isActive={activeDropdown === item.label}
                onHover={(label) => setActiveDropdown(label)}
                onLeave={() => setActiveDropdown(null)}
              />
            ))}
          </nav>
        )}

        {/* Mobile: Spacer */}
        {isMobile && <div style={{ flex: 1 }} />}

        {/* Right: Icons */}
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 'var(--space-xs)',
          flexShrink: 0,
        }}>
          {/* Mobile hamburger */}
          {isMobile && (
            <IconButton onClick={() => setMenuOpen(!menuOpen)} label="Menu" size={36}>
              <MenuIcon />
            </IconButton>
          )}

          {/* Theme Toggle */}
          <IconButton onClick={onThemeToggle} label="Toggle theme" size={36}>
            {darkMode ? <MoonIcon /> : <SunIcon />}
          </IconButton>

          {/* Notifications (desktop only, when callback provided) */}
          {!isMobile && onNotificationClick && (
            <IconButton onClick={onNotificationClick} label="Notifications" size={36}>
              <BellIcon />
            </IconButton>
          )}

          {/* Account */}
          <IconButton onClick={onAccountClick} label="Account" size={40}>
            <UserIcon />
          </IconButton>
        </div>
      </div>

      {/* Mobile menu dropdown */}
      {isMobile && menuOpen && (
        <div style={{
          backgroundColor: 'var(--bg-color)',
          borderTop: '1px solid var(--border-color)',
          padding: 'var(--space-sm)',
        }}>
          {visibleNavItems.map((item) => (
            <MobileNavItem key={item.label} item={item} onClose={() => setMenuOpen(false)} />
          ))}
        </div>
      )}
    </header>
  );
};

// === Sub-components ===

const IconButton: React.FC<{
  onClick?: () => void;
  label: string;
  size?: number;
  children: React.ReactNode;
}> = ({ onClick, label, size = 36, children }) => (
  <button
    onClick={onClick}
    aria-label={label}
    style={{
      width: size,
      height: size,
      borderRadius: 'var(--radius-full)',
      backgroundColor: 'rgba(255,255,255,0.1)',
      border: 'none',
      cursor: 'pointer',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      transition: 'background var(--transition-fast)',
      color: 'var(--surface-light)',
    }}
    onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'rgba(255,255,255,0.2)'}
    onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'rgba(255,255,255,0.1)'}
  >
    {children}
  </button>
);

const NavItemButton: React.FC<{
  item: NavItem;
  isActive: boolean;
  onHover: (label: string | null) => void;
  onLeave: () => void;
}> = ({ item, isActive, onHover, onLeave }) => (
  <div
    onMouseEnter={() => item.children && onHover(item.label)}
    onMouseLeave={onLeave}
    style={{ position: 'relative' }}
  >
    <button
      onClick={item.onClick}
      style={{
        background: 'none',
        border: 'none',
        color: 'var(--surface-light)',
        cursor: 'pointer',
        fontFamily: 'var(--font-sans)',
        fontWeight: 500,
        fontSize: '0.9rem',
        padding: 'var(--space-xs) var(--space-sm)',
        borderRadius: 'var(--radius-sm)',
        transition: 'background var(--transition-fast)',
      }}
      onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'rgba(255,255,255,0.1)'}
      onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
    >
      {item.label} {item.children && '▾'}
    </button>

    {item.children && isActive && (
      <div style={{
        position: 'absolute',
        top: '100%',
        left: '50%',
        transform: 'translateX(-50%)',
        backgroundColor: 'var(--bg-color)',
        minWidth: 160,
        boxShadow: 'var(--shadow-lg)',
        borderRadius: 'var(--radius-md)',
        padding: 'var(--space-xs) 0',
        zIndex: 200,
        marginTop: 4,
      }}>
        {item.children.map((child) => (
          <button
            key={child.label}
            onClick={child.onClick}
            style={{
              display: 'block',
              width: '100%',
              padding: 'var(--space-sm) var(--space-md)',
              border: 'none',
              background: 'none',
              textAlign: 'left',
              cursor: 'pointer',
              fontFamily: 'var(--font-sans)',
              fontSize: '0.9rem',
              color: 'var(--text-color)',
              transition: 'background var(--transition-fast)',
            }}
            onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--secondary-color)'}
            onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
          >
            {child.label}
          </button>
        ))}
      </div>
    )}
  </div>
);

const MobileNavItem: React.FC<{ item: NavItem; onClose: () => void }> = ({ item, onClose }) => (
  <button
    onClick={() => { item.onClick?.(); onClose(); }}
    style={{
      display: 'block',
      width: '100%',
      padding: 'var(--space-sm) var(--space-md)',
      border: 'none',
      background: 'none',
      textAlign: 'left',
      cursor: 'pointer',
      fontFamily: 'var(--font-sans)',
      fontWeight: 500,
      fontSize: '0.95rem',
      color: 'var(--text-color)',
      borderRadius: 'var(--radius-sm)',
    }}
  >
    {item.label}
  </button>
);

// === Icons ===
const SunIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="5" />
    <path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42" />
  </svg>
);

const MoonIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
  </svg>
);

const BellIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9M13.73 21a2 2 0 0 1-3.46 0" />
  </svg>
);

const UserIcon = () => (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
    <circle cx="12" cy="7" r="4" />
  </svg>
);

const MenuIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M3 12h18M3 6h18M3 18h18" />
  </svg>
);
