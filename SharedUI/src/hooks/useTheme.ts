import { useEffect, useState, useCallback } from 'react';
import type { BrandConfig, BrandTheme } from '../types';

export function useTheme(brandConfig: BrandConfig) {
  const [darkMode, setDarkMode] = useState(() => {
    if (typeof window === 'undefined') return false;
    // Check for saved preference, then brand default, then system preference
    const saved = localStorage.getItem('theme-mode');
    if (saved) return saved === 'dark';
    if (brandConfig.theme.defaultTheme === 'dark') return true;
    if (brandConfig.theme.defaultTheme === 'auto') {
      return window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    return false;
  });

  // Apply brand colors to CSS variables
  useEffect(() => {
    applyTheme(brandConfig.theme);
  }, [brandConfig.theme]);

  // Sync dark mode with body class and localStorage
  useEffect(() => {
    document.body.classList.toggle('dark-mode', darkMode);
    localStorage.setItem('theme-mode', darkMode ? 'dark' : 'light');
  }, [darkMode]);

  const toggleDarkMode = useCallback(() => {
    setDarkMode(prev => !prev);
  }, []);

  return { darkMode, toggleDarkMode };
}

/**
 * Apply theme colors to CSS variables
 * Can be called standalone (e.g., from SSR/static shell) or via useTheme hook
 */
export function applyTheme(theme: BrandTheme) {
  const root = document.documentElement;
  
  // Required colors
  root.style.setProperty('--primary-color', theme.primaryColor);
  root.style.setProperty('--secondary-color', theme.secondaryColor);
  root.style.setProperty('--accent-color', theme.accentColor);
  
  // Optional surface/text colors (override CSS defaults if provided)
  if (theme.surfaceLight) root.style.setProperty('--surface-light', theme.surfaceLight);
  if (theme.surfaceDark) root.style.setProperty('--surface-dark', theme.surfaceDark);
  if (theme.textLight) root.style.setProperty('--text-light', theme.textLight);
  if (theme.textDark) root.style.setProperty('--text-dark', theme.textDark);
}


