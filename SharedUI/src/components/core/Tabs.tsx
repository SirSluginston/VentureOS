import React from 'react';

interface Tab {
  id: string;
  label: string;
  content: React.ReactNode;
  disabled?: boolean;
}

interface TabsProps {
  tabs: Tab[];
  activeTab: string;
  onTabChange: (tabId: string) => void;
  variant?: 'underline' | 'pills';
}

export const Tabs: React.FC<TabsProps> = ({
  tabs,
  activeTab,
  onTabChange,
  variant = 'underline',
}) => {
  const isUnderline = variant === 'underline';

  return (
    <div style={{ width: '100%' }}>
      {/* Tab Headers */}
      <div
        role="tablist"
        style={{
          display: 'flex',
          gap: isUnderline ? 'var(--space-md)' : 'var(--space-xs)',
          borderBottom: isUnderline ? '2px solid var(--border-color)' : 'none',
          padding: isUnderline ? '0' : 'var(--space-xs)',
          backgroundColor: isUnderline ? 'transparent' : 'var(--secondary-color)',
          borderRadius: isUnderline ? '0' : 'var(--radius-md)',
        }}
      >
        {tabs.map((tab) => {
          const isActive = tab.id === activeTab;
          
          return (
            <button
              key={tab.id}
              role="tab"
              aria-selected={isActive}
              aria-controls={`panel-${tab.id}`}
              disabled={tab.disabled}
              onClick={() => !tab.disabled && onTabChange(tab.id)}
              style={{
                background: isUnderline
                  ? 'none'
                  : isActive ? 'var(--primary-color)' : 'transparent',
                border: 'none',
                borderBottom: isUnderline && isActive ? '3px solid var(--primary-color)' : 'none',
                marginBottom: isUnderline ? '-2px' : '0',
                padding: 'var(--space-sm) var(--space-md)',
                borderRadius: isUnderline ? '0' : 'var(--radius-sm)',
                fontFamily: 'var(--font-sans)',
                fontWeight: 600,
                fontSize: '0.95rem',
                color: isActive
                  ? isUnderline ? 'var(--primary-color)' : 'var(--surface-light)'
                  : 'var(--text-color)',
                opacity: tab.disabled ? 0.5 : 1,
                cursor: tab.disabled ? 'not-allowed' : 'pointer',
                transition: 'all var(--transition-fast)',
              }}
              onMouseEnter={(e) => {
                if (!tab.disabled && !isActive) {
                  e.currentTarget.style.backgroundColor = isUnderline ? 'rgba(0,0,0,0.05)' : 'var(--accent-color)';
                }
              }}
              onMouseLeave={(e) => {
                if (!tab.disabled && !isActive) {
                  e.currentTarget.style.backgroundColor = isUnderline ? 'transparent' : 'transparent';
                }
              }}
            >
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Tab Content */}
      <div style={{ marginTop: 'var(--space-lg)' }}>
        {tabs.map((tab) => (
          <div
            key={tab.id}
            id={`panel-${tab.id}`}
            role="tabpanel"
            hidden={tab.id !== activeTab}
            style={{
              display: tab.id === activeTab ? 'block' : 'none',
            }}
          >
            {tab.content}
          </div>
        ))}
      </div>
    </div>
  );
};


