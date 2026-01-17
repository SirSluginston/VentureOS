import React, { useState } from 'react';
import { createRoot } from 'react-dom/client';

// Import styles
import '../styles/theme.css';

// Import components
import { StatePage } from '../templates/StatePage';
import { CityPage } from '../templates/CityPage';
import { CompanyPage } from '../templates/CompanyPage';
import { Button, Card, Badge, Alert, Tabs, Modal } from '../components/core';

// Import mock data
import { oshaTrailBrand, tennesseeData, knoxvilleData, walmartData } from './mockData';

type DemoPage = 'state' | 'city' | 'company' | 'components';

const App: React.FC = () => {
  const [currentPage, setCurrentPage] = useState<DemoPage>('state');
  const [showModal, setShowModal] = useState(false);
  const [activeTab, setActiveTab] = useState('buttons');

  const renderPage = () => {
    switch (currentPage) {
      case 'state':
        return <StatePage brand={oshaTrailBrand} data={tennesseeData} />;
      case 'city':
        return <CityPage brand={oshaTrailBrand} data={knoxvilleData} />;
      case 'company':
        return <CompanyPage brand={oshaTrailBrand} data={walmartData} />;
      case 'components':
        return <ComponentShowcase 
          showModal={showModal} 
          setShowModal={setShowModal}
          activeTab={activeTab}
          setActiveTab={setActiveTab}
        />;
    }
  };

  return (
    <div>
      {/* Demo Navigation */}
      <div style={{
        position: 'fixed',
        bottom: 20,
        right: 20,
        zIndex: 9999,
        display: 'flex',
        gap: 8,
        backgroundColor: 'rgba(0,0,0,0.8)',
        padding: 12,
        borderRadius: 12,
        boxShadow: '0 4px 20px rgba(0,0,0,0.3)',
      }}>
        <DemoButton active={currentPage === 'state'} onClick={() => setCurrentPage('state')}>
          State
        </DemoButton>
        <DemoButton active={currentPage === 'city'} onClick={() => setCurrentPage('city')}>
          City
        </DemoButton>
        <DemoButton active={currentPage === 'company'} onClick={() => setCurrentPage('company')}>
          Company
        </DemoButton>
        <DemoButton active={currentPage === 'components'} onClick={() => setCurrentPage('components')}>
          Components
        </DemoButton>
      </div>

      {renderPage()}
    </div>
  );
};

const DemoButton: React.FC<{ 
  active: boolean; 
  onClick: () => void; 
  children: React.ReactNode 
}> = ({ active, onClick, children }) => (
  <button
    onClick={onClick}
    style={{
      padding: '8px 16px',
      border: 'none',
      borderRadius: 8,
      backgroundColor: active ? '#c05621' : '#4a5568',
      color: 'white',
      fontWeight: 600,
      cursor: 'pointer',
      transition: 'all 0.2s',
    }}
  >
    {children}
  </button>
);

const ComponentShowcase: React.FC<{
  showModal: boolean;
  setShowModal: (v: boolean) => void;
  activeTab: string;
  setActiveTab: (v: string) => void;
}> = ({ showModal, setShowModal, activeTab, setActiveTab }) => (
  <div style={{ 
    padding: 40, 
    maxWidth: 1200, 
    margin: '0 auto',
    backgroundColor: 'var(--bg-color)',
    minHeight: '100vh',
  }}>
    <h1 style={{ 
      fontFamily: 'var(--font-serif)', 
      color: 'var(--primary-color)',
      marginBottom: 40,
    }}>
      🛤️ SharedUI v2 Component Library
    </h1>

    <Tabs
      tabs={[
        {
          id: 'buttons',
          label: 'Buttons',
          content: (
            <Card title="Button Variants" style={{ marginBottom: 24 }}>
              <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
                <Button variant="primary">Primary</Button>
                <Button variant="secondary">Secondary</Button>
                <Button variant="outline">Outline</Button>
                <Button variant="ghost">Ghost</Button>
                <Button variant="danger">Danger</Button>
              </div>
              <div style={{ display: 'flex', gap: 12, marginTop: 16, alignItems: 'center' }}>
                <Button size="sm">Small</Button>
                <Button size="md">Medium</Button>
                <Button size="lg">Large</Button>
                <Button loading>Loading</Button>
                <Button disabled>Disabled</Button>
              </div>
            </Card>
          ),
        },
        {
          id: 'badges',
          label: 'Badges',
          content: (
            <Card title="Badge Variants" style={{ marginBottom: 24 }}>
              <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
                <Badge>Default</Badge>
                <Badge variant="primary">Primary</Badge>
                <Badge variant="secondary">Secondary</Badge>
                <Badge variant="success">Success</Badge>
                <Badge variant="warning">Warning</Badge>
                <Badge variant="danger">Danger</Badge>
                <Badge variant="outline">Outline</Badge>
              </div>
            </Card>
          ),
        },
        {
          id: 'alerts',
          label: 'Alerts',
          content: (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              <Alert variant="info" title="Information">
                This is an informational alert with some helpful context.
              </Alert>
              <Alert variant="success" title="Success!">
                Your changes have been saved successfully.
              </Alert>
              <Alert variant="warning" title="Warning">
                Please review your data before continuing.
              </Alert>
              <Alert variant="danger" title="Error" dismissible onDismiss={() => {}}>
                Something went wrong. Please try again.
              </Alert>
            </div>
          ),
        },
        {
          id: 'cards',
          label: 'Cards',
          content: (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: 24 }}>
              <Card title="Basic Card" subtitle="With subtitle">
                <p style={{ margin: 0 }}>This is a basic card with title and subtitle.</p>
              </Card>
              <Card title="Hoverable Card" hoverable>
                <p style={{ margin: 0 }}>Hover over me for an effect!</p>
              </Card>
              <Card>
                <p style={{ margin: 0 }}>A card without a title, just content.</p>
              </Card>
            </div>
          ),
        },
        {
          id: 'modal',
          label: 'Modal',
          content: (
            <Card title="Modal Demo">
              <Button onClick={() => setShowModal(true)}>Open Modal</Button>
              <Modal
                open={showModal}
                onClose={() => setShowModal(false)}
                title="Example Modal"
                size="md"
              >
                <p style={{ margin: '0 0 16px' }}>
                  This is a modal dialog. It can contain any content you need.
                </p>
                <p style={{ margin: '0 0 16px' }}>
                  Press Escape or click outside to close.
                </p>
                <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
                  <Button variant="outline" onClick={() => setShowModal(false)}>
                    Cancel
                  </Button>
                  <Button onClick={() => setShowModal(false)}>
                    Confirm
                  </Button>
                </div>
              </Modal>
            </Card>
          ),
        },
      ]}
      activeTab={activeTab}
      onTabChange={setActiveTab}
    />
  </div>
);

// Mount app
const root = createRoot(document.getElementById('root')!);
root.render(<App />);

