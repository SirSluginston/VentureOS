import React from 'react';
import { Shell } from '../components/layout/Shell';
import { Hero } from '../components/blocks/Hero';
import { StatGrid } from '../components/blocks/StatGrid';
import { Directory } from '../components/blocks/Directory';
import { InfoSidebar } from '../components/blocks/InfoSidebar';
import { FilterSidebar } from '../components/blocks/FilterSidebar';
import type { BrandConfig, NationPageData } from '../types';

interface NationPageProps {
  brand: BrandConfig;
  data: NationPageData;
  onAccountClick?: () => void;
  onNotificationClick?: () => void;
}

export const NationPage: React.FC<NationPageProps> = ({
  brand,
  data,
  onAccountClick,
  onNotificationClick,
}) => {
  const breadcrumbs = [
    { label: 'Explore', path: '/explore' },
    { label: data.name },
  ];

  const scoreInfo = brand.scoreConfig && data.stats.score !== undefined ? {
    value: data.stats.score,
    label: brand.scoreConfig.name,
    betaDisclaimer: brand.scoreConfig.betaDisclaimer,
  } : undefined;

  return (
    <Shell
      brand={brand}
      onAccountClick={onAccountClick}
      onNotificationClick={onNotificationClick}
    >
      <Hero
        title={data.name}
        subtitle={brand.tagline || 'Explore worker safety data across the nation'}
        breadcrumbs={breadcrumbs}
        score={scoreInfo}
      />

      {/* 3-Column Layout: Filters | Main | Info */}
      <div className="page-layout">
        <FilterSidebar showAd={true} />

        <main className="page-main">
          <StatGrid stats={data.stats} columns={4} />
          
          <Directory
            title="States & Territories"
            items={data.directory}
            basePath=""
            initialLimit={60}
          />
        </main>

        <InfoSidebar meta={data.meta} showAd={true} />
      </div>
    </Shell>
  );
};

