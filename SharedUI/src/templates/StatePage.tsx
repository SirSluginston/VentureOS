import React from 'react';
import { Shell } from '../components/layout/Shell';
import { Hero } from '../components/blocks/Hero';
import { StatGrid } from '../components/blocks/StatGrid';
import { Directory } from '../components/blocks/Directory';
import { RecentEvents } from '../components/blocks/RecentEvents';
import { InfoSidebar } from '../components/blocks/InfoSidebar';
import { FilterSidebar } from '../components/blocks/FilterSidebar';
import type { BrandConfig, StatePageData } from '../types';

interface StatePageProps {
  brand: BrandConfig;
  data: StatePageData;
  onAccountClick?: () => void;
  onNotificationClick?: () => void;
}

export const StatePage: React.FC<StatePageProps> = ({
  brand,
  data,
  onAccountClick,
  onNotificationClick,
}) => {
  const breadcrumbs = [
    { label: 'USA', path: '/explore' },
    { label: data.name },
  ];

  const scoreInfo = brand.scoreConfig && data.stats.score !== undefined ? {
    value: data.stats.score,
    label: brand.scoreConfig.name,
    betaDisclaimer: brand.scoreConfig.betaDisclaimer,
  } : undefined;

  const agencies = Object.keys(data.recentByAgency);

  return (
    <Shell
      brand={brand}
      onAccountClick={onAccountClick}
      onNotificationClick={onNotificationClick}
    >
      <Hero
        title={data.name}
        subtitle={`Worker safety data for ${data.name}`}
        breadcrumbs={breadcrumbs}
        score={scoreInfo}
      />

      {/* 3-Column Layout: Filters | Main | Info */}
      <div className="page-layout">
        <FilterSidebar showAd={true} />

        <main className="page-main">
        <StatGrid stats={data.stats} />
        
        <Directory
          title="Cities"
          items={data.directory}
          basePath={`/${data.slug.toLowerCase()}`}
        />

        {/* Recent Events by Agency */}
        {agencies.map((agency) => (
          <RecentEvents
            key={agency}
            title={`Recent ${agency} Incidents`}
            events={data.recentByAgency[agency]}
            viewMorePath={`/${data.slug.toLowerCase()}/events/${agency.toLowerCase()}`}
            showAgencyBadge={false}
          />
        ))}
        </main>

        <InfoSidebar meta={data.meta} showAd={true} />
      </div>
    </Shell>
  );
};

