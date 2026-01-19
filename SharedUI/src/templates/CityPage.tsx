import React from 'react';
import { Shell } from '../components/layout/Shell';
import { Hero } from '../components/blocks/Hero';
import { StatGrid } from '../components/blocks/StatGrid';
import { Directory } from '../components/blocks/Directory';
import { RecentEvents } from '../components/blocks/RecentEvents';
import { InfoSidebar } from '../components/blocks/InfoSidebar';
import { FilterSidebar } from '../components/blocks/FilterSidebar';
import type { BrandConfig, CityPageData } from '../types';

interface CityPageProps {
  brand: BrandConfig;
  data: CityPageData;
}

export const CityPage: React.FC<CityPageProps> = ({
  brand,
  data,
}) => {
  const breadcrumbs = [
    { label: 'Home', path: '/' },
    { label: 'USA', path: '/usa' },
    { label: data.stateName, path: `/state/${data.state.toLowerCase()}` },
    { label: data.name },
  ];

  const scoreInfo = brand.scoreConfig && data.stats.score !== undefined ? {
    value: data.stats.score,
    label: brand.scoreConfig.name,
    betaDisclaimer: brand.scoreConfig.betaDisclaimer,
  } : undefined;

  const agencies = Object.keys(data.recentByAgency);
  const basePath = `/${data.state.toLowerCase()}/${data.slug}`;

  return (
    <Shell
      brand={brand}
    >
      <Hero
        title={`${data.name}, ${data.state}`}
        subtitle={`Worker safety data for ${data.name}`}
        breadcrumbs={breadcrumbs}
        score={scoreInfo}
      />

      {/* 3-Column Layout: Filters | Main | Info */}
      <div className="page-layout">
        <FilterSidebar />

        <main className="page-main">
          <StatGrid stats={data.stats} />
          
          <Directory
            title="Companies"
            items={data.directory}
            basePath="/company"
          />

          {/* Recent Events by Agency */}
          {agencies.map((agency) => (
            <RecentEvents
              key={agency}
              title={`Recent ${agency} Incidents`}
              events={data.recentByAgency[agency]}
              viewMorePath={`${basePath}/events/${agency.toLowerCase()}`}
              showAgencyBadge={false}
            />
          ))}
        </main>

        <InfoSidebar meta={data.meta} />
      </div>
    </Shell>
  );
};

