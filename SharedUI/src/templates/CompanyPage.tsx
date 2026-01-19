import React from 'react';
import { Shell } from '../components/layout/Shell';
import { Hero } from '../components/blocks/Hero';
import { StatGrid } from '../components/blocks/StatGrid';
import { Directory } from '../components/blocks/Directory';
import { RecentEvents } from '../components/blocks/RecentEvents';
import { InfoSidebar } from '../components/blocks/InfoSidebar';
import { FilterSidebar } from '../components/blocks/FilterSidebar';
import type { BrandConfig, CompanyPageData } from '../types';

interface CompanyPageProps {
  brand: BrandConfig;
  data: CompanyPageData;
}

export const CompanyPage: React.FC<CompanyPageProps> = ({
  brand,
  data,
}) => {
  const breadcrumbs = [
    { label: 'Home', path: '/' },
    { label: 'USA', path: '/usa' },
    { label: data.name },
  ];

  const scoreInfo = brand.scoreConfig && data.stats.score !== undefined ? {
    value: data.stats.score,
    label: brand.scoreConfig.name,
    betaDisclaimer: brand.scoreConfig.betaDisclaimer,
  } : undefined;

  const hasSites = data.sites && data.sites.length > 0;

  return (
    <Shell
      brand={brand}
    >
      <Hero
        title={data.name}
        subtitle={data.meta?.industry ? `${data.meta.industry}` : undefined}
        breadcrumbs={breadcrumbs}
        score={scoreInfo}
      />

      {/* 3-Column Layout: Filters | Main | Info */}
      <div className="page-layout">
        <FilterSidebar />

        <main className="page-main">
          <StatGrid stats={data.stats} />
          
          {/* Sites directory (if company has multiple locations) */}
          {hasSites && (
            <Directory
              title="Locations"
              items={data.sites!}
              basePath="/site"
            />
          )}

          {/* Recent Events - show more if no sites directory */}
          <RecentEvents
            title="Recent Incidents"
            events={data.recentEvents.slice(0, hasSites ? 5 : 10)}
            viewMorePath={`/company/${data.slug}/events`}
            showAgencyBadge={true}
          />
        </main>

        <InfoSidebar meta={data.meta} />
      </div>
    </Shell>
  );
};

