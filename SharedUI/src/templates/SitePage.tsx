import React from 'react';
import { Shell } from '../components/layout/Shell';
import { Hero } from '../components/blocks/Hero';
import { StatGrid } from '../components/blocks/StatGrid';
import { RecentEvents } from '../components/blocks/RecentEvents';
import { InfoSidebar } from '../components/blocks/InfoSidebar';
import { FilterSidebar } from '../components/blocks/FilterSidebar';
import type { BrandConfig, SitePageData } from '../types';

interface SitePageProps {
  brand: BrandConfig;
  data: SitePageData;
  onAccountClick?: () => void;
  onNotificationClick?: () => void;
}

export const SitePage: React.FC<SitePageProps> = ({
  brand,
  data,
  onAccountClick,
  onNotificationClick,
}) => {
  const breadcrumbs = [
    { label: 'USA', path: '/explore' },
    { label: 'Companies', path: '/companies' },
    { label: data.companyName, path: `/company/${data.companySlug}` },
    { label: `#${data.siteId}` },
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
        subtitle={`Location #${data.siteId} of ${data.companyName}`}
        breadcrumbs={breadcrumbs}
        score={scoreInfo}
      />

      {/* 3-Column Layout: Filters | Main | Info */}
      <div className="page-layout">
        <FilterSidebar showAd={true} />

        <main className="page-main">
          <StatGrid stats={data.stats} />
          
          {/* Compare to Other Sites */}
          {data.siblings && data.siblings.length > 0 && (
            <section style={{
              padding: 'var(--space-lg)',
              backgroundColor: 'var(--bg-color)',
              marginBottom: 'var(--space-lg)',
              borderRadius: 'var(--radius-md)',
              border: '1px solid var(--border-color)',
            }}>
              <h2 style={{
                margin: '0 0 var(--space-md)',
                fontFamily: 'var(--font-serif)',
                fontSize: '1.5rem',
                color: 'var(--text-color)',
              }}>
                Compare to Other Locations
              </h2>
              <div style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: 'var(--space-sm)',
              }}>
                {data.siblings.slice(0, 10).map((sibling) => (
                  <a
                    key={sibling.slug}
                    href={`/company/${data.companySlug}/site/${sibling.slug}`}
                    style={{
                      padding: 'var(--space-sm) var(--space-md)',
                      backgroundColor: 'var(--secondary-color)',
                      borderRadius: 'var(--radius-md)',
                      textDecoration: 'none',
                      color: 'var(--text-dark)',
                      fontFamily: 'var(--font-sans)',
                      fontSize: '0.9rem',
                      display: 'flex',
                      alignItems: 'center',
                      gap: 'var(--space-xs)',
                      transition: 'background var(--transition-fast)',
                    }}
                    onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--accent-color)'}
                    onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'var(--secondary-color)'}
                  >
                    <span>#{sibling.name}</span>
                    <span style={{
                      backgroundColor: 'var(--primary-color)',
                      color: 'var(--text-dark)',
                      padding: '2px 6px',
                      borderRadius: 'var(--radius-sm)',
                      fontSize: '0.75rem',
                    }}>
                      {sibling.count}
                    </span>
                  </a>
                ))}
              </div>
            </section>
          )}

          {/* Recent Events */}
          <RecentEvents
            title="Recent Incidents"
            events={data.recentEvents}
            viewMorePath={`/company/${data.companySlug}/site/${data.siteId}/events`}
            showAgencyBadge={true}
          />
        </main>

        <InfoSidebar meta={data.meta} showAd={true} />
      </div>
    </Shell>
  );
};

