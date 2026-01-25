import React from 'react';
import { Shell } from '../components/layout/Shell';
import { Hero } from '../components/blocks/Hero';
import { StatGrid } from '../components/blocks/StatGrid';
import { Directory } from '../components/blocks/Directory';
import { RecentEvents } from '../components/blocks/RecentEvents';
import { InfoSidebar } from '../components/blocks/InfoSidebar';
import { FilterSidebar } from '../components/blocks/FilterSidebar';
import type { BrandConfig, CityPageData } from '../types';

const STATE_NAMES: Record<string, string> = {
  "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
  "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
  "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
  "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
  "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
  "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
  "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
  "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
  "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
  "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
  "DC": "District of Columbia", "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam"
};

interface CityPageProps {
  brand: BrandConfig;
  data: CityPageData;
}

export const CityPage: React.FC<CityPageProps> = ({
  brand,
  data,
}) => {
  // Extract state abbreviation from slug (e.g., "tx-austin" -> "TX")
  const stateAbbrev = data.slug?.split('-')[0]?.toUpperCase() || data.state;
  const stateName = STATE_NAMES[stateAbbrev] || stateAbbrev;

  const breadcrumbs = [
    { label: 'Home', path: '/' },
    { label: 'USA', path: '/usa' },
    { label: stateName, path: `/state/${stateAbbrev.toLowerCase()}` },
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
        title={data.name}
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

