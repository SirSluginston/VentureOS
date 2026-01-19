import type { BrandConfig, StatePageData, CityPageData, CompanyPageData } from '../types';

export const oshaTrailBrand: BrandConfig = {
  slug: 'osha-trail',  // URL-friendly: osha-trail.com
  name: 'OSHA Trail',
  tagline: 'Follow the Safety Trail',
  logoUrl: undefined, // Would be actual logo URL
  
  // Theme matches DynamoDB: BRAND#OSHATrail → CONFIG
  theme: {
    primaryColor: '#1a365d',    // Dark navy - main brand color
    secondaryColor: '#2d3748',  // Dark gray - supporting color
    accentColor: '#319795',     // Teal - call-to-action, highlights
    surfaceLight: '#fafafa',
    surfaceDark: '#1a1a2e',
    defaultTheme: 'light',
  },
  
  scoreConfig: {
    name: 'Safety Score',
    type: 'osha-safety',
    icon: 'shield',
    betaDisclaimer: true,
  },
  
  footer: {
    copyrightBrand: 'OSHA Trail',
    yearCreated: 2024,
    poweredBy: 'SirSluginston VentureOS',
  },
  
  navItems: [
    { label: 'Home', path: '/', inNavbar: true },
    { label: 'Explore', path: '/explore', inNavbar: true },
    { label: 'Companies', path: '/companies', inNavbar: true },
    { label: 'About', path: '/about', inNavbar: true },
    { label: 'Account', path: '/account', inNavbar: false },
  ],
};

export const tennesseeData: StatePageData = {
  name: 'Tennessee',
  slug: 'tn',
  abbreviation: 'TN',
  stats: {
    totalEvents: 5420,
    totalCities: 95,
    totalCompanies: 1847,
    totalInjuries: 3241,
    totalFatalities: 89,
    totalFines: 12500000,
    avgFines: 2306,
    score: 68,
    trend: { direction: 'down', percentage: 8 },
  },
  directory: [
    { slug: 'nashville', name: 'Nashville', count: 892, subtitle: 'Davidson County' },
    { slug: 'memphis', name: 'Memphis', count: 743, subtitle: 'Shelby County' },
    { slug: 'knoxville', name: 'Knoxville', count: 521, subtitle: 'Knox County' },
    { slug: 'chattanooga', name: 'Chattanooga', count: 412, subtitle: 'Hamilton County' },
    { slug: 'clarksville', name: 'Clarksville', count: 287, subtitle: 'Montgomery County' },
    { slug: 'murfreesboro', name: 'Murfreesboro', count: 234, subtitle: 'Rutherford County' },
    { slug: 'franklin', name: 'Franklin', count: 198, subtitle: 'Williamson County' },
    { slug: 'jackson', name: 'Jackson', count: 156, subtitle: 'Madison County' },
    { slug: 'johnson-city', name: 'Johnson City', count: 143, subtitle: 'Washington County' },
    { slug: 'bartlett', name: 'Bartlett', count: 121, subtitle: 'Shelby County' },
  ],
  recentByAgency: {
    OSHA: [
      {
        eventId: 'evt-001',
        eventTitle: 'Fall from Height at Walmart Distribution Center',
        eventDescription: 'Employee fell approximately 15 feet from elevated platform while retrieving inventory.',
        eventDate: '2026-01-15',
        companySlug: 'walmart-inc',
        companyName: 'Walmart Inc',
        city: 'Nashville',
        state: 'TN',
        agency: 'OSHA',
      },
      {
        eventId: 'evt-002',
        eventTitle: 'Amputation at AutoZone Manufacturing',
        eventDescription: 'Worker sustained partial finger amputation while operating hydraulic press.',
        eventDate: '2026-01-12',
        companySlug: 'autozone-inc',
        companyName: 'AutoZone Inc',
        city: 'Memphis',
        state: 'TN',
        agency: 'OSHA',
      },
      {
        eventId: 'evt-003',
        eventTitle: 'Chemical Exposure at Eastman Chemical',
        eventDescription: 'Multiple employees reported respiratory symptoms following chemical spill.',
        eventDate: '2026-01-10',
        companySlug: 'eastman-chemical',
        companyName: 'Eastman Chemical Company',
        city: 'Kingsport',
        state: 'TN',
        agency: 'OSHA',
      },
      {
        eventId: 'evt-004',
        eventTitle: 'Struck By Equipment at FedEx Hub',
        eventDescription: 'Package handler struck by forklift in sorting facility.',
        eventDate: '2026-01-08',
        companySlug: 'fedex-corp',
        companyName: 'FedEx Corporation',
        city: 'Memphis',
        state: 'TN',
        agency: 'OSHA',
      },
      {
        eventId: 'evt-005',
        eventTitle: 'Hospitalization at Nissan Plant',
        eventDescription: 'Assembly line worker hospitalized following machinery malfunction.',
        eventDate: '2026-01-05',
        companySlug: 'nissan-north-america',
        companyName: 'Nissan North America',
        city: 'Smyrna',
        state: 'TN',
        agency: 'OSHA',
      },
    ],
    MSHA: [
      {
        eventId: 'evt-006',
        eventTitle: 'Roof Fall at Bledsoe Coal Mine',
        eventDescription: 'Section of mine roof collapsed, injuring two workers.',
        eventDate: '2026-01-14',
        companySlug: 'bledsoe-coal',
        companyName: 'Bledsoe Coal Company',
        city: 'Pikeville',
        state: 'TN',
        agency: 'MSHA',
      },
    ],
  },
  meta: {
    population: 7051339,
    flagUrl: 'https://upload.wikimedia.org/wikipedia/commons/9/9e/Flag_of_Tennessee.svg',
  },
};

export const knoxvilleData: CityPageData = {
  name: 'Knoxville',
  slug: 'knoxville',
  state: 'TN',
  stateName: 'Tennessee',
  stats: {
    totalEvents: 521,
    totalCompanies: 234,
    totalInjuries: 312,
    totalFatalities: 8,
    totalFines: 1250000,
    avgFines: 2399,
    score: 72,
  },
  directory: [
    { slug: 'walmart-inc', name: 'Walmart Inc', count: 23, subtitle: 'Retail' },
    { slug: 'pilot-flying-j', name: 'Pilot Flying J', count: 18, subtitle: 'Travel Centers' },
    { slug: 'denso-manufacturing', name: 'DENSO Manufacturing', count: 15, subtitle: 'Auto Parts' },
    { slug: 'clayton-homes', name: 'Clayton Homes', count: 12, subtitle: 'Housing' },
    { slug: 'covenant-health', name: 'Covenant Health', count: 9, subtitle: 'Healthcare' },
  ],
  recentByAgency: {
    OSHA: tennesseeData.recentByAgency.OSHA.slice(0, 3),
  },
  meta: {
    population: 192648,
  },
};

export const walmartData: CompanyPageData = {
  name: 'Walmart Inc',
  slug: 'walmart-inc',
  stats: {
    totalEvents: 1847,
    totalInjuries: 1203,
    totalFatalities: 12,
    totalFines: 8500000,
    avgFines: 4602,
    score: 45,
  },
  sites: [
    { slug: '1234', name: '1234', count: 8, subtitle: 'Nashville, TN' },
    { slug: '5678', name: '5678', count: 6, subtitle: 'Memphis, TN' },
    { slug: '9012', name: '9012', count: 5, subtitle: 'Knoxville, TN' },
    { slug: '3456', name: '3456', count: 4, subtitle: 'Chattanooga, TN' },
  ],
  recentEvents: tennesseeData.recentByAgency.OSHA.filter(e => e.companySlug === 'walmart-inc'),
  meta: {
    logoUrl: 'https://logo.clearbit.com/walmart.com',
    industry: 'Retail - General Merchandise',
    founded: 1962,
    headquarters: 'Bentonville, AR',
  },
};

