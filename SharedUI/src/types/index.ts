/**
 * Brand Configuration
 * 
 * Stored in DynamoDB: VentureOS-Projects
 * PK: BRAND#{slug}, SK: CONFIG
 * 
 * Structure is IDENTICAL - no mapping/transformation.
 * Frontend fetches and uses directly.
 */
export interface BrandConfig {
  slug: string;
  name: string;
  tagline?: string;
  logoUrl?: string;
  
  theme: BrandTheme;
  scoreConfig?: ScoreConfig;
  typography?: BrandTypography;
  
  footer: {
    copyrightBrand: string;
    yearCreated: number;
    poweredBy?: string;
    links?: FooterLink[];
  };
  
  navItems: NavItem[];
}

export interface BrandTheme {
  primaryColor: string;
  secondaryColor: string;
  accentColor: string;
  surfaceLight?: string;
  surfaceDark?: string;
  textLight?: string;
  textDark?: string;
  defaultTheme?: 'light' | 'dark' | 'auto';
}

export interface ScoreConfig {
  name: string;
  type: string;
  icon?: string;
  betaDisclaimer?: boolean;
}

export interface BrandTypography {
  fontSans?: string;
  fontSerif?: string;
  spaceUnit?: number;
  radiusMaster?: number;
}

export interface NavItem {
  label: string;
  path?: string;
  onClick?: () => void;
  inNavbar?: boolean;              // false = hidden (like Account/Settings)
  children?: NavItem[];
}

export interface FooterLink {
  label: string;
  url: string;
}

// === Page Data Types ===
export interface EntityStats {
  totalViolations: number;
  totalInjuries?: number;
  totalFatalities?: number;
  totalFines?: number;
  avgFines?: number;
  score?: number;
  scoreLabel?: string;
  trend?: {
    direction: 'up' | 'down' | 'flat';
    percentage: number;
  };
}

export interface DirectoryItem {
  slug: string;
  name: string;
  count: number;
  subtitle?: string;               // e.g., state abbreviation, industry
}

export interface RecentEvent {
  eventId: string;
  eventTitle: string;
  eventDescription?: string;
  eventDate?: string;
  companySlug?: string;
  companyName?: string;
  citySlug?: string;
  city?: string;
  state?: string;
  agency: string;
}

export interface EntityMeta {
  population?: number;
  flagUrl?: string;
  logoUrl?: string;
  industry?: string;
  founded?: number;
  headquarters?: string;
  [key: string]: unknown;          // Extensible
}

// === Page-Specific Data ===
export interface NationPageData {
  name: string;                    // "United States"
  slug: string;                    // "usa"
  stats: EntityStats;
  directory: DirectoryItem[];      // States
  meta?: EntityMeta;
}

export interface StatePageData {
  name: string;                    // "Tennessee"
  slug: string;                    // "tn"
  abbreviation: string;            // "TN"
  stats: EntityStats;
  directory: DirectoryItem[];      // Cities
  recentByAgency: Record<string, RecentEvent[]>;  // { OSHA: [...], MSHA: [...] }
  meta?: EntityMeta;
}

export interface CityPageData {
  name: string;                    // "Knoxville"
  slug: string;                    // "knoxville"
  state: string;                   // "TN"
  stateName: string;               // "Tennessee"
  stats: EntityStats;
  directory: DirectoryItem[];      // Companies
  recentByAgency: Record<string, RecentEvent[]>;
  meta?: EntityMeta;
}

export interface CompanyPageData {
  name: string;                    // "Walmart Inc"
  slug: string;                    // "walmart-inc"
  stats: EntityStats;
  sites?: DirectoryItem[];         // Sites (if applicable)
  recentEvents: RecentEvent[];
  meta?: EntityMeta;
}

export interface SitePageData {
  name: string;                    // "Walmart #1234"
  siteId: string;                  // "1234"
  companySlug: string;
  companyName: string;
  stats: EntityStats;
  siblings?: DirectoryItem[];      // Other sites for comparison
  recentEvents: RecentEvent[];
  meta?: EntityMeta;
}


