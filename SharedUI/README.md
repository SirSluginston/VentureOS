# @sirsluginston/sharedui

Server-Driven UI component library for SirSluginston Co.

## Installation

```bash
npm install @sirsluginston/sharedui
```

**Peer Dependencies:**
```bash
npm install react react-dom
```

## Quick Start

```tsx
import { StatePage, applyTheme } from '@sirsluginston/sharedui';
import '@sirsluginston/sharedui/dist/style.css';

// Apply brand theme
applyTheme({
  primaryColor: '#1a365d',
  secondaryColor: '#2d3748',
  accentColor: '#319795'
});

// Render a page
function App() {
  return (
    <StatePage
      brand={brandConfig}
      data={stateData}
    />
  );
}
```

## Exports

### Page Templates (Primary)
Pre-composed pages for SDUI rendering:

| Template | Description |
|----------|-------------|
| `NationPage` | Nation-level overview with state directory |
| `StatePage` | State-level with cities directory + recent events |
| `CityPage` | City-level with companies directory + recent events |
| `CompanyPage` | Company profile with sites + event history |
| `SitePage` | Individual site (e.g., "Walmart #1234") |

### Block Components
Composable sections for custom pages:

- `Hero` - Title, breadcrumbs, score badge
- `StatGrid` - Statistics cards grid
- `Directory` - Searchable entity list (cities, companies, etc.)
- `RecentEvents` - Event/incident feed
- `FilterSidebar` - Left sidebar with filters
- `InfoSidebar` - Right sidebar with metadata + ad slot
- `AdSlot` - Placeholder for advertisements
- `MobileAdBanner` - Sticky bottom banner for mobile

### Layout Components
- `Shell` - Page wrapper (header, footer, theming)
- `Header` - Brand navigation header
- `Footer` - Copyright and links

### Core UI Components
- `Button`, `Card`, `Badge`, `Input`
- `Table`, `Modal`, `Alert`, `Tabs`
- `Tooltip`, `Spinner`, `Skeleton`

### Hooks & Utilities
- `useTheme()` - Dark mode toggle hook
- `applyTheme(config)` - Apply brand colors to CSS variables

### Types
Full TypeScript definitions exported:
- `BrandConfig` - Brand configuration
- `NationPageData`, `StatePageData`, `CityPageData`, etc.
- `EntityStats`, `DirectoryItem`, `RecentEvent`

## Theming

SharedUI uses CSS variables for theming. Override at runtime:

```ts
import { applyTheme } from '@sirsluginston/sharedui';

applyTheme({
  primaryColor: '#your-primary',
  secondaryColor: '#your-secondary', 
  accentColor: '#your-accent'
});
```

Or override in CSS:
```css
:root {
  --primary-color: #custom;
  --accent-color: #custom;
}
```

## Responsive Layout

The `page-layout` CSS class provides:
- **Desktop (>1400px)**: 3-column grid (filters | main | info)
- **Tablet (992-1400px)**: 2-column (main | info)
- **Mobile (<992px)**: Single column + sticky ad banner

## Development

```bash
# Install dependencies
npm install

# Start dev server with demo
npm run dev

# Build library
npm run build
```

## License

MIT © SirSluginston Co

