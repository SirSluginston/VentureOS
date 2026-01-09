import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({ region: 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(client);

const TABLE_NAME = 'VentureOS-Projects';

// Pages for BRAND#SirSluginston
const sirSluginstonPages = [
  {
    PK: 'BRAND#SirSluginston',
    SK: 'PAGE#Home',
    Config: {
      HasShell: true,
      InNavbar: true,
      NavbarOrder: 1,
      NavbarLabel: 'Home',
    },
    Identity: {
      PageTitle: 'Home',
      PageTagline: 'Welcome to SirSluginston Co',
      Route: '/',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
  {
    PK: 'BRAND#SirSluginston',
    SK: 'PAGE#Projects',
    Config: {
      HasShell: true,
      InNavbar: true,
      NavbarOrder: 2,
      NavbarLabel: 'Projects',
    },
    Identity: {
      PageTitle: 'Projects',
      PageTagline: 'All SirSluginston Co Projects',
      Route: '/projects',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
  {
    PK: 'BRAND#SirSluginston',
    SK: 'PAGE#About',
    Config: {
      HasShell: true,
      InNavbar: true,
      NavbarOrder: 3,
      NavbarLabel: 'About',
    },
    Identity: {
      PageTitle: 'About',
      PageTagline: 'About SirSluginston Co',
      Route: '/about',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
  {
    PK: 'BRAND#SirSluginston',
    SK: 'PAGE#Admin',
    Config: {
      HasShell: true,
      InNavbar: true,
      NavbarOrder: 4,
      NavbarLabel: 'Admin',
      NavbarRoles: ['Admin'],
      AllowedRoles: ['Admin'],
    },
    Identity: {
      PageTitle: 'Admin',
      PageTagline: 'Administration',
      Route: '/admin',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
  {
    PK: 'BRAND#SirSluginston',
    SK: 'PAGE#Account',
    Config: {
      HasShell: true,
      InNavbar: false, // Accessed through icon
    },
    Identity: {
      PageTitle: 'Account',
      PageTagline: 'Account Settings',
      Route: '/account',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
];

// Pages for other brands (without Projects)
const otherBrandPages = [
  {
    PK: 'BRAND#OSHAtrail',
    SK: 'PAGE#Home',
    Config: {
      HasShell: true,
      InNavbar: true,
      NavbarOrder: 1,
      NavbarLabel: 'Home',
    },
    Identity: {
      PageTitle: 'Home',
      PageTagline: 'Welcome to OSHA Trail',
      Route: '/',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
  {
    PK: 'BRAND#OSHAtrail',
    SK: 'PAGE#About',
    Config: {
      HasShell: true,
      InNavbar: true,
      NavbarOrder: 2,
      NavbarLabel: 'About',
    },
    Identity: {
      PageTitle: 'About',
      PageTagline: 'About OSHA Trail',
      Route: '/about',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
  {
    PK: 'BRAND#OSHAtrail',
    SK: 'PAGE#Admin',
    Config: {
      HasShell: true,
      InNavbar: true,
      NavbarOrder: 3,
      NavbarLabel: 'Admin',
      NavbarRoles: ['Admin'],
      AllowedRoles: ['Admin'],
    },
    Identity: {
      PageTitle: 'Admin',
      PageTagline: 'Administration',
      Route: '/admin',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
  {
    PK: 'BRAND#OSHAtrail',
    SK: 'PAGE#Account',
    Config: {
      HasShell: true,
      InNavbar: false,
    },
    Identity: {
      PageTitle: 'Account',
      PageTagline: 'Account Settings',
      Route: '/account',
      Version: '1.0.0',
    },
    Meta: {
      LastUpdated: new Date().toISOString(),
    },
  },
];

async function createPages() {
  const allPages = [...sirSluginstonPages, ...otherBrandPages];
  
  // Add pages for TransportTrail and HabiTasks
  const otherBrands = ['BRAND#TransportTrail', 'BRAND#HabiTasks'];
  for (const brand of otherBrands) {
    const brandPages = otherBrandPages.map(page => ({
      ...page,
      PK: brand,
      Identity: {
        ...page.Identity,
        PageTagline: page.Identity.PageTagline.replace('OSHA Trail', brand.replace('BRAND#', '')),
      },
    }));
    allPages.push(...brandPages);
  }

  console.log(`Creating ${allPages.length} page entries...`);

  for (const page of allPages) {
    try {
      await docClient.send(new PutCommand({
        TableName: TABLE_NAME,
        Item: page,
      }));
      console.log(`✅ Created ${page.PK} - ${page.SK}`);
    } catch (error) {
      console.error(`❌ Failed to create ${page.PK} - ${page.SK}:`, error.message);
    }
  }

  console.log('\n✅ All pages created!');
}

createPages().catch(console.error);

