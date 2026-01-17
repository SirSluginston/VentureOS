import React from 'react';
import type { RecentEvent } from '../../types';

interface RecentEventsProps {
  title: string;
  events: RecentEvent[];
  viewMorePath?: string;
  showAgencyBadge?: boolean;
}

export const RecentEvents: React.FC<RecentEventsProps> = ({
  title,
  events,
  viewMorePath,
  showAgencyBadge = false,
}) => {
  if (events.length === 0) return null;

  return (
    <section style={{
      marginBottom: 'var(--space-lg)',
    }}>
      <div>
        {/* Header */}
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 'var(--space-md)',
        }}>
          <h2 style={{
            margin: 0,
            fontFamily: 'var(--font-serif)',
            fontSize: '1.5rem',
            color: 'var(--text-color)',
          }}>
            {title}
          </h2>

          {viewMorePath && (
            <a
              href={viewMorePath}
              style={{
                color: 'var(--accent-color)',
                textDecoration: 'none',
                fontFamily: 'var(--font-sans)',
                fontWeight: 600,
                fontSize: '0.9rem',
              }}
            >
              View More →
            </a>
          )}
        </div>

        {/* Events List */}
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          gap: 'var(--space-sm)',
        }}>
          {events.map((event) => (
            <EventCard 
              key={event.eventId} 
              event={event} 
              showAgencyBadge={showAgencyBadge}
            />
          ))}
        </div>
      </div>
    </section>
  );
};

const EventCard: React.FC<{ event: RecentEvent; showAgencyBadge: boolean }> = ({ 
  event, 
  showAgencyBadge 
}) => (
  <div style={{
    padding: 'var(--space-md) var(--space-lg)',
    border: '2px solid var(--border-color)',
    borderRadius: 'var(--radius-md)',
    backgroundColor: 'var(--bg-color)',
  }}
  >
    <div style={{
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'flex-start',
      gap: 'var(--space-md)',
      flexWrap: 'wrap',
    }}>
      {/* Left: Title & Description */}
      <div style={{ flex: 1, minWidth: 200 }}>
        <h3 style={{
          margin: 0,
          fontFamily: 'var(--font-sans)',
          fontWeight: 600,
          fontSize: '1rem',
          color: 'var(--text-color)',
        }}>
          {event.eventTitle}
        </h3>
        {event.eventDescription && (
          <p style={{
            margin: 'var(--space-xs) 0 0',
            fontFamily: 'var(--font-sans)',
            fontSize: '0.9rem',
            color: 'var(--text-color)',
            opacity: 0.7,
            display: '-webkit-box',
            WebkitLineClamp: 2,
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
          }}>
            {event.eventDescription}
          </p>
        )}
      </div>

      {/* Right: Meta */}
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-end',
        gap: 'var(--space-xs)',
      }}>
        {showAgencyBadge && (
          <span style={{
            backgroundColor: 'var(--primary-color)',
            color: 'var(--bg-color)',
            padding: 'var(--space-xs) var(--space-sm)',
            borderRadius: 'var(--radius-sm)',
            fontFamily: 'var(--font-sans)',
            fontWeight: 600,
            fontSize: '0.75rem',
            textTransform: 'uppercase',
          }}>
            {event.agency}
          </span>
        )}
        {event.eventDate && (
          <span style={{
            fontFamily: 'var(--font-sans)',
            fontSize: '0.85rem',
            color: 'var(--text-color)',
            opacity: 0.6,
          }}>
            {formatDate(event.eventDate)}
          </span>
        )}
        {(event.city || event.state) && (
          <span style={{
            fontFamily: 'var(--font-sans)',
            fontSize: '0.85rem',
            color: 'var(--text-color)',
            opacity: 0.6,
          }}>
            {[event.city, event.state].filter(Boolean).join(', ')}
          </span>
        )}
        {event.companyName && (
          <a
            href={`/company/${event.companySlug}`}
            style={{
              fontFamily: 'var(--font-sans)',
              fontSize: '0.85rem',
              color: 'var(--primary-color)',
              textDecoration: 'none',
              fontWeight: 600,
              transition: 'color var(--transition-fast)',
            }}
            onMouseEnter={(e) => e.currentTarget.style.color = 'var(--accent-color)'}
            onMouseLeave={(e) => e.currentTarget.style.color = 'var(--primary-color)'}
          >
            {event.companyName} →
          </a>
        )}
      </div>
    </div>
  </div>
);

function formatDate(dateString: string): string {
  try {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch {
    return dateString;
  }
}

