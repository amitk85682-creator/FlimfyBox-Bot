export interface AppUser {
  id: number;
  username: string;
  first_name: string;
  photo_url?: string;
  is_guest?: boolean;
}

export interface WatchlistEntry {
  movie_id: number;
  created_at: string;
  availability_notification_requested: boolean;
  availability_notified_at: string | null;
}

export interface MemberReview {
  id: number;
  movie_id: number;
  user_id: number;
  author: string;
  content: string;
  created_at: string;
  updated_at: string;
}

export interface ChatMessage {
  id: number;
  user_id: number;
  username: string;
  first_name: string;
  message: string;
  created_at: string;
  source: 'miniapp' | 'telegram';
  client_id: string;
  relay_status: 'sent' | 'pending';
  photo_url: string;
}

export interface UpcomingReminderEntry {
  tmdb_id: string;
  title: string;
  release_date: string;
  release_notification_requested: boolean;
  availability_notification_requested: boolean;
  release_notified_at: string | null;
  availability_notified_at: string | null;
}

export interface UserMovieRequest {
  id: number;
  user_id: number;
  username: string;
  first_name: string;
  title: string;
  priority: boolean;
  requested_at: string;
}

export interface RecommendationEvent {
  user_id: number;
  movie_id: number | null;
  event_type: string;
  source: string;
  metadata: Record<string, unknown>;
  created_at: string;
}

// In-memory state (resets on container restart)
export const watchlistByUser = new Map<number, Map<number, WatchlistEntry>>();
export const ratingsByMovie = new Map<number, Map<number, number>>();
export const reviewsByMovie = new Map<number, MemberReview[]>();
export const checkinDaysByUser = new Map<number, Set<string>>();
export const priorityTicketsByUser = new Map<number, number>();
export const upcomingRemindersByUser = new Map<number, Map<string, UpcomingReminderEntry>>();
export const movieRequests: UserMovieRequest[] = [];
export const recommendationEvents: RecommendationEvent[] = [];
export const tempLinks = new Map<string, { movie_id: number; movie_file_id: number | null; created_at: number }>();

let nextReviewId = 500;
let nextRequestId = 100;
let nextChatId = 1;

export const globalChatMessages: ChatMessage[] = [
  {
    id: nextChatId++,
    user_id: 9001,
    username: 'aarav_cinema',
    first_name: 'Aarav',
    message: 'Just finished Dune: Part Two in 4K — the sound design is unreal. Anyone have another sci-fi rec?',
    created_at: new Date(Date.now() - 42 * 60 * 1000).toISOString(),
    source: 'telegram',
    client_id: 'seed-chat-1',
    relay_status: 'sent',
    photo_url: '',
  },
  {
    id: nextChatId++,
    user_id: 9002,
    username: 'meera_watches',
    first_name: 'Meera',
    message: 'If you loved that scale, definitely put Interstellar or Your Name on your watchlist next!',
    created_at: new Date(Date.now() - 28 * 60 * 1000).toISOString(),
    source: 'miniapp',
    client_id: 'seed-chat-2',
    relay_status: 'sent',
    photo_url: '',
  },
  {
    id: nextChatId++,
    user_id: 9003,
    username: 'kabir_dev',
    first_name: 'Kabir',
    message: 'Panchayat Season 2 is still the best comfort watch on the entire catalogue.',
    created_at: new Date(Date.now() - 12 * 60 * 1000).toISOString(),
    source: 'miniapp',
    client_id: 'seed-chat-3',
    relay_status: 'sent',
    photo_url: '',
  },
];

// Seed initial community ratings & reviews so detail pages feel alive
function seedCommunityData(): void {
  const seedRatings: Array<[number, Array<[number, number]>]> = [
    [101, [[9001, 5], [9002, 5], [9003, 4]]],
    [102, [[9001, 5], [9002, 4]]],
    [105, [[9001, 5], [9003, 5]]],
    [106, [[9002, 5], [9003, 5]]],
    [107, [[9001, 5], [9002, 5], [9003, 5]]],
    [109, [[9002, 5], [9003, 5]]],
    [112, [[9001, 5], [9002, 5]]],
    [113, [[9001, 5], [9003, 4]]],
    [117, [[9002, 5], [9003, 5]]],
  ];
  for (const [movieId, entries] of seedRatings) {
    ratingsByMovie.set(movieId, new Map(entries));
  }

  const seedReviews: Array<Omit<MemberReview, 'id'>> = [
    {
      movie_id: 101,
      user_id: 9001,
      author: 'Aarav',
      content: 'Watched this again after years and it still hits just as hard. Brilliant blend of humor and heart.',
      created_at: '2026-08-15T14:20:00Z',
      updated_at: '2026-08-15T14:20:00Z',
    },
    {
      movie_id: 107,
      user_id: 9002,
      author: 'Meera',
      content: 'The pacing never lets up for a single minute. Still my favorite Gotham story ever put on screen.',
      created_at: '2026-09-02T19:10:00Z',
      updated_at: '2026-09-02T19:10:00Z',
    },
    {
      movie_id: 113,
      user_id: 9003,
      author: 'Kabir',
      content: 'Massive visual ambition paired with an intimate character arc. The desert sequences look razor sharp in 1080p.',
      created_at: '2026-10-18T11:45:00Z',
      updated_at: '2026-10-18T11:45:00Z',
    },
    {
      movie_id: 109,
      user_id: 9002,
      author: 'Meera',
      content: 'Grounded writing and effortless performances from the entire cast. Every episode feels like visiting old friends.',
      created_at: '2026-10-25T16:00:00Z',
      updated_at: '2026-10-25T16:00:00Z',
    },
  ];
  for (const item of seedReviews) {
    const list = reviewsByMovie.get(item.movie_id) || [];
    list.push({ id: nextReviewId++, ...item });
    reviewsByMovie.set(item.movie_id, list);
  }
}

seedCommunityData();

export function allocateReviewId(): number {
  return nextReviewId++;
}

export function allocateRequestId(): number {
  return nextRequestId++;
}

export function allocateChatId(): number {
  return nextChatId++;
}

export function getUserWatchlist(userId: number): Map<number, WatchlistEntry> {
  let map = watchlistByUser.get(userId);
  if (!map) {
    map = new Map<number, WatchlistEntry>();
    // Seed one saved title for the default browser preview user so My List isn't blank
    if (userId === 1001) {
      map.set(113, {
        movie_id: 113,
        created_at: new Date(Date.now() - 3600_000).toISOString(),
        availability_notification_requested: false,
        availability_notified_at: null,
      });
    }
    watchlistByUser.set(userId, map);
  }
  return map;
}

export function getUserCheckinDays(userId: number, todayIso: string): Set<string> {
  let days = checkinDaysByUser.get(userId);
  if (!days) {
    days = new Set<string>();
    // Seed a 3-day prior streak ending yesterday so the streak card looks engaging immediately
    const baseMs = new Date(`${todayIso}T00:00:00Z`).getTime();
    for (let ago = 1; ago <= 3; ago += 1) {
      days.add(new Date(baseMs - ago * 86_400_000).toISOString().slice(0, 10));
    }
    checkinDaysByUser.set(userId, days);
  }
  return days;
}
