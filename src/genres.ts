export interface CanonicalGenre {
  id: string;
  label: string;
  group: string;
  description: string;
}

export const CANONICAL_GENRES: CanonicalGenre[] = [
  { id: 'action', label: 'Action', group: 'Action & Adventure', description: 'Fast, explosive, and high-stakes viewing.' },
  { id: 'adventure', label: 'Adventure', group: 'Action & Adventure', description: 'Quest-driven stories and faraway journeys.' },
  { id: 'animation', label: 'Animation', group: 'Animation & Anime', description: 'Bold visual storytelling and inventive worlds.' },
  { id: 'anime', label: 'Anime', group: 'Animation & Anime', description: 'Anime, manga-inspired worlds and beloved fandoms.' },
  { id: 'biography', label: 'Biography', group: 'Drama & Documentary', description: 'Real-life stories with emotional depth and context.' },
  { id: 'comedy', label: 'Comedy', group: 'Comedy', description: 'Light, witty, and crowd-pleasing entertainment.' },
  { id: 'crime', label: 'Crime', group: 'Crime & Mystery', description: 'Sharp investigations, pressure, and power plays.' },
  { id: 'documentary', label: 'Documentary', group: 'Documentary & Reality', description: 'Truth, culture, and world-expanding nonfiction.' },
  { id: 'drama', label: 'Drama', group: 'Drama', description: 'Character-first stories with emotional weight.' },
  { id: 'erotic', label: 'Erotic', group: 'Adult / Mature', description: 'Mature, romantic, and explicit content.' },
  { id: 'family', label: 'Family', group: 'Family & Feel-Good', description: 'Warm and accessible storytelling for every age.' },
  { id: 'fantasy', label: 'Fantasy', group: 'Sci-Fi & Fantasy', description: 'Magic, myth, and imaginative wonder.' },
  { id: 'history', label: 'History', group: 'Drama & Documentary', description: 'Historic eras, legacies, and major turning points.' },
  { id: 'horror', label: 'Horror', group: 'Horror', description: 'Suspense, dread, and fear-driven cinema.' },
  { id: 'music', label: 'Music', group: 'Music & Culture', description: 'Performance, rhythm, and creative energy.' },
  { id: 'mystery', label: 'Mystery', group: 'Crime & Mystery', description: 'Clues, tension, and cinematic intrigue.' },
  { id: 'reality', label: 'Reality', group: 'Documentary & Reality', description: 'Authentic, unscripted, and high-interest stories.' },
  { id: 'romance', label: 'Romance', group: 'Romance', description: 'Emotional connection, chemistry, and longing.' },
  { id: 'scifi', label: 'Sci-Fi', group: 'Sci-Fi & Fantasy', description: 'Future worlds, tech, and speculative fiction.' },
  { id: 'short', label: 'Short', group: 'Short-form', description: 'Compact, sharp, and instantly watchable stories.' },
  { id: 'sport', label: 'Sport', group: 'Sport & Lifestyle', description: 'Competition, ambition, and underdog energy.' },
  { id: 'thriller', label: 'Thriller', group: 'Thriller', description: 'Tension, danger, and edge-of-the-seat suspense.' },
  { id: 'war', label: 'War', group: 'Action & Adventure', description: 'Conflict, endurance, and high-stakes battlefields.' },
  { id: 'western', label: 'Western', group: 'Classic & Cinematic', description: 'Dust, law, and frontier storytelling.' },
  { id: 'musical', label: 'Musical', group: 'Music & Culture', description: 'Songs, emotion, and performance-led stories.' },
  { id: 'cyberpunk', label: 'Cyberpunk', group: 'Sci-Fi & Fantasy', description: 'Neon futures, corporate tension, and dystopian energy.' },
  { id: 'dystopian', label: 'Dystopian', group: 'Sci-Fi & Fantasy', description: 'Oppressive worlds and fragile resistance.' },
  { id: 'space-opera', label: 'Space Opera', group: 'Sci-Fi & Fantasy', description: 'Epic cosmic scale and heroic journeys.' },
  { id: 'psychological-horror', label: 'Psychological Horror', group: 'Horror', description: 'Fear driven by the mind, guilt, and dread.' },
  { id: 'supernatural-horror', label: 'Supernatural Horror', group: 'Horror', description: 'Hauntings, curses, and unknown forces.' },
  { id: 'political-drama', label: 'Political Drama', group: 'Drama', description: 'Power, consequence, and high-pressure choices.' },
  { id: 'historical-drama', label: 'Historical Drama', group: 'Drama', description: 'Large-scale narratives shaped by eras and events.' },
  { id: 'romantic-comedy', label: 'Romantic Comedy', group: 'Comedy', description: 'Warm chemistry and easygoing fun.' },
  { id: 'dark-comedy', label: 'Dark Comedy', group: 'Comedy', description: 'Humor built on chaos, irony, and edge.' },
  { id: 'detective', label: 'Detective', group: 'Crime & Mystery', description: 'Clues, suspects, and moral puzzles.' },
  { id: 'procedural', label: 'Procedural', group: 'Crime & Mystery', description: 'Case-by-case tension and investigative structure.' },
  { id: 'heist', label: 'Heist', group: 'Action & Adventure', description: 'Strategy, stealing, and adrenaline.' },
  { id: 'survival', label: 'Survival', group: 'Action & Adventure', description: 'Danger, resilience, and impossible odds.' },
  { id: 'superhero', label: 'Superhero', group: 'Action & Adventure', description: 'Legend, power, and larger-than-life conflict.' },
  { id: 'spy', label: 'Spy', group: 'Action & Adventure', description: 'Espionage, stealth, and geopolitical thrill.' },
  { id: 'isekai', label: 'Isekai', group: 'Animation & Anime', description: 'Reborn into a new world and adventure unfolds.' },
  { id: 'shonen', label: 'Shonen', group: 'Animation & Anime', description: 'High energy, ambition, and action-driven arcs.' },
  { id: 'shojo', label: 'Shojo', group: 'Animation & Anime', description: 'Romance, emotion, and layered character arcs.' },
  { id: 'seinen', label: 'Seinen', group: 'Animation & Anime', description: 'Mature tone and layered themes.' },
  { id: 'kaiju', label: 'Kaiju', group: 'Animation & Anime', description: 'Monster-scale spectacle and destruction.' },
  { id: 'zombie', label: 'Zombie', group: 'Horror', description: 'Apocalypse, panic, and survival instincts.' },
  { id: 'vampire', label: 'Vampire', group: 'Horror', description: 'Dark romance, bloodlust, and nightfall.' },
];

export const GENRE_BY_ID: Record<string, CanonicalGenre> = Object.fromEntries(
  CANONICAL_GENRES.map((genre) => [genre.id, genre])
);

const GENRE_ALIASES: Record<string, Set<string>> = {
  action: new Set(['action', 'martial arts', 'superhero', 'spy', 'heist', 'survival', 'battle']),
  adventure: new Set(['adventure', 'quest', 'expedition', 'journey']),
  animation: new Set(['animation', 'animated', 'cartoons', 'cartoon']),
  anime: new Set(['anime', 'isekai', 'shonen', 'shojo', 'seinen', 'kaiju']),
  biography: new Set(['biography', 'bio']),
  comedy: new Set(['comedy', 'comdey', 'conedy', 'omedy', 'action comedy']),
  crime: new Set(['crime', 'cime', 'gangster', 'detective', 'procedural', 'heist']),
  documentary: new Set(['documentary', 'docudrama', 'real life']),
  drama: new Set(['drama', 'dram', 'drame', 'family drama', 'political drama', 'medical drama', 'legal drama', 'teen drama', 'coming-of-age']),
  erotic: new Set(['erotic']),
  family: new Set(['family', 'kids', 'children']),
  fantasy: new Set(['fantasy', 'myth', 'dark fantasy']),
  history: new Set(['history', 'histry', 'historical', 'period']),
  horror: new Set(['horror', 'slasher', 'monster', 'zombie', 'vampire', 'haunting']),
  music: new Set(['music']),
  mystery: new Set(['mystery', 'investigation', 'whodunit', 'detective']),
  reality: new Set(['reality', 'reality show', 'talk show', 'game show']),
  romance: new Set(['romance', 'romantic', 'love story']),
  scifi: new Set(['sci-fi', 'sci fi', 'science fiction', 'science-fiction', 'cyberpunk', 'dystopian', 'time travel', 'space opera', 'mecha']),
  short: new Set(['short', 'short film']),
  sport: new Set(['sport', 'sports', 'wrestling']),
  thriller: new Set(['thriller', 'suspense', 'psychological', 'crime thriller']),
  war: new Set(['war', 'war action thriller']),
  western: new Set(['western', 'frontier']),
  musical: new Set(['musical', 'song']),
  cyberpunk: new Set(['cyberpunk']),
  dystopian: new Set(['dystopian']),
  'space-opera': new Set(['space opera', 'space-opera']),
  'psychological-horror': new Set(['psychological horror', 'psychological-horror']),
  'supernatural-horror': new Set(['supernatural horror', 'supernatural-horror']),
  'political-drama': new Set(['political drama', 'political-drama']),
  'historical-drama': new Set(['historical drama', 'historical-drama']),
  'romantic-comedy': new Set(['romantic comedy', 'romantic-comedy']),
  'dark-comedy': new Set(['dark comedy', 'dark-comedy']),
  detective: new Set(['detective']),
  procedural: new Set(['procedural']),
  heist: new Set(['heist']),
  survival: new Set(['survival']),
  superhero: new Set(['superhero']),
  spy: new Set(['spy', 'espionage']),
  isekai: new Set(['isekai']),
  shonen: new Set(['shonen']),
  shojo: new Set(['shojo']),
  seinen: new Set(['seinen']),
  kaiju: new Set(['kaiju']),
  zombie: new Set(['zombie']),
  vampire: new Set(['vampire']),
};

const GENRE_GARBAGE = new Set([
  '', 'n/a', 'na', 'a', 'n', 'unknown', 'dubbed', 'bluray', 'hdcam', 'hdrip', 'hdtc', 'hdts', 'web-dl',
]);

export const MOOD_GENRES: Record<string, Set<string>> = {
  any: new Set(),
  fun: new Set(['comedy', 'animation', 'animated', 'family', 'musical']),
  mind: new Set(['thriller', 'mystery', 'crime', 'sci-fi', 'sci fi', 'science fiction', 'detective']),
  action: new Set(['action', 'adventure', 'war', 'fantasy', 'superhero']),
  heart: new Set(['romance', 'romantic', 'drama', 'family']),
  anime: new Set(['anime', 'animation', 'animated', 'isekai', 'shonen', 'shojo', 'seinen']),
};

export function normalizeCatalogueGenres(rawValue: string | null | undefined): Set<string> {
  if (!rawValue || typeof rawValue !== 'string') return new Set();
  const result = new Set<string>();
  for (const token of rawValue.split(/[,&/|]/)) {
    let value = token.replace(/[–—]/g, '-').replace(/\s+/g, ' ').trim().toLowerCase();
    value = value.replace(/[^a-z0-9\s-]/g, ' ').replace(/\s+/g, ' ').trim();
    if (!value || GENRE_GARBAGE.has(value) || value.startsWith('quality ')) continue;
    for (const [genreId, aliases] of Object.entries(GENRE_ALIASES)) {
      if (aliases.has(value)) result.add(genreId);
    }
    if (value.includes('science fiction') || value === 'sci fi') result.add('scifi');
    if (value.includes('dark comedy')) result.add('dark-comedy');
    if (value.includes('romantic comedy')) result.add('romantic-comedy');
    if (value.includes('psychological horror')) result.add('psychological-horror');
    if (value.includes('supernatural horror')) result.add('supernatural-horror');
    if (value.includes('historical drama')) result.add('historical-drama');
    if (value.includes('political drama')) result.add('political-drama');
  }
  return result;
}

export function isCatalogueTv(
  category?: string | null,
  seasonsData?: Record<string, unknown> | null,
  contentType?: string | null
): boolean {
  const ct = String(contentType || '').trim().toLowerCase();
  const cat = String(category || '').trim().toLowerCase();
  return (
    ct === 'web series' ||
    ct === 'tv series' ||
    cat === 'web series' ||
    (Boolean(seasonsData) && typeof seasonsData === 'object' && Object.keys(seasonsData!).length > 0)
  );
}

export function browseTypeMatches(
  browseType: string,
  category?: string | null,
  seasonsData?: Record<string, unknown> | null,
  contentType?: string | null
): boolean {
  if (browseType === 'all') return true;
  const tv = isCatalogueTv(category, seasonsData, contentType);
  return browseType === 'tv' ? tv : !tv;
}
