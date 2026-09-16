document.body.classList.add('app-booting');
const tg = window.Telegram?.WebApp || {
            expand() {}, ready() {}, close() {}, openLink(url) { window.open(url, '_blank'); },
            HapticFeedback: { notificationOccurred() {}, impactOccurred() {} }, initDataUnsafe: {}, initData: ''
        };
        tg.expand();
        tg.ready();
        const BOT_USERNAME = "FlimfyBoxBot"; // change if needed

        // State
        let allMovies = [];
        let tmdbMoviesMap = {};
        let activeMovie = null;
        let savedMovieIds = new Set();
        let myListRequestId = 0;
        let detailsRequestId = 0;
        let heroTimer = null;
        let heroItems = [];
        let heroIndex = 0;
        let newReleaseRequestId = 0;
        let trendingRequestId = 0;
        let genreCatalog = null;
        let genreCatalogType = null;
        let genreCatalogRequestId = 0;
        let genreDetailRequestId = 0;
        let genreDetailMovies = [];
        let genreDetailVisibleCount = 24;
        let genreDetailActiveId = null;
        let browseType = 'all';
        let browseRequestId = 0;
        let globalChatTimer = null;
        let initialHomePending = 0;

        function startInitialHomeLoading() {
            initialHomePending = 3;
            document.body.classList.add('app-booting');
            const screen = document.getElementById('appLoadingScreen');
            if (screen) screen.classList.remove('is-complete');
        }

        function completeInitialHomeLoadingStep() {
            if (initialHomePending <= 0) return;
            initialHomePending -= 1;
            if (initialHomePending > 0) return;
            document.body.classList.remove('app-booting');
            const screen = document.getElementById('appLoadingScreen');
            if (screen) screen.classList.add('is-complete');
        }

        const CANONICAL_GENRE_TAXONOMY = [
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
            { id: 'fantasy', label: 'Fantasy', group: 'Sci‑Fi & Fantasy', description: 'Magic, myth, and imaginative wonder.' },
            { id: 'history', label: 'History', group: 'Drama & Documentary', description: 'Historic eras, legacies, and major turning points.' },
            { id: 'horror', label: 'Horror', group: 'Horror', description: 'Suspense, dread, and fear-driven cinema.' },
            { id: 'music', label: 'Music', group: 'Music & Culture', description: 'Performance, rhythm, and creative energy.' },
            { id: 'mystery', label: 'Mystery', group: 'Crime & Mystery', description: 'Clues, tension, and cinematic intrigue.' },
            { id: 'reality', label: 'Reality', group: 'Documentary & Reality', description: 'Authentic, unscripted, and high-interest stories.' },
            { id: 'romance', label: 'Romance', group: 'Romance', description: 'Emotional connection, chemistry, and longing.' },
            { id: 'scifi', label: 'Sci‑Fi', group: 'Sci‑Fi & Fantasy', description: 'Future worlds, tech, and speculative fiction.' },
            { id: 'short', label: 'Short', group: 'Short-form', description: 'Compact, sharp, and instantly watchable stories.' },
            { id: 'sport', label: 'Sport', group: 'Sport & Lifestyle', description: 'Competition, ambition, and underdog energy.' },
            { id: 'thriller', label: 'Thriller', group: 'Thriller', description: 'Tension, danger, and edge-of-the-seat suspense.' },
            { id: 'war', label: 'War', group: 'Action & Adventure', description: 'Conflict, endurance, and high-stakes battlefields.' },
            { id: 'western', label: 'Western', group: 'Classic & Cinematic', description: 'Dust, law, and frontier storytelling.' },
            { id: 'musical', label: 'Musical', group: 'Music & Culture', description: 'Songs, emotion, and performance-led stories.' },
            { id: 'cyberpunk', label: 'Cyberpunk', group: 'Sci‑Fi & Fantasy', description: 'Neon futures, corporate tension, and dystopian energy.' },
            { id: 'dystopian', label: 'Dystopian', group: 'Sci‑Fi & Fantasy', description: 'Oppressive worlds and fragile resistance.' },
            { id: 'space-opera', label: 'Space Opera', group: 'Sci‑Fi & Fantasy', description: 'Epic cosmic scale and heroic journeys.' },
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
            { id: 'anime-genre', label: 'Anime', group: 'Animation & Anime', description: 'Animated storytelling with massive cultural reach.' },
            { id: 'isekai', label: 'Isekai', group: 'Animation & Anime', description: 'Reborn into a new world and adventure unfolds.' },
            { id: 'shonen', label: 'Shonen', group: 'Animation & Anime', description: 'High energy, ambition, and action-driven arcs.' },
            { id: 'shojo', label: 'Shojo', group: 'Animation & Anime', description: 'Romance, emotion, and beautifully layered character arcs.' },
            { id: 'seinen', label: 'Seinen', group: 'Animation & Anime', description: 'Mature tone and layered themes.' },
            { id: 'kaiju', label: 'Kaiju', group: 'Animation & Anime', description: 'Monster-scale spectacle and destruction.' },
            { id: 'zombie', label: 'Zombie', group: 'Horror', description: 'Apocalypse, panic, and survival instincts.' },
            { id: 'vampire', label: 'Vampire', group: 'Horror', description: 'Dark romance, bloodlust, and nightfall.' }
        ];

        const CANONICAL_GENRE_MAP = Object.fromEntries(CANONICAL_GENRE_TAXONOMY.map(genre => [genre.id, genre]));
        const GENRE_GROUP_ORDER = ['Action & Adventure', 'Crime & Mystery', 'Sci‑Fi & Fantasy', 'Horror', 'Drama', 'Comedy', 'Animation & Anime', 'Documentary & Reality', 'Music & Culture', 'Romance', 'Family & Feel-Good', 'Sport & Lifestyle', 'Short-form', 'Classic & Cinematic', 'Adult / Mature'];
        const MOBILE_FEATURED_GENRE_ORDER = ['action', 'comedy', 'drama', 'horror', 'animation', 'thriller', 'romance', 'scifi', 'crime', 'fantasy'];
        const GENRE_VISUAL_PALETTE = {
            'Action & Adventure': { from: '#f97316', to: '#7c2d12', glow: 'rgba(249, 115, 22, .38)' },
            'Crime & Mystery': { from: '#38bdf8', to: '#0f172a', glow: 'rgba(59, 130, 246, .28)' },
            'Sci‑Fi & Fantasy': { from: '#8b5cf6', to: '#0f172a', glow: 'rgba(139, 92, 246, .35)' },
            'Horror': { from: '#ef4444', to: '#111827', glow: 'rgba(239, 68, 68, .35)' },
            'Drama': { from: '#f59e0b', to: '#3f3f46', glow: 'rgba(245, 158, 11, .25)' },
            'Comedy': { from: '#f472b6', to: '#4c1d95', glow: 'rgba(244, 114, 182, .28)' },
            'Animation & Anime': { from: '#22c55e', to: '#0f766e', glow: 'rgba(34, 197, 94, .24)' },
            'Documentary & Reality': { from: '#a78bfa', to: '#1e293b', glow: 'rgba(167, 139, 250, .24)' },
            'Music & Culture': { from: '#fb7185', to: '#312e81', glow: 'rgba(251, 113, 133, .24)' },
            'Romance': { from: '#f472b6', to: '#7c2d12', glow: 'rgba(244, 114, 182, .25)' },
            'Family & Feel-Good': { from: '#34d399', to: '#14532d', glow: 'rgba(52, 211, 153, .22)' },
            'Sport & Lifestyle': { from: '#14b8a6', to: '#0f172a', glow: 'rgba(20, 184, 166, .24)' },
            'Short-form': { from: '#38bdf8', to: '#1d4ed8', glow: 'rgba(56, 189, 248, .22)' },
            'Classic & Cinematic': { from: '#c084fc', to: '#312e81', glow: 'rgba(192, 132, 252, .24)' },
            'Adult / Mature': { from: '#f87171', to: '#3f3f46', glow: 'rgba(248, 113, 113, .2)' }
        };
        // Stable curated poster assignment. These fixed, recognizable movie
        // key-art URLs never read catalogue movies, counts, or API results.
        const GENRE_ARTWORK = {
            action: 'https://upload.wikimedia.org/wikipedia/en/6/6e/Mad_Max_Fury_Road.jpg',
            adventure: 'https://upload.wikimedia.org/wikipedia/en/e/e7/Jurassic_Park_poster.jpg',
            animation: 'https://upload.wikimedia.org/wikipedia/en/1/13/Toy_Story.jpg',
            anime: 'https://upload.wikimedia.org/wikipedia/en/d/db/Spirited_Away_Japanese_poster.png',
            'anime-genre': 'https://m.media-amazon.com/images/M/MV5BMjI1ODZkYTgtYTY3Yy00ZTJkLWFkOTgtZDUyYWM4MzQwNjk0XkEyXkFqcGc@._V1_.jpg',
            biography: 'https://upload.wikimedia.org/wikipedia/en/9/9f/Bohemian_Rhapsody.png',
            comedy: 'https://upload.wikimedia.org/wikipedia/en/b/b9/Hangoverposter09.jpg',
            crime: 'https://upload.wikimedia.org/wikipedia/en/1/1c/Godfather_ver1.jpg',
            documentary: 'https://upload.wikimedia.org/wikipedia/en/9/9c/Free_Solo.png',
            drama: 'https://upload.wikimedia.org/wikipedia/en/b/b8/A_Beautiful_Mind_Poster.jpg',
            erotic: 'https://upload.wikimedia.org/wikipedia/en/5/5e/50ShadesofGreyCoverArt.jpg',
            family: 'https://m.media-amazon.com/images/M/MV5BMTAxOTMwOTkwNDZeQTJeQWpwZ15BbWU4MDEyMTI1NjMx._V1_.jpg',
            fantasy: 'https://upload.wikimedia.org/wikipedia/en/f/fb/Lord_Rings_Fellowship_Ring.jpg',
            history: 'https://upload.wikimedia.org/wikipedia/en/3/38/Schindler%27s_List_movie.jpg',
            horror: 'https://upload.wikimedia.org/wikipedia/en/7/7b/Exorcist_ver2.jpg',
            music: 'https://m.media-amazon.com/images/M/MV5BMDFjOWFkYzktYzhhMC00NmYyLTkwY2EtYjViMDhmNzg0OGFkXkEyXkFqcGc@._V1_.jpg',
            mystery: 'https://upload.wikimedia.org/wikipedia/en/1/1f/Knives_Out_poster.jpeg',
            reality: 'https://upload.wikimedia.org/wikipedia/en/c/cd/Trumanshow.jpg',
            romance: 'https://upload.wikimedia.org/wikipedia/en/8/86/Posternotebook.jpg',
            scifi: 'https://upload.wikimedia.org/wikipedia/en/b/b4/Spider-Man-_Across_the_Spider-Verse_poster.jpg',
            short: 'https://upload.wikimedia.org/wikipedia/en/8/89/Le_ballon_rouge_%281956%29.png',
            sport: 'https://upload.wikimedia.org/wikipedia/en/1/18/Rocky_poster.jpg',
            thriller: 'https://m.media-amazon.com/images/M/MV5BMTk0MDQ3MzAzOV5BMl5BanBnXkFtZTgwNzU1NzE3MjE@._V1_.jpg',
            war: 'https://m.media-amazon.com/images/M/MV5BYzkxZjg2NDQtMGVjMy00NWZkLTk0ZDEtZWE3NDYwYjAyMTg1XkEyXkFqcGc@._V1_.jpg',
            western: 'https://upload.wikimedia.org/wikipedia/en/4/45/Good_the_bad_and_the_ugly_poster.jpg',
            musical: 'https://m.media-amazon.com/images/M/MV5BMDllYjliOTUtMDJjZC00ODIzLWJmNGMtOWI2NzQxMjA2NzdlXkEyXkFqcGc@._V1_.jpg',
            cyberpunk: 'https://m.media-amazon.com/images/M/MV5BOWQ4YTBmNTQtMDYxMC00NGFjLTkwOGQtNzdhNmY1Nzc1MzUxXkEyXkFqcGc@._V1_.jpg',
            dystopian: 'https://m.media-amazon.com/images/M/MV5BMWI1OGM4YjQtNmIxNi00YmE2LWJkNTAtY2Q0YjU4NTI5NWQyXkEyXkFqcGc@._V1_.jpg',
            'space-opera': 'https://m.media-amazon.com/images/M/MV5BYjRkYzAzNjktZmRhMy00NjRiLWE0OTMtYmRmMTE5NDkzY2NlXkEyXkFqcGc@._V1_.jpg',
            'psychological-horror': 'https://m.media-amazon.com/images/M/MV5BNzY2NzI4OTE5MF5BMl5BanBnXkFtZTcwMjMyNDY4Mw@@._V1_.jpg',
            'supernatural-horror': 'https://m.media-amazon.com/images/M/MV5BMTM3NjA1NDMyMV5BMl5BanBnXkFtZTcwMDQzNDMzOQ@@._V1_.jpg',
            'political-drama': 'https://m.media-amazon.com/images/M/MV5BZGQzMzcwMDYtMmNjNS00YzZlLTg2MjUtNTE0MThlNTFjMDQ0XkEyXkFqcGc@._V1_.jpg',
            'historical-drama': 'https://m.media-amazon.com/images/M/MV5BYWQ4YmNjYjEtOWE1Zi00Y2U4LWI4NTAtMTU0MjkxNWQ1ZmJiXkEyXkFqcGc@._V1_.jpg',
            'romantic-comedy': 'https://m.media-amazon.com/images/M/MV5BMTYxNDMyOTAxN15BMl5BanBnXkFtZTgwMDg1ODYzNTM@._V1_.jpg',
            'dark-comedy': 'https://m.media-amazon.com/images/M/MV5BMzM5NjUxOTEyMl5BMl5BanBnXkFtZTgwNjEyMDM0MDE@._V1_.jpg',
            detective: 'https://m.media-amazon.com/images/M/MV5BMTg0NjEwNjUxM15BMl5BanBnXkFtZTcwMzk0MjQ5Mg@@._V1_.jpg',
            procedural: 'https://m.media-amazon.com/images/M/MV5BNDFkMTRkZmQtM2I0NC00NjJjLWJlMDctNTNiZWYxYzhjZDZiXkEyXkFqcGc@._V1_.jpg',
            heist: 'https://m.media-amazon.com/images/M/MV5BMmNhZDkxYTgtMDM3ZC00NTQ3LWFjZTUtNzc1Y2QyNWZjNDRmXkEyXkFqcGc@._V1_.jpg',
            survival: 'https://m.media-amazon.com/images/M/MV5BYTgwNmQzZDctMjNmOS00OTExLTkwM2UtNzJmOTJhODFjOTdlXkEyXkFqcGc@._V1_.jpg',
            superhero: 'https://upload.wikimedia.org/wikipedia/en/1/1c/The_Dark_Knight_%282008_film%29.jpg',
            spy: 'https://m.media-amazon.com/images/M/MV5BMWQ1ZDM4NDktMWY0NC00MjcxLWJlMDMtNmE2MGVhYzRjMWQ0XkEyXkFqcGc@._V1_.jpg',
            isekai: 'https://m.media-amazon.com/images/M/MV5BN2NhYzU2NDEtYzI1NS00MjgzLThjZGUtOTYxNGJkZjZmNDdjXkEyXkFqcGc@._V1_.jpg',
            shonen: 'https://m.media-amazon.com/images/M/MV5BMTA5MTc1M2EtZWQ2Ni00ZmU2LTg3MzQtOTliMjE4OGM0ZWFiXkEyXkFqcGc@._V1_.jpg',
            shojo: 'https://m.media-amazon.com/images/M/MV5BZTEyZDhlNDctMGMyNy00YTczLTgyMjktNGRjMDI2MzM0YTU5XkEyXkFqcGc@._V1_.jpg',
            seinen: 'https://m.media-amazon.com/images/M/MV5BNzljMjA3MTQtMjM1OS00OGJjLWJiYzctZDRiMTk1NWI5YzQ5XkEyXkFqcGc@._V1_.jpg',
            kaiju: 'https://m.media-amazon.com/images/M/MV5BODE2NTdmMmYtY2U1OS00MjExLWIwNjQtYjQ5NTA0ZDZmZjZiXkEyXkFqcGc@._V1_.jpg',
            zombie: 'https://m.media-amazon.com/images/M/MV5BODA3OTM4NWQtMjU1OS00NzA2LTlhZDAtYjU2YjM5MWUxNTdiXkEyXkFqcGc@._V1_.jpg',
            vampire: 'https://m.media-amazon.com/images/M/MV5BNjY4NDlkMzctMzRmZC00YWZjLTg3MGItZTI2M2NkZjg3YTIxXkEyXkFqcGc@._V1_.jpg'
        };

        function getGenreVisualPalette(groupName) {
            return GENRE_VISUAL_PALETTE[groupName] || { from: '#8b7bff', to: '#0f172a', glow: 'rgba(139, 123, 255, .24)' };
        }

        const GENRE_ALIASES = {
            action: ['action', 'martial arts', 'martial-arts', 'superhero', 'spy', 'heist', 'survival', 'war action', 'war-action', 'battle'],
            adventure: ['adventure', 'quest', 'expedition', 'journey'],
            animation: ['animation', 'animated', 'cartoons', 'cartoon'],
            anime: ['anime', 'anime genre', 'isekai', 'shonen', 'shojo', 'seinen', 'kaiju'],
            biography: ['biography', 'bio'],
            comedy: ['comedy', 'dark comedy', 'romantic comedy', 'sitcom', 'sketch comedy', 'action comedy', 'comdey', 'conedy', 'omedy', 'comedy drama', 'comedy. drama'],
            crime: ['crime', 'cime', 'gangster', 'detective', 'procedural', 'heist'],
            documentary: ['documentary', 'docudrama', 'real life', 'reality documentary'],
            drama: ['drama', 'dram', 'drame', 'family drama', 'political drama', 'medical drama', 'legal drama', 'historical drama', 'coming-of-age', 'teen drama'],
            erotic: ['erotic', 'adult', 'hot', 'unrated', '18+'],
            family: ['family', 'kids', 'children'],
            fantasy: ['fantasy', 'myth', 'dark fantasy'],
            history: ['history', 'histry', 'historical', 'period'],
            horror: ['horror', 'psychological horror', 'supernatural horror', 'slasher', 'monster', 'zombie', 'vampire', 'haunting'],
            music: ['music', 'musical'],
            mystery: ['mystery', 'investigation', 'whodunit', 'detective'],
            reality: ['reality', 'reality show', 'reality-tv', 'reality tv', 'tv shows', 'talk show', 'game show', 'talk-show', 'game-show', 'sitcom', 'reality-series'],
            romance: ['romance', 'romantic', 'love story', 'love-story'],
            scifi: ['sci-fi', 'sci fi', 'science fiction', 'science-fiction', 'cyberpunk', 'dystopian', 'time travel', 'space opera', 'mecha'],
            short: ['short', 'short film'],
            sport: ['sport', 'sports', 'wrestling'],
            thriller: ['thriller', 'suspense', 'psychological', 'crime thriller'],
            war: ['war', 'war action thriller'],
            western: ['western', 'frontier'],
            musical: ['musical', 'song'],
            cyberpunk: ['cyberpunk'],
            dystopian: ['dystopian'],
            'space-opera': ['space opera', 'space-opera'],
            'psychological-horror': ['psychological horror', 'psychological-horror'],
            'supernatural-horror': ['supernatural horror', 'supernatural-horror'],
            'political-drama': ['political drama', 'political-drama'],
            'historical-drama': ['historical drama', 'historical-drama'],
            'romantic-comedy': ['romantic comedy', 'romantic-comedy'],
            'dark-comedy': ['dark comedy', 'dark-comedy'],
            detective: ['detective'],
            procedural: ['procedural'],
            heist: ['heist'],
            survival: ['survival'],
            superhero: ['superhero'],
            spy: ['spy', 'espionage'],
            'isekai': ['isekai'],
            shonen: ['shonen'],
            shojo: ['shojo'],
            seinen: ['seinen'],
            kaiju: ['kaiju'],
            zombie: ['zombie'],
            vampire: ['vampire']
        };

        const KNOWN_GENRE_GARBAGE = new Set([
            '', 'n/a', 'na', 'a', 'n', 'unknown', 'quality : bluray', 'quality : hdcam', 'quality : hdrip',
            'quality : hdtc', 'quality : hdts', 'quality : web-dl', 'bluray', 'hdcam', 'hdrip', 'hdtc', 'hdts',
            'web-dl', 'dubbed', 'random', 'kids', 'adult', 'hot', 'unrated', 'political', 'social', 'devotional',
            'survival', 'spy', 'turkish', 'john putch', 'simran choudhary', 'nandu vijay krishna', 'stars: chammak chandra',
            'cime', 'comdey', 'conedy', 'omedy', 'com�dieetthriller', 'reality tv', 'reality-tv', 'talk-show', 'talk show', 'game-show',
            'game show', 'tv-shows', 'tv shows', 'tv reality show', 'reality'
        ]);

        function normalizeGenreText(rawText) {
            if (!rawText || typeof rawText !== 'string') return [];
            const tokens = rawText.split(/[,&/|]/).map(part => part.trim()).filter(Boolean);
            const canonical = new Set();
            tokens.forEach(token => {
                let value = token
                    .replace(/[–—]/g, '-')
                    .replace(/[\u2018\u2019]/g, "'")
                    .replace(/\s+/g, ' ')
                    .trim()
                    .toLowerCase();
                value = value.replace(/\.+/g, ' ').replace(/[^a-z0-9\s-]/g, ' ');
                value = value.replace(/\s+/g, ' ').trim();
                if (!value || KNOWN_GENRE_GARBAGE.has(value)) return;
                const matched = Object.keys(GENRE_ALIASES).find(key => GENRE_ALIASES[key].includes(value));
                if (matched) {
                    canonical.add(matched);
                    return;
                }
                if (value.includes('science fiction') || value.includes('sci fi')) {
                    canonical.add('scifi');
                    return;
                }
                if (value.includes('dark comedy')) {
                    canonical.add('dark-comedy');
                    return;
                }
                if (value.includes('political drama')) {
                    canonical.add('political-drama');
                    return;
                }
                if (value.includes('historical drama')) {
                    canonical.add('historical-drama');
                    return;
                }
                if (value.includes('romantic comedy')) {
                    canonical.add('romantic-comedy');
                    return;
                }
                if (value.includes('supernatural horror')) {
                    canonical.add('supernatural-horror');
                    return;
                }
                if (value.includes('psychological horror')) {
                    canonical.add('psychological-horror');
                    return;
                }
                if (value.includes('space opera')) {
                    canonical.add('space-opera');
                    return;
                }
                if (value.includes('cyberpunk')) {
                    canonical.add('cyberpunk');
                }
            });
            return Array.from(canonical);
        }

        function findGenreById(genreId) {
            return CANONICAL_GENRE_TAXONOMY.find(genre => genre.id === genreId) || CANONICAL_GENRE_TAXONOMY[0];
        }

        async function loadGenreCatalog(force = false) {
            const requestedType = browseType;
            if (genreCatalog && genreCatalogType === requestedType && !force) return genreCatalog;
            const requestId = ++genreCatalogRequestId;
            const response = await fetch(`/api/genres?type=${encodeURIComponent(requestedType)}`);
            const data = await response.json();
            if (!response.ok || data.status !== 'success') throw new Error(data.message || 'Could not load genres');
            if (requestId !== genreCatalogRequestId || requestedType !== browseType) return null;
            genreCatalog = Object.fromEntries((data.genres || []).map(genre => [genre.id, genre]));
            genreCatalogType = requestedType;
            return genreCatalog;
        }

        async function loadBrowseCollections() {
            const requestId = ++browseRequestId;
            const container = document.getElementById('browseCollections');
            container.innerHTML = '<div class="genre-loading"><div class="loader"></div><span>Curating decade collections…</span></div>';
            try {
                const response = await fetch(`/api/browse?type=${encodeURIComponent(browseType)}`);
                const data = await response.json();
                if (requestId !== browseRequestId) return;
                if (!response.ok || data.status !== 'success') throw new Error(data.message || 'Could not load collections');
                const collectionCaption = browseType === 'tv'
                    ? 'Web series & TV shows'
                    : browseType === 'movies'
                        ? 'Movies only'
                        : 'Movies, series & TV';
                container.innerHTML = (data.collections || []).filter(collection => collection.movies.length).map(collection => `
                    <section class="browse-collection movie-row">
                        <div class="row-header"><div class="row-header-left">${collection.label}</div><span class="row-caption">${collectionCaption}</span></div>
                        <div class="horizontal-scroll">${renderCards(collection.movies, 'card', false)}</div>
                    </section>
                `).join('');
            } catch (error) {
                if (requestId === browseRequestId) container.innerHTML = `<div class="empty-search-state">${error.message}</div>`;
            }
        }

        window.setBrowseType = async function(type) {
            if (!['all', 'movies', 'tv'].includes(type) || type === browseType && genreCatalogType === type) return;
            browseType = type;
            genreCatalog = null;
            genreCatalogType = null;
            genreCatalogRequestId++;
            genreDetailVisibleCount = 24;
            document.querySelectorAll('[data-browse-type]').forEach(button => {
                button.classList.toggle('active', button.dataset.browseType === browseType);
            });
            document.getElementById('browseCollections').innerHTML = '';
            renderExploreScreen();
            await loadBrowseCollections();
            if (document.getElementById('genreDetailPage')?.classList.contains('open') && genreDetailActiveId) {
                await window.openGenreDetail(genreDetailActiveId);
            }
        };

        window.surpriseMe = async function() {
            const result = document.getElementById('surpriseResult');
            result.textContent = 'Finding a real title in the catalogue…';
            try {
                const response = await fetch(`/api/browse/surprise?type=${encodeURIComponent(browseType)}`);
                const data = await response.json();
                if (!response.ok || data.status !== 'success') throw new Error(data.message || 'Could not find a title');
                if (!data.movie) {
                    result.textContent = data.message || 'No eligible local titles are available.';
                    return;
                }
                const movie = data.movie;
                if (!allMovies.some(item => String(item.id) === String(movie.id))) allMovies.push(movie);
                result.innerHTML = `<strong>${movie.title}</strong><span>${movie.year || ''} · ${movie.category || ''}</span>`;
                result.onclick = () => openCardDetails(movie, false);
                result.classList.add('is-clickable');
            } catch (error) {
                result.textContent = error.message || 'Could not find a title';
            }
        };

        // Utility
        function showToast(msg) {
            const t = document.getElementById('toast');
            t.innerText = msg;
            t.classList.add('show');
            setTimeout(() => t.classList.remove('show'), 2500);
        }

        function scrollRow(elementId, amount) {
            const el = document.getElementById(elementId);
            if (el) el.scrollBy({ left: amount, behavior: 'smooth' });
        }

        function telegramAuthHeaders() {
            return tg.initData ? { 'X-Telegram-Init-Data': tg.initData } : {};
        }

        function setActiveNav(index) {
            document.querySelectorAll('.nav-item').forEach((item, itemIndex) => {
                item.classList.toggle('active', itemIndex === index);
            });
        }

        window.openMorePanel = function() {
            document.getElementById('morePanel').classList.add('open');
            document.querySelectorAll('.nav-item').forEach(item => item.classList.remove('active'));
        };

        window.closeMorePanel = function() {
            document.getElementById('morePanel').classList.remove('open');
        };

        window.closeInfoModal = function() {
            document.getElementById('infoModal').classList.remove('open');
            if (globalChatTimer) {
                clearInterval(globalChatTimer);
                globalChatTimer = null;
            }
        };

        function setupMotionEffects() {
            if (!('IntersectionObserver' in window)) return;
            const observer = new IntersectionObserver(entries => {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        entry.target.classList.add('is-revealed');
                        observer.unobserve(entry.target);
                    }
                });
            }, { threshold: 0.12, rootMargin: '0px 0px -30px' });
            const observeRows = () => document.querySelectorAll('.movie-row:not(.is-revealed)').forEach(row => {
                observer.observe(row);
                row.querySelectorAll('.card').forEach(card => card.classList.add('is-revealed'));
            });
            observeRows();
            new MutationObserver(observeRows).observe(document.getElementById('mainContent'), { childList: true, subtree: true });
        }

        window.showInfoPanel = function(title, body, icon = 'circle-info', actions = '') {
            closeMorePanel();
            document.getElementById('infoModalTitle').textContent = title;
            document.getElementById('infoModalBody').innerHTML = body;
            document.getElementById('infoModalIcon').innerHTML = `<i class="fas fa-${icon}"></i>`;
            document.getElementById('infoModalActions').innerHTML = actions || '<button type="button" class="primary-action" onclick="closeInfoModal()">Done</button>';
            document.getElementById('infoModal').classList.add('open');
        };

        window.showTvSeries = function() {
            closeMorePanel();
            showExplore();
            window.setBrowseType('tv');
        };

        window.showUpcoming = function() {
            closeMorePanel();
            showInfoPanel('Upcoming titles', '<div class="upcoming-list"><div class="loader">Loading TMDB releases…</div></div>', 'calendar-plus', '<button type="button" class="primary-action" onclick="closeInfoModal()">Close</button>');
            fetch('/api/upcoming?limit=18')
                .then(response => response.json())
                .then(data => {
                    if (data.status !== 'success') throw new Error(data.message || 'Could not load upcoming titles');
                    const list = data.movies || [];
                    document.querySelector('.upcoming-list').innerHTML = list.length
                        ? list.map(item => `<div class="upcoming-item"><img src="${escapeHtml(item.image)}" alt=""><div><strong>${escapeHtml(item.title)}</strong><span>${escapeHtml(item.category)} · ${escapeHtml(item.release_date)}</span></div></div>`).join('')
                        : '<p>No upcoming titles found right now.</p>';
                })
                .catch(error => {
                    document.querySelector('.upcoming-list').innerHTML = `<p>${error.message}</p>`;
                });
        };

        window.showGlobalChat = function() {
            closeMorePanel();
            if (globalChatTimer) clearInterval(globalChatTimer);
            showInfoPanel('Global Chat', '<div class="chat-shell"><div class="chat-messages"><div class="loader">Loading messages…</div></div><form class="chat-form" onsubmit="sendGlobalChat(event)"><input class="chat-input" maxlength="500" placeholder="Write a message…" required><button class="primary-action" type="submit">Send</button></form></div>', 'comment', '<button type="button" class="primary-action" onclick="closeInfoModal()">Close</button>');
            loadGlobalChat();
            globalChatTimer = setInterval(loadGlobalChat, 5000);
        };

        window.loadGlobalChat = function() {
            fetch('/api/global-chat', { headers: telegramAuthHeaders() })
                .then(response => response.json().then(data => ({ ok: response.ok, data })))
                .then(({ ok, data }) => {
                    if (!ok) throw new Error(data.message || 'Open this chat inside Telegram.');
                    const messages = document.querySelector('.chat-messages');
                    if (!messages) return;
                    messages.innerHTML = (data.messages || []).map(item => {
                        const author = item.username ? `@${item.username}` : item.first_name;
                        return `<div class="chat-message"><div class="chat-author">${author}</div><div class="chat-text">${escapeHtml(item.message)}</div></div>`;
                    }).join('') || '<p>No messages yet. Start the conversation.</p>';
                    messages.scrollTop = messages.scrollHeight;
                })
                .catch(error => {
                    const messages = document.querySelector('.chat-messages');
                    if (messages) messages.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
                });
        };

        window.sendGlobalChat = function(event) {
            event.preventDefault();
            const form = event.currentTarget;
            const input = form.querySelector('.chat-input');
            const message = input.value.trim();
            if (!message) return;
            input.disabled = true;
            fetch('/api/global-chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', ...telegramAuthHeaders() },
                body: JSON.stringify({ message })
            })
                .then(response => response.json().then(data => ({ ok: response.ok, data })))
                .then(({ ok, data }) => {
                    if (!ok) throw new Error(data.message || 'Could not send message');
                    input.value = '';
                    loadGlobalChat();
                })
                .catch(error => showToast(error.message))
                .finally(() => { input.disabled = false; });
        };

        function escapeHtml(value) {
            return String(value || '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
        }

        window.showSettings = function() {
            const notifications = localStorage.getItem('flimfybox-notifications') !== 'off';
            const compact = localStorage.getItem('flimfybox-compact') === 'on';
            showInfoPanel('Settings', `
                <label class="settings-toggle"><span>New title notifications</span><input id="settingsNotifications" type="checkbox" ${notifications ? 'checked' : ''} onchange="localStorage.setItem('flimfybox-notifications', this.checked ? 'on' : 'off'); showToast(this.checked ? 'Notifications enabled' : 'Notifications disabled')"></label>
                <label class="settings-toggle"><span>Compact cards</span><input id="settingsCompact" type="checkbox" ${compact ? 'checked' : ''} onchange="localStorage.setItem('flimfybox-compact', this.checked ? 'on' : 'off'); document.body.classList.toggle('compact-cards', this.checked); showToast('Display preference saved')"></label>
                <p class="info-note">Your preferences are saved on this device.</p>
            `, 'gear', '<button type="button" class="primary-action" onclick="closeInfoModal()">Save & close</button>');
        };

        document.addEventListener('keydown', event => {
            if (event.key === 'Escape') closeInfoModal();
        });
        document.body.classList.toggle('compact-cards', localStorage.getItem('flimfybox-compact') === 'on');

        window.showHome = function() {
            closeMorePanel();
            closeInfoModal();
            document.body.classList.remove('screen-explore', 'screen-search', 'screen-list');
            document.body.classList.add('screen-home');
            document.getElementById('mainContent').style.display = '';
            document.getElementById('myListContent').style.display = 'none';
            document.getElementById('searchResultsContent').style.display = 'none';
            document.getElementById('exploreContent').style.display = 'none';
            document.getElementById('genreDetailPage').classList.remove('open');
            document.querySelectorAll('.movie-row').forEach(row => row.style.display = '');
            setActiveNav(0);
            window.scrollTo({ top: 0, behavior: 'smooth' });
        };

        window.showExplore = function() {
            closeMorePanel();
            closeInfoModal();
            document.body.classList.remove('screen-home', 'screen-search', 'screen-list');
            document.body.classList.add('screen-explore');
            document.getElementById('mainContent').style.display = 'none';
            document.getElementById('myListContent').style.display = 'none';
            document.getElementById('searchResultsContent').style.display = 'none';
            document.getElementById('exploreContent').style.display = 'block';
            document.getElementById('genreDetailPage').classList.remove('open');
            document.getElementById('searchDropdown').classList.remove('active');
            setActiveNav(2);
            renderExploreScreen();
            loadBrowseCollections();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        };

        window.showSearch = function() {
            closeMorePanel();
            closeInfoModal();
            showHome();
            document.body.classList.remove('screen-home', 'screen-explore', 'screen-list');
            document.body.classList.add('screen-search');
            setActiveNav(1);
            document.getElementById('mainContent').style.display = 'none';
            document.getElementById('exploreContent').style.display = 'none';
            document.getElementById('myListContent').style.display = 'none';
            document.getElementById('searchResultsContent').style.display = 'block';
            setTimeout(() => document.getElementById('searchInput').focus(), 120);
        };

        window.showMyList = async function() {
            closeMorePanel();
            closeInfoModal();
            document.body.classList.remove('screen-home', 'screen-explore', 'screen-search');
            document.body.classList.add('screen-list');
            const requestId = ++myListRequestId;
            const grid = document.getElementById('myListGrid');
            document.getElementById('mainContent').style.display = 'none';
            document.getElementById('searchResultsContent').style.display = 'none';
            document.getElementById('exploreContent').style.display = 'none';
            document.getElementById('genreDetailPage').classList.remove('open');
            document.getElementById('myListContent').style.display = 'block';
            document.getElementById('searchDropdown').classList.remove('active');
            setActiveNav(3);
            grid.innerHTML = '<div class="loader" style="grid-column:1/-1">Loading your saved titles…</div>';
            try {
                const response = await fetch('/api/my-list', { headers: telegramAuthHeaders() });
                const data = await response.json();
                if (requestId !== myListRequestId) return;
                if (!response.ok || data.status !== 'success') throw new Error(data.message || 'Could not load My List');
                (data.movies || []).forEach(movie => {
                    if (!allMovies.some(existing => String(existing.id) === String(movie.id))) allMovies.push(movie);
                });
                savedMovieIds = new Set((data.movies || []).map(movie => String(movie.id)));
                grid.innerHTML = data.movies?.length
                    ? renderCards(data.movies, 'grid-card', false)
                    : '<div class="empty-my-list">Your list is empty.<br><span>Save a title with the + button.</span></div>';
            } catch (error) {
                if (requestId !== myListRequestId) return;
                grid.innerHTML = `<div class="empty-my-list">${error.message}</div>`;
            }
        };

        window.toggleCurrentMyList = async function() {
            if (!activeMovie || activeMovie.source === 'tmdb' || String(activeMovie.id).startsWith('tmdb_')) {
                showToast('Only available titles can be saved right now.');
                return;
            }
            try {
                // Snapshot the details-page movie. The home carousel changes in
                // the background, so never read a mutable global after await.
                const movieToSave = activeMovie;
                const movieId = String(movieToSave.id);
                const isSaved = savedMovieIds.has(movieId);
                const response = await fetch(isSaved ? `/api/my-list/${movieId}` : '/api/my-list', {
                    method: isSaved ? 'DELETE' : 'POST',
                    headers: { 'Content-Type': 'application/json', ...telegramAuthHeaders() },
                    body: JSON.stringify({ movie_id: movieId })
                });
                const data = await response.json();
                if (!response.ok || data.status !== 'success') throw new Error(data.message || 'Could not save title');
                const button = document.getElementById('detailMyListButton');
                if (isSaved) {
                    savedMovieIds.delete(movieId);
                    if (button) button.innerHTML = '<i class="fas fa-plus"></i>';
                    showToast('Removed from My List');
                } else {
                    savedMovieIds.add(movieId);
                    if (button) button.innerHTML = '<i class="fas fa-check"></i>';
                    showToast('Saved to My List');
                }
            } catch (error) {
                showToast(error.message || 'Could not save title');
            }
        };

        // Pagination State
        let currentPage = 1;
        let isFetching = false;
        let hasMoreMovies = true;

        // Load movies from API with Infinite Scroll support
        async function loadMovies(page = 1) {
            if (isFetching || !hasMoreMovies) return;
            isFetching = true;

            try {
                // Agar page 1 se zyada hai, toh neeche ek loading spinner dikhao
                if (page > 1) {
                    document.getElementById('moreGrid').insertAdjacentHTML('beforeend', '<div id="scrollLoader" style="grid-column: 1 / -1; text-align: center; padding: 20px;"><div class="loader" style="width:30px;height:30px;border-width:3px;margin:0 auto;"></div></div>');
                }

                const res = await fetch(`/api/movies?page=${page}&limit=40`);
                const data = await res.json();
                
                // Naya data aate hi loader hata do
                if (page > 1) {
                    const loader = document.getElementById('scrollLoader');
                    if (loader) loader.remove();
                }

                if (data.status === 'success') {
                    const newMovies = data.movies.filter(m => m.image);
                    hasMoreMovies = data.has_more; 
                    
                    if (page === 1) {
                        // Pehli baar: Pura UI setup karo
                        allMovies = newMovies;
                        renderHome(allMovies); 
                        completeInitialHomeLoadingStep();
                    } else {
                        // Scrolling par: Purani movies mein nayi jod do
                        allMovies = [...allMovies, ...newMovies]; 
                        const newCardsHTML = renderCards(newMovies, 'grid-card', false);
                        document.getElementById('moreGrid').insertAdjacentHTML('beforeend', newCardsHTML);
                    }
                    currentPage++; // Agli baar ke liye page badha do
                } else {
                    console.error('API error:', data.message);
                    document.getElementById('moreGrid').innerHTML = '<div class="empty-search-state" style="grid-column:1/-1">Catalogue is temporarily unavailable.<br><span>Please try again in a moment.</span></div>';
                    if (page === 1) completeInitialHomeLoadingStep();
                }
            } catch (e) {
                console.error('Fetch failed', e);
                document.getElementById('moreGrid').innerHTML = '<div class="empty-search-state" style="grid-column:1/-1">We could not load the catalogue.<br><span>Check your connection and try again.</span></div>';
                if (page === 1) completeInitialHomeLoadingStep();
            } finally {
                isFetching = false;
            }
        }

        // 🔥 NAYA: Infinite Scroll Listener
        window.addEventListener('scroll', () => {
            // Agar user page ke bottom se 600px upar hai, toh advance mein next page load kar lo
            if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 600) {
                // Check karo ki normal page open hai (Search result open na ho)
                if (document.getElementById('searchResultsContent').style.display === 'none') {
                    loadMovies(currentPage);
                }
            }
        });

        function renderExploreScreen() {
            if (!genreCatalog) {
                document.getElementById('exploreBody').innerHTML = '<div class="genre-loading"><div class="loader"></div><span>Mapping the real catalogue…</span></div>';
                loadGenreCatalog().then(() => {
                    if (genreCatalog && genreCatalogType === browseType) renderExploreScreen();
                }).catch(error => {
                    document.getElementById('exploreBody').innerHTML = `<div class="empty-search-state">${error.message}</div>`;
                });
                return;
            }

            const query = (document.getElementById('genreSearchInput')?.value || '').trim().toLowerCase();
            const visibleGenres = CANONICAL_GENRE_TAXONOMY.filter(genre => {
                const catalogueGenre = genreCatalog[genre.id];
                // A selected media type should never show empty genre tiles.
                if (browseType !== 'all' && (!catalogueGenre || catalogueGenre.count < 1)) return false;
                const match = genre.label.toLowerCase().includes(query) || genre.group.toLowerCase().includes(query) || genre.description.toLowerCase().includes(query);
                const subqueryMatch = Object.keys(GENRE_ALIASES).some(aliasKey => {
                    const aliasList = GENRE_ALIASES[aliasKey] || [];
                    return aliasList.some(alias => alias.toLowerCase().includes(query));
                });
                return !query || match || subqueryMatch;
            });

            const isCompactBrowse = window.matchMedia('(max-width: 699px)').matches;
            const orderedGenres = isCompactBrowse
                ? visibleGenres
                    .filter(genre => MOBILE_FEATURED_GENRE_ORDER.includes(genre.id))
                    .sort((a, b) => MOBILE_FEATURED_GENRE_ORDER.indexOf(a.id) - MOBILE_FEATURED_GENRE_ORDER.indexOf(b.id))
                : visibleGenres;
            const grouped = isCompactBrowse
                ? [{ groupName: '', items: orderedGenres }]
                : GENRE_GROUP_ORDER.map(groupName => {
                const items = visibleGenres.filter(genre => genre.group === groupName);
                return { groupName, items };
            }).filter(section => section.items.length);

            const body = document.getElementById('exploreBody');
            if (!grouped.length) {
                body.innerHTML = '<div class="empty-search-state" style="margin:20px;">No genres match your search.</div>';
                return;
            }

            body.innerHTML = grouped.map(section => {
                const cards = section.items.map(genre => {
                    const catalogueGenre = genreCatalog[genre.id] || {};
                    const count = catalogueGenre.count || 0;
                    const posters = Array.isArray(catalogueGenre.posters) && catalogueGenre.posters.length
                        ? catalogueGenre.posters
                        : [GENRE_ARTWORK[genre.id]];
                    const palette = getGenreVisualPalette(genre.group);
                    return `
                        <button class="genre-card genre-${genre.id} ${count === 0 ? 'empty' : ''}" type="button" data-genre-group="${genre.group}" onclick="openGenreDetail('${genre.id}')" style="--genre-from: ${palette.from}; --genre-to: ${palette.to}; --genre-glow: ${palette.glow};">
                            <span class="genre-card-posters poster-count-${Math.min(posters.length, 3)}" aria-hidden="true">
                                ${posters.slice(0, 3).map((poster, index) => `<img class="genre-card-poster genre-card-poster-${index + 1}" src="${poster}" alt="" loading="lazy" decoding="async">`).join('')}
                            </span>
                            <div class="genre-card-meta">
                                <span>${count} ${count === 1 ? 'title' : 'titles'}</span>
                                <span>${genre.group}</span>
                            </div>
                            <div class="genre-card-name">${genre.label}</div>
                        </button>
                    `;
                }).join('');
                return `
                    <section class="genre-section">
                        ${section.groupName ? `<div class="genre-section-title">${section.groupName}</div>` : ''}
                        <div class="genre-card-grid">${cards}</div>
                    </section>
                `;
            }).join('');
        }

        window.openGenreDetail = async function(genreId) {
            const requestId = ++genreDetailRequestId;
            const genre = findGenreById(genreId);
            genreDetailActiveId = genreId;
            genreDetailVisibleCount = 24;
            genreDetailMovies = [];
            let data;
            try {
                await loadGenreCatalog();
                const response = await fetch(`/api/genre/${encodeURIComponent(genreId)}?type=${encodeURIComponent(browseType)}`);
                data = await response.json();
                if (!response.ok || data.status !== 'success') throw new Error(data.message || 'Could not load this genre');
            } catch (error) {
                showToast(error.message || 'Could not load this genre');
                return;
            }
            if (requestId !== genreDetailRequestId) return;
            genreDetailMovies = data.movies || [];
            const count = data.genre?.count || 0;
            const hero = document.getElementById('genreDetailHero');
            const title = document.getElementById('genreDetailTitle');
            const meta = document.getElementById('genreDetailMeta');
            const summary = document.getElementById('genreDetailSummary');
            const grid = document.getElementById('genreDetailGrid');

            title.innerText = genre.label;
            meta.innerText = `${count} ${count === 1 ? 'title' : 'titles'} in the catalogue`;
            summary.innerHTML = `
                <div class="genre-detail-summary-inner">
                    <div class="genre-detail-kicker">${genre.group}</div>
                    <p>${genre.description}</p>
                    <div class="genre-detail-actions">
                    <button class="btn-request" type="button" onclick="showHome();">Browse all</button>
                    ${count === 0 ? `<button class="btn-request btn-sm-outline" type="button" onclick="requestMovie('${genre.label.replace(/'/g, "\\'")}')">Request title</button>` : ''}
                    </div>
                </div>
            `;

            hero.dataset.genre = genreId;
            hero.style.backgroundImage = `url("${GENRE_ARTWORK[genreId]}"), linear-gradient(135deg, ${getGenreVisualPalette(genre.group).from}, ${getGenreVisualPalette(genre.group).to})`;
            hero.style.backgroundSize = 'cover';
            hero.style.backgroundPosition = 'center';
            renderGenreDetailResults(genre, count);

            document.getElementById('exploreContent').style.display = 'none';
            document.getElementById('genreDetailPage').classList.add('open');
            window.scrollTo({ top: 0, behavior: 'smooth' });
        };

        function renderGenreDetailResults(genre, count) {
            const grid = document.getElementById('genreDetailGrid');
            const visibleMovies = genreDetailMovies.slice(0, genreDetailVisibleCount);
            const hasMore = visibleMovies.length < genreDetailMovies.length;
            grid.innerHTML = `
                <div class="genre-catalogue-heading">
                    <div>
                        <span class="genre-catalogue-kicker">Catalogue</span>
                        <h3>${genre.label} titles</h3>
                    </div>
                    <span class="genre-catalogue-count">${count} ${count === 1 ? 'title' : 'titles'}</span>
                </div>
                ${visibleMovies.length
                    ? renderCards(visibleMovies, 'grid-card', false)
                    : '<div class="empty-search-state genre-empty" style="grid-column:1/-1;">Nothing here yet.<br><span>This canonical genre has no titles in the real catalogue.</span></div>'}
                ${hasMore ? `<button class="btn-request genre-load-more" type="button" onclick="loadMoreGenreResults()">Load more</button>` : ''}
            `;
        }

        window.loadMoreGenreResults = function() {
            if (!genreDetailMovies.length || genreDetailVisibleCount >= genreDetailMovies.length) return;
            genreDetailVisibleCount += 24;
            const genre = findGenreById(genreDetailActiveId);
            if (genre) renderGenreDetailResults(genre, genreDetailMovies.length);
        };

        window.closeGenreDetail = function() {
            document.getElementById('genreDetailPage').classList.remove('open');
            document.getElementById('exploreContent').style.display = 'block';
            genreDetailActiveId = null;
            genreDetailMovies = [];
            genreDetailVisibleCount = 24;
            renderExploreScreen();
            document.querySelectorAll('[data-browse-type]').forEach(button => button.classList.toggle('active', button.dataset.browseType === browseType));
            loadBrowseCollections();
        };

        function renderHeroProgress(total, activeIndex) {
            const dots = Array.from({ length: total }, (_, i) =>
                `<span class="hero-progress-dot ${i === activeIndex ? 'active' : ''}"></span>`
            ).join('');
            document.getElementById('heroSlider').insertAdjacentHTML('beforeend', `<div class="hero-progress">${dots}</div>`);
        }

        function showHeroSlide(index) {
            if (!heroItems.length) return;
            heroIndex = (index + heroItems.length) % heroItems.length;
            const movie = heroItems[heroIndex];
            const heroSlider = document.getElementById('heroSlider');
            activeMovie = movie;
            const openHeroDetails = () => {
                if (!allMovies.some(item => String(item.id) === String(movie.id))) {
                    allMovies.push(movie);
                }
                openDetails(String(movie.id), false);
            };
            heroSlider.style.backgroundImage = `url(${movie.image})`;
            document.getElementById('heroTitle').innerText = movie.title;
            document.getElementById('heroMeta').innerText = [movie.year, movie.category, movie.language].filter(Boolean).join(' • ');
            document.querySelector('#heroSlider .eyebrow').innerText = 'TRENDING NOW';
            document.getElementById('heroWatch').onclick = openHeroDetails;
            document.getElementById('heroInfo').onclick = openHeroDetails;
            heroSlider.querySelectorAll('.hero-progress-dot').forEach((dot, dotIndex) => dot.classList.toggle('active', dotIndex === heroIndex));
            const position = document.getElementById('heroPosition');
            if (position) position.textContent = `${heroIndex + 1} / ${heroItems.length}`;
        }

        function stepHero(direction) {
            if (!heroItems.length) return;
            showHeroSlide(heroIndex + direction);
            if (heroTimer) {
                clearInterval(heroTimer);
                heroTimer = setInterval(() => stepHero(1), 6500);
            }
        }
        window.stepHero = stepHero;

        function getRecentSearches() {
            try {
                const items = JSON.parse(localStorage.getItem('flimfybox_recent_searches') || '[]');
                return Array.isArray(items) ? items.slice(0, 6) : [];
            } catch (error) {
                return [];
            }
        }

        function saveRecentSearches(items) {
            localStorage.setItem('flimfybox_recent_searches', JSON.stringify(items.slice(0, 6)));
        }

        function addRecentSearch(term) {
            const value = term.trim();
            if (!value) return;
            const list = getRecentSearches().filter(item => item.toLowerCase() !== value.toLowerCase());
            list.unshift(value);
            saveRecentSearches(list);
        }

        function getRecentlyViewed() {
            try {
                const items = JSON.parse(localStorage.getItem('flimfybox_recently_viewed') || '[]');
                return Array.isArray(items) ? items : [];
            } catch (error) {
                return [];
            }
        }

        function saveRecentlyViewed(items) {
            localStorage.setItem('flimfybox_recently_viewed', JSON.stringify(items.slice(0, 8)));
        }

        function addRecentlyViewed(movie) {
            if (!movie || !movie.title) return;
            const items = getRecentlyViewed().filter(item => String(item.id) !== String(movie.id));
            items.unshift({
                id: movie.id,
                title: movie.title,
                image: movie.image,
                year: movie.year,
                category: movie.category,
                genre: movie.genre,
                source: movie.source || 'local'
            });
            saveRecentlyViewed(items);
        }

        function renderHome(movies) {
            if (heroTimer) {
                clearInterval(heroTimer);
                heroTimer = null;
            }

            const recentViewed = getRecentlyViewed();
            const recentScroll = document.getElementById('recentScroll');
            const recentRow = document.getElementById('rowRecent');
            if (recentViewed.length) {
                recentScroll.innerHTML = renderCards(recentViewed, 'card', false);
                recentRow.style.display = '';
            } else {
                recentRow.style.display = 'none';
                recentScroll.innerHTML = '';
            }

            const catalogueRows = [
                {
                    row: 'rowHollywood',
                    target: 'hollywoodScroll',
                    matches: movie => `${movie.category || ''} ${movie.language || ''}`.toLowerCase().includes('hollywood')
                        || String(movie.category || '').toLowerCase() === 'english'
                },
                {
                    row: 'rowBollywood',
                    target: 'bollywoodScroll',
                    matches: movie => `${movie.category || ''} ${movie.language || ''}`.toLowerCase().includes('bollywood')
                        || String(movie.category || '').toLowerCase() === 'hindi'
                },
                {
                    row: 'rowAnime',
                    target: 'animeScroll',
                    matches: movie => `${movie.category || ''} ${movie.genre || ''}`.toLowerCase().includes('anime')
                }
            ];
            catalogueRows.forEach(({ row, target, matches }) => {
                const items = movies.filter(matches).slice(0, 12);
                const rowElement = document.getElementById(row);
                document.getElementById(target).innerHTML = items.length
                    ? renderCards(items, 'card', false)
                    : '';
                rowElement.style.display = items.length ? '' : 'none';
            });

            const requestId = ++newReleaseRequestId;
            fetch('/api/home/new-releases')
                .then(response => response.json())
                .then(data => {
                    if (requestId !== newReleaseRequestId) return;
                    if (data.status !== 'success') throw new Error(data.message || 'Could not load new releases');
                    const newReleases = data.movies || [];
                    const newReleasesRow = document.getElementById('rowActualNewReleases');
                    const newReleasesCaption = newReleasesRow.querySelector('.row-caption');
                    if (newReleasesCaption) {
                        const releaseWindow = Number(data.window_days) || 60;
                        newReleasesCaption.innerText = `Released in the last ${releaseWindow} days`;
                    }
                    document.getElementById('actualNewReleasesScroll').innerHTML = newReleases.length
                        ? renderCards(newReleases, 'card', false)
                        : '<div class="search-empty-state">No qualifying new releases right now.<br><span>Recently added catalogue titles are shown below.</span></div>';
                    newReleasesRow.style.display = '';
                })
                .catch(error => {
                    console.error('New release load failed:', error);
                    const fallbackRow = document.getElementById('rowActualNewReleases');
                    const fallbackCaption = fallbackRow?.querySelector('.row-caption');
                    if (fallbackCaption) fallbackCaption.innerText = 'Released in the last 60 days';
                    document.getElementById('actualNewReleasesScroll').innerHTML = '<div class="search-empty-state">No qualifying new releases right now.<br><span>Recently added catalogue titles are shown below.</span></div>';
                })
                .finally(completeInitialHomeLoadingStep);

            const trendingRequest = ++trendingRequestId;
            fetch('/api/home/trending?source=day')
                .then(response => response.json())
                .then(data => {
                    if (trendingRequest !== trendingRequestId) return;
                    if (data.status !== 'success') throw new Error(data.message || 'Could not load trending titles');
                    const trending = Array.isArray(data.results) ? data.results : [];
                    const trendingRow = document.getElementById('rowTrending');
                    document.getElementById('trendingScroll').innerHTML = trending.length
                        ? renderCards(trending.slice(0, 12), 'card', false)
                        : '';
                    trendingRow.style.display = trending.length ? '' : 'none';
                    const heroSlider = document.getElementById('heroSlider');
                    heroItems = trending.slice(0, Number(data.hero_limit || 10))
                        .filter((movie, index, items) => items.findIndex(item => String(item.id) === String(movie.id)) === index);
                    if (!heroItems.length) {
                        heroSlider.classList.add('is-loading');
                        document.querySelector('#heroSlider .eyebrow').innerText = 'TRENDING NOW';
                        document.getElementById('heroTitle').innerText = 'Trending titles aren\'t in the catalogue yet';
                        document.getElementById('heroMeta').innerText = 'TMDB ranking is available, but local catalogue coverage is empty';
                        return;
                    }
                    heroSlider.classList.remove('is-loading');
                    heroIndex = 0;
                    heroSlider.querySelector('.hero-progress')?.remove();
                    renderHeroProgress(heroItems.length, heroIndex);
                    showHeroSlide(heroIndex);
                    heroTimer = setInterval(() => stepHero(1), 6500);
                })
                .catch(error => {
                    console.error('Trending load failed:', error);
                    const heroSlider = document.getElementById('heroSlider');
                    heroSlider.classList.add('is-loading');
                    document.querySelector('#heroSlider .eyebrow').innerText = 'TRENDING NOW';
                    document.getElementById('heroTitle').innerText = 'Trending titles are temporarily unavailable';
                    document.getElementById('heroMeta').innerText = 'Please refresh in a moment';
                })
                .finally(completeInitialHomeLoadingStep);

            document.getElementById('moreGrid').innerHTML = renderCards(movies.slice(15), 'grid-card', false);
        }

        window.openCardDetails = function(movie, isTMDB) {
            if (!isTMDB && !allMovies.some(item => String(item.id) === String(movie.id))) {
                allMovies.push(movie);
            }
            openDetails(String(movie.id), isTMDB);
        };

        function renderCards(movies, cardClass = 'card', forceTMDB = false) {
            if (!movies.length) return '<div class="search-empty-state" style="grid-column:1/-1;">No titles to show right now.<br><span>Check back soon.</span></div>';
            return movies.map(m => {
                const isTMDB = forceTMDB || m.source === 'tmdb';
                const rating = m.rating && m.rating !== 'N/A' ? `⭐ ${m.rating}` : '';
                const badge = isTMDB
                    ? '<div class="card-badge request">Request</div>'
                    : (rating ? `<div class="card-badge available">${rating}</div>` : '');
                return `
                    <div class="${cardClass}" tabindex="0" role="button" aria-label="Open ${m.title}" onclick='openCardDetails(${JSON.stringify(m).replace(/'/g, "&#39;")}, ${isTMDB})' onkeydown="if(event.key==='Enter') openCardDetails(${JSON.stringify(m).replace(/'/g, "&#39;")}, ${isTMDB})">
                        <img src="${m.image}" class="card-img" loading="lazy" onerror="this.onerror=null; this.removeAttribute('src'); this.classList.add('image-fallback')">
                        <div class="card-title">${m.title}</div>
                        <div class="card-meta"><span>${m.year || '—'}</span>${badge}</div>
                    </div>
                `;
            }).join('');
        }

// Hybrid Search
let searchTimeout;
let searchRequestId = 0;

document.getElementById('genreSearchInput')?.addEventListener('input', (e) => {
    renderExploreScreen();
});

document.getElementById('searchInput').addEventListener('input', (e) => {
    clearTimeout(searchTimeout);
    const q = e.target.value.trim();
    const dropdown = document.getElementById('searchDropdown');

    if (!q) {
        const recent = getRecentSearches();
        dropdown.innerHTML = recent.length
            ? `<div class="search-empty-state"><strong>Recent searches</strong><div class="search-recent">${recent.map(item => `<button class="search-recent-chip" type="button" onclick="document.getElementById('searchInput').value='${item.replace(/'/g, "\\'")}'; document.getElementById('searchInput').dispatchEvent(new Event('input', { bubbles: true }));">${item}</button>`).join('')}</div></div>`
            : '<div class="search-empty-state"><strong>Search your catalogue</strong><span>Try a title, genre, or keyword.</span></div>';
        dropdown.classList.add('active');
        return;
    }

    dropdown.innerHTML = '<div class="loader">Searching catalog…</div>';
    dropdown.classList.add('active');

    searchTimeout = setTimeout(async () => {
        searchRequestId++;
        const currentId = searchRequestId;
        try {
            const response = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
            const searchData = await response.json();
            if (currentId !== searchRequestId) return;
            if (!response.ok || searchData.status !== 'success') throw new Error(searchData.message || 'Search failed');
            const results = searchData.results || [];
            results.forEach(r => {
                if (r.source === 'tmdb') tmdbMoviesMap[r.id] = r;
                else if (!allMovies.some(m => String(m.id) === String(r.id))) allMovies.push(r);
            });

            if (!results.length) {
                addRecentSearch(q);
                const safeQuery = q.replace(/'/g, "\\'");
                dropdown.innerHTML = `<div class="empty-search-state"><strong>No match found</strong><span>“${q}” isn't in the catalogue yet.</span><div style="margin-top:12px;"><button class="btn-sm btn-sm-primary" onclick="requestSilent('${safeQuery}')"><i class="fas fa-paper-plane"></i> Request this title</button></div></div>`;
                return;
            }

            addRecentSearch(q);
            dropdown.innerHTML = results.slice(0, 8).map(r => {
                const isTMDB = r.source === 'tmdb';
                const status = isTMDB ? 'Request' : 'Available';
                const action = isTMDB ? `<button class="btn-sm btn-sm-outline" onclick="requestMovie('${String(r.title || '').replace(/'/g, "\\'")}' )">Request</button>` : '<button class="btn-sm btn-sm-primary">View</button>';
                const click = isTMDB
                    ? `onclick="openDetails('${r.id}', true); document.getElementById('searchDropdown').classList.remove('active');"`
                    : `onclick="openDetails('${r.id}', false); document.getElementById('searchDropdown').classList.remove('active');"`;
                return `<div class="search-item fade-in" ${click}>
                    <img src="${r.image}" loading="lazy" onerror="this.src='/static/miniapp/poster-placeholder.svg'">
                    <div class="search-item-info"><div class="search-item-title">${r.title}</div>
                    <div class="search-item-meta"><span>${r.year || '—'}</span><span class="status-pill ${isTMDB ? 'request' : 'available'}">${status}</span></div></div>
                    <div class="search-actions">${action}</div></div>`;
            }).join('');
        } catch (error) {
            console.error('Search failed:', error);
            dropdown.innerHTML = '<div class="search-empty-state"><strong>Search unavailable</strong><span>Please try again in a moment.</span></div>';
        }
    }, 260);
});


// Hide dropdown if clicked outside
document.addEventListener('click', (e) => {
    const dropdown = document.getElementById('searchDropdown');
    const container = document.querySelector('.search-section');
    if (!container.contains(e.target)) {
        dropdown.classList.remove('active');
    }
});
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        document.getElementById('searchDropdown').classList.remove('active');
        closeTrailer();
        closeWebPlayer();
    }
});
        // Details
        window.openDetails = function(id, isTMDB) {
            const movie = isTMDB ? tmdbMoviesMap[id] : allMovies.find(m => m.id == id);
            if (!movie) return;
            activeMovie = movie;
            addRecentlyViewed(movie);
            const requestId = ++detailsRequestId;
            const myListButton = document.getElementById('detailMyListButton');
            if (myListButton) {
                myListButton.innerHTML = savedMovieIds.has(String(movie.id))
                    ? '<i class="fas fa-check"></i>' : '<i class="fas fa-plus"></i>';
            }
            const detailsPage = document.getElementById('detailsPage');
            const detailsBackdrop = document.getElementById('dpBackdrop');
            const detailsPoster = document.getElementById('dpFloatPoster');
            const detailsTitle = document.getElementById('dpTitle');
            const detailsRating = document.getElementById('dpRating');
            const detailsGenre = document.getElementById('dpGenre');
            const detailsDescription = document.getElementById('dpDesc');
            detailsBackdrop.style.backgroundImage = movie.image ? `url(${movie.image})` : '';
            detailsPoster.src = movie.image || '/static/miniapp/poster-placeholder.svg';
            detailsTitle.innerText = movie.title || 'Loading details…';
            detailsRating.innerText = movie.rating && movie.rating !== 'N/A' ? movie.rating : '—';
            detailsGenre.innerText = movie.genre || movie.category || 'Catalogue title';
            detailsDescription.innerText = 'Loading story and availability…';
            document.getElementById('castSection').innerHTML = '';
            document.getElementById('dpTrailerBtn').innerHTML = '';
            renderCommunityRating(movie.id, movie.title || 'this title');
            document.getElementById('dpSeasons').innerHTML = '';
            document.getElementById('dpLinks').innerHTML = '';
            detailsPage.classList.add('open', 'is-loading');
            if (isTMDB) {
                const backdropImg = movie.image;
                document.getElementById('dpBackdrop').style.backgroundImage = `url(${backdropImg})`;
                document.getElementById('dpFloatPoster').src = movie.image;
                document.getElementById('dpTitle').innerText = movie.title;
                document.getElementById('dpRating').innerText = movie.rating && movie.rating !== 'N/A' ? movie.rating : '—';
                document.getElementById('dpGenre').innerText = movie.genre || 'Action, Drama';
                document.getElementById('dpDesc').innerText = movie.description || 'No description available.';
                document.getElementById('castSection').innerHTML = '';
                document.getElementById('dpTrailerBtn').innerHTML = `<button class="btn-request" onclick="requestMovie('${String(movie.title || '').replace(/'/g, "\\'")}' )"><i class="fas fa-hand-paper"></i> Request this title</button>`;
                document.getElementById('dpLinks').innerHTML = '';
                detailsPage.classList.remove('is-loading');
                return;
            }

            fetch(`/api/movie/${id}`)
                .then(res => res.json())
                .then(data => {
                    if (requestId !== detailsRequestId || String(activeMovie?.id) !== String(id)) return;
                    if (data.status === 'success') {
                        const m = data.movie;
                        activeMovie = { ...movie, ...m, source: 'local' };
                        addRecentlyViewed(activeMovie);
                        const backdropUrl = m.backdrop ? m.backdrop : m.image;
                        document.getElementById('dpBackdrop').style.backgroundImage = `url(${backdropUrl})`;
                        document.getElementById('dpFloatPoster').src = m.image;
                        document.getElementById('dpTitle').innerText = m.title;
                        document.getElementById('dpRating').innerText = m.rating && m.rating !== 'N/A' ? m.rating : '—';
                        document.getElementById('dpGenre').innerText = m.genre || 'Drama';
                        document.getElementById('dpDesc').innerText = m.description || 'Story details are not available yet.';
                        if (m.cast && m.cast.trim().length > 0) {
                            const actors = m.cast.split(',');
                            let castHtml = '<div style="margin-bottom:20px;">';
                            actors.forEach(actor => {
                                const cleanName = actor.trim();
                                if (cleanName) castHtml += `<span class="cast-chip">${cleanName}</span>`;
                            });
                            castHtml += '</div>';
                            document.getElementById('castSection').innerHTML = castHtml;
                        } else {
                            document.getElementById('castSection').innerHTML = '';
                        }
                        document.getElementById('dpTrailerBtn').innerHTML = '';
                        renderCommunityRating(m.id, m.title || movie.title || 'this title');

                        const seasonsContainer = document.getElementById('dpSeasons');
                        const linksContainer = document.getElementById('dpLinks');
                        seasonsContainer.innerHTML = '';
                        linksContainer.innerHTML = '';

                        if (m.files && m.files.length) {
                            let hasSeasons = false;
                            const seasonsMap = {};
                            const movieFiles = [];

                            m.files.forEach(f => {
                                let info = (f.extra_info || '').toUpperCase();
                                let s = null; let e = null; let epStr = null;
                                let sMatch = info.match(/S(\d+)|SEASON\s*(\d+)/);
                                if (sMatch) s = parseInt(sMatch[1] || sMatch[2], 10);
                                let eMatch = info.match(/E(\d+(?:-\d+)?)|EP\s*(\d+(?:-\d+)?)|EPISODE\s*(\d+(?:-\d+)?)/);
                                if (eMatch) { epStr = eMatch[1] || eMatch[2] || eMatch[3]; e = parseInt(epStr.split('-')[0], 10); }
                                if (s !== null) {
                                    hasSeasons = true;
                                    if (!seasonsMap[s]) seasonsMap[s] = { episodes: {} };
                                    const sortEp = e !== null ? e : 0;
                                    const displayTitle = e !== null ? `EP ${epStr.padStart(2, '0')}` : `Season ${s} extras`;
                                    if (!seasonsMap[s].episodes[sortEp]) seasonsMap[s].episodes[sortEp] = { title: displayTitle, qualities: [] };
                                    seasonsMap[s].episodes[sortEp].qualities.push(f);
                                } else {
                                    movieFiles.push(f);
                                }
                            });

                            if (hasSeasons) {
                                const seasonNumbers = Object.keys(seasonsMap).map(Number).sort((a, b) => a - b);
                                let seasonsHtml = `<div class="season-scroll-wrapper"><div class="season-pill-container" id="seasonPillContainer">`;
                                seasonNumbers.forEach(sn => {
                                    seasonsHtml += `<div class="season-pill" data-season="${sn}" onclick="selectSeason(${m.id}, ${sn})">Season ${sn}</div>`;
                                });
                                seasonsHtml += `</div></div>`;
                                seasonsContainer.innerHTML = seasonsHtml;
                                window.currentMovieSeasons = seasonsMap;
                                selectSeason(m.id, seasonNumbers[0]);
                            } else {
                                let links = '<div class="dl-heading">Available qualities</div>';
                                m.files.forEach(f => {
                                    links += `
                                        <button class="dl-btn" onclick="downloadMovie(${m.id}, ${f.id})">
                                            <span class="quality-text"><i class="fas fa-download"></i> ${f.quality} <span class="file-size">${f.size || 'N/A'}</span></span>
                                            <span class="action">Download</span>
                                        </button>
                                    `;
                                });
                                linksContainer.innerHTML = links;
                            }
                        } else {
                            linksContainer.innerHTML = `
                                <div class="dl-heading">Download</div>
                                <button class="dl-btn" onclick="downloadMovie(${m.id})">
                                    <span class="quality-text"><i class="fas fa-download"></i> 1080p Full HD</span>
                                    <span class="action">Download</span>
                                </button>
                            `;
                        }
                        detailsPage.classList.remove('is-loading');
                    }
                })
                .catch(error => {
                    if (requestId !== detailsRequestId || String(activeMovie?.id) !== String(id)) return;
                    console.error('Details load failed:', error);
                    detailsDescription.innerText = 'Details are temporarily unavailable. You can go back and try again.';
                    document.getElementById('dpTrailerBtn').innerHTML = '<button class="btn-request" onclick="showToast(\'Could not load title details\')"><i class="fas fa-triangle-exclamation"></i> Retry later</button>';
                    detailsPage.classList.remove('is-loading');
                });
        };
        window.renderCommunityRating = function(movieId, title) {
                const container = document.getElementById('communityRating');
                if (!container) return;
                container.innerHTML = '<h2 class="community-rating-title"></h2><div class="community-rating-stars"></div><div class="community-rating-summary">Loading rating…</div>';
                container.querySelector('.community-rating-title').textContent = `Rate ${title}`;
                const stars = container.querySelector('.community-rating-stars');
                for (let value = 1; value <= 5; value += 1) {
                    const button = document.createElement('button');
                    button.type = 'button';
                    button.className = 'community-rating-star';
                    button.dataset.rating = String(value);
                    button.setAttribute('aria-label', `Rate ${value} out of 5`);
                    button.textContent = '☆';
                    button.addEventListener('mouseenter', () => previewCommunityStars(container, value));
                    button.addEventListener('mouseleave', () => previewCommunityStars(container, 0));
                    button.addEventListener('click', () => submitCommunityRating(movieId, value, container));
                    stars.appendChild(button);
                }
                fetch(`/api/movie/${encodeURIComponent(movieId)}/rating`)
                    .then(response => response.json().then(data => ({ ok: response.ok, data })))
                    .then(({ ok, data }) => {
                        if (!ok || data.status !== 'success') throw new Error(data.message || 'Rating unavailable');
                        updateCommunityRating(container, data);
                    })
                    .catch(() => {
                        container.querySelector('.community-rating-summary').textContent = 'Rating temporarily unavailable';
                    });
        };
        function previewCommunityStars(container, value) {
                container.querySelectorAll('.community-rating-star').forEach((button) => {
                    const selected = Number(button.dataset.rating) <= value;
                    button.classList.toggle('preview', selected);
                    button.textContent = selected ? '★' : '☆';
                });
        }
        function updateCommunityRating(container, data) {
                const own = data.user_rating;
                container.querySelectorAll('.community-rating-star').forEach((button) => {
                    const selected = own && Number(button.dataset.rating) <= own;
                    button.classList.toggle('selected', Boolean(selected));
                    button.textContent = selected ? '★' : '☆';
                    button.disabled = !data.can_rate;
                });
                const summary = container.querySelector('.community-rating-summary');
                summary.innerHTML = data.count
                    ? `<strong>FlimfyBox Rating</strong><br>${Number(data.average).toFixed(1)} / 5 · ${data.count} rating${data.count === 1 ? '' : 's'}${own ? `<br>Your rating: ${own} / 5` : ''}`
                    : '<strong>FlimfyBox Rating</strong><br>No ratings yet';
        }
        function submitCommunityRating(movieId, value, container) {
                if (container.dataset.submitting === 'true') return;
                container.dataset.submitting = 'true';
                container.querySelectorAll('.community-rating-star').forEach(button => { button.disabled = true; });
                container.querySelector('.community-rating-summary').textContent = 'Saving your rating…';
                fetch(`/api/movie/${encodeURIComponent(movieId)}/rating`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ rating: value })
                }).then(response => response.json().then(data => ({ ok: response.ok, data })))
                    .then(({ ok, data }) => {
                        if (!ok || data.status !== 'success') throw new Error(data.message || 'Could not save rating');
                        updateCommunityRating(container, data);
                    })
                    .catch(error => {
                        container.querySelector('.community-rating-summary').textContent = error.message;
                        container.querySelectorAll('.community-rating-star').forEach(button => { button.disabled = false; });
                    })
                    .finally(() => { container.dataset.submitting = 'false'; });
        }
        window.playCurrentTrailer = function() {
            if (!activeMovie) return;
            if (activeMovie.trailer_key) {
                playTrailer(activeMovie.trailer_key);
                return;
            }
            showToast('Trailer is not available for this title');
        };


        window.selectSeason = function(movieId, seasonNum) {
            // Update Active Pill
            document.querySelectorAll('.season-pill').forEach(el => {
                if (parseInt(el.getAttribute('data-season')) === seasonNum) {
                    el.classList.add('active');
                    // Scroll into view
                    el.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
                } else {
                    el.classList.remove('active');
                }
            });

            const seasonData = window.currentMovieSeasons[seasonNum];
            const linksContainer = document.getElementById('dpLinks');
            
            if (!seasonData || Object.keys(seasonData.episodes).length === 0) {
                linksContainer.innerHTML = `<div class="empty-season">No episodes available for this season yet.</div>`;
                return;
            }

            const epNumbers = Object.keys(seasonData.episodes).map(Number).sort((a, b) => a - b);
            
            let html = `<div class="dl-heading">SEASON ${seasonNum} • ${epNumbers.length} EPISODES</div><div class="episodes-list">`;
            
            epNumbers.forEach(epNum => {
                const ep = seasonData.episodes[epNum];
                let epDisplayNum = epNum > 0 ? epNum.toString().padStart(2, '0') : '--';
                let epLabel = epNum > 0 ? 'EPISODE' : 'EXTRAS';
                let actualTitle = ep.title.replace(/^EP \d+\s*/i, ''); // Remove redundant 'EP 01' from title if it exists, leaving the rest if it's there
                if (!actualTitle || actualTitle === ep.title) {
                    actualTitle = ep.title;
                }

                html += `
                <div class="episode-card">
                    <div class="ep-header">
                        <div class="ep-number-group">
                            <div class="ep-number-label">${epLabel}</div>
                            <div class="ep-number">${epDisplayNum}</div>
                        </div>
                        <div class="ep-title">${actualTitle}</div>
                    </div>
                    <div class="ep-qualities">`;
                
                ep.qualities.forEach(q => {
                    html += `
                        <button class="ep-dl-btn" onclick="downloadMovie(${movieId}, ${q.id})">
                            <span class="ep-qtext"><i class="fas fa-play-circle"></i> ${q.quality} <span class="ep-size">${q.size || ''}</span></span>
                            <span class="ep-action"><i class="fas fa-download"></i></span>
                        </button>
                    `;
                });
                
                html += `</div></div>`;
            });
            html += `</div>`;
            
            linksContainer.innerHTML = html;
        };

        window.closeDetails = function() {
            document.getElementById('detailsPage').classList.remove('open');
        };

        window.playTrailer = function(key) {
            document.getElementById('trailerIframe').src = `https://www.youtube.com/embed/${key}?autoplay=1&rel=0`;
            document.getElementById('trailerModal').classList.add('active');
        };

        window.closeTrailer = function() {
            document.getElementById('trailerIframe').src = '';
            document.getElementById('trailerModal').classList.remove('active');
        };

        // In-App Web Player Modal Logic
        window.openWebPlayer = function(tmdbId) {
            document.getElementById('searchDropdown').classList.remove('active');
            const modal = document.getElementById('webPlayerModal');
            const iframeCont = document.getElementById('wpIframeContainer');
            const titleEl = document.getElementById('wpTitle');
            
            modal.classList.add('active');
            titleEl.innerText = 'Loading Player...';
            iframeCont.innerHTML = '<div class="wp-loader"><div class="loader"></div>Fetching Secure Stream...</div>';
            
            // Background async call to fetch IMDb ID
            fetch(`/api/imdb_id/${tmdbId}`)
                .then(res => res.json())
                .then(data => {
                    if (data.status === 'success' && data.imdb_id) {
                        titleEl.innerText = 'Secure Player · Premium Stream';
                        // streamimdb.ru requires IMDb ID for playing
                        iframeCont.innerHTML = `<iframe src="https://streamimdb.ru/embed/movie/${data.imdb_id}" allowfullscreen allow="autoplay"></iframe>`;
                    } else {
                        titleEl.innerText = 'Error loading stream';
                        iframeCont.innerHTML = '<div style="color:white;text-align:center;">❌ Could not find streaming source. Try Requesting the movie instead.</div>';
                    }
                })
                .catch(e => {
                    titleEl.innerText = 'Network Error';
                    iframeCont.innerHTML = '<div style="color:white;text-align:center;">❌ Network error while loading player.</div>';
                });
        };

        window.closeWebPlayer = function() {
            document.getElementById('webPlayerModal').classList.remove('active');
            document.getElementById('wpIframeContainer').innerHTML = ''; // Stop video playback
        };

        window.requestMovie = function(title) {
            tg.HapticFeedback.notificationOccurred('success');
            showToast('⏳ Requesting...');
            const user = tg.initDataUnsafe?.user || {id: 0, username: 'webapp', first_name: 'User'};
            fetch('/api/request', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title, user_id: user.id, username: user.username, first_name: user.first_name})
            })
            .then(r => r.json())
            .then(d => {
                if (d.status === 'success') showToast('✅ Request sent!');
                else showToast('❌ Failed');
            })
            .catch(() => showToast('❌ Error'));
        };

        // 🔥 NAYA: Silent Request (Jab TMDB aur Google dono fail ho jayein)
        window.requestSilent = function(title) {
            tg.HapticFeedback.notificationOccurred('success');
            showToast('⏳ Sending Request...');
            const user = tg.initDataUnsafe?.user || {id: 0, username: 'webapp', first_name: 'User'};
            
            fetch('/api/request', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title: title, user_id: user.id, username: user.username, first_name: user.first_name})
            })
            .then(r => r.json())
            .then(d => {
                if (d.status === 'success') {
                    showToast('✅ Request Sent to Admin!');
                    // Request bhejte hi Mini app close kar do (Seamless feel ke liye)
                    setTimeout(() => { tg.close(); }, 1500);
                } else {
                    showToast('❌ Failed to send');
                }
            })
            .catch(() => showToast('❌ Network Error'));
        };

        // 🛡️ NAYA: Anti-Bot Middleware Par Bhejne Wala Function
        window.downloadBot = function(id) {
            tg.HapticFeedback.impactOccurred('heavy');
            // Seedha Bot ki jagah pehle Secure verification page par bhejenge
            tg.openLink(`${window.location.origin}/watch/${id}`);
        };

        window.downloadMovie = function(id, fileId = null) {
            tg.HapticFeedback.impactOccurred('heavy');
            const filePath = fileId ? `/file/${fileId}` : '';
            tg.openLink(`${window.location.origin}/watch/${id}${filePath}`);
        };

        // Start
        setupMotionEffects();
        startInitialHomeLoading();
        loadMovies();
        
        // 🪄 NAYA JUGAD: URL se query nikal kar auto-search karna
        setTimeout(() => {
            const urlParams = new URLSearchParams(window.location.search);
            const reqQuery = urlParams.get('req');
            
            if (reqQuery) {
                const searchInput = document.getElementById('searchInput');
                searchInput.value = reqQuery;
                showToast("🔍 Finding correct spelling...");
                // Search ko trigger karo
                searchInput.dispatchEvent(new Event('input', { bubbles: true }));
            }
        }, 500); // Thoda ruk kar karenge taaki app load ho jaye
