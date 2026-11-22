(function (globalScope) {
    'use strict';

    const SERIES_HINT_RE = /\b(tv|series|web\s*series|mini\s*series|show|shows|kdrama|k-drama|cdrama|dorama|sitcom|season\s*\d+)\b/i;
    const EPISODE_FILE_RE = /\bS(?:EASON)?\s*0*\d+(?:\s*E(?:PISODE)?\s*0*\d+)?\b|\b(?:E(?:PISODE)?|EP)\s*0*\d+\b/i;
    const CREDIT_ROLE_PREFIX_RE = /^(director|directed\s+by|creator|created\s+by|writer|written\s+by|producer|starring|cast)\s*[:\-–]\s*/i;

    function isSeriesItem(item) {
        if (!item || typeof item !== 'object') return false;
        const mediaType = String(item.media_type || '').trim().toLowerCase();
        if (mediaType === 'tv' || mediaType === 'series') return true;
        if (item.seasons_data && typeof item.seasons_data === 'object' && Object.keys(item.seasons_data).length > 0) {
            return true;
        }
        const hints = [item.content_type, item.category, item.type]
            .filter(Boolean)
            .join(' ');
        if (SERIES_HINT_RE.test(hints)) return true;
        if (Array.isArray(item.files) && item.files.some((file) => {
            const meta = `${file?.extra_info || ''} ${file?.quality || ''}`;
            return EPISODE_FILE_RE.test(meta);
        })) {
            return true;
        }
        return false;
    }

    function computeInitials(name) {
        const cleaned = String(name || '').replace(/[^\p{L}\p{N}\s'-]/gu, ' ').trim();
        if (!cleaned) return 'FB';
        const parts = cleaned.split(/\s+/).filter(Boolean);
        if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
        return `${parts[0][0] || ''}${parts[parts.length - 1][0] || ''}`.toUpperCase();
    }

    function accentToneForName(name) {
        const str = String(name || '');
        let hash = 0;
        for (let i = 0; i < str.length; i += 1) {
            hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
        }
        const tones = ['gold', 'violet', 'slate', 'amber', 'indigo'];
        return tones[Math.abs(hash) % tones.length];
    }

    function parseCredits(castInput, directorInput) {
        const seen = new Set();
        const people = [];

        const pushPerson = (rawName, defaultRole) => {
            let text = String(rawName || '').trim();
            if (!text || /^(n\/a|na|unknown|none|-)$/i.test(text)) return;

            let role = defaultRole;
            const prefixMatch = text.match(CREDIT_ROLE_PREFIX_RE);
            if (prefixMatch) {
                const rawRole = prefixMatch[1].toLowerCase();
                if (rawRole.startsWith('direct')) role = 'Director';
                else if (rawRole.startsWith('creat')) role = 'Creator';
                else if (rawRole.startsWith('writ')) role = 'Writer';
                else if (rawRole.startsWith('produc')) role = 'Producer';
                text = text.slice(prefixMatch[0].length).trim();
            }

            const parenRoleMatch = text.match(/^(.+?)\s*\(([^()]{2,28})\)$/);
            if (parenRoleMatch) {
                text = parenRoleMatch[1].trim();
                role = parenRoleMatch[2].trim();
            }

            if (!text || text.length < 2) return;
            const dedupeKey = text.toLowerCase();
            if (seen.has(dedupeKey)) return;
            seen.add(dedupeKey);

            people.push({
                name: text,
                role: role || 'Cast',
                initials: computeInitials(text),
                tone: accentToneForName(text),
            });
        };

        if (directorInput) {
            String(directorInput)
                .split(/[,;|]/)
                .forEach((entry) => pushPerson(entry, 'Director'));
        }

        if (Array.isArray(castInput)) {
            castInput.forEach((entry) => {
                if (entry && typeof entry === 'object') {
                    pushPerson(entry.name || entry.title, entry.role || entry.character || 'Cast');
                } else {
                    pushPerson(entry, 'Cast');
                }
            });
        } else if (typeof castInput === 'string' && castInput.trim()) {
            castInput
                .split(/[,;|]/)
                .forEach((entry) => pushPerson(entry, 'Cast'));
        }

        return people.slice(0, 14);
    }

    function tokenizeGenreList(value) {
        return String(value || '')
            .toLowerCase()
            .split(/[,&/|·•]+/)
            .map((token) => token.trim())
            .filter((token) => token && token !== 'unknown' && token !== 'n/a');
    }

    const ADULT_HINT_RE = /\b(18\+|adult|erotic|erotica|ullu|kooku|altbalaji|primeplay|hotshots|fliz|nuefliks|wow entertainment|desiflix|atrangii)\b/i;

    function isAdultItem(item) {
        if (!item || typeof item !== 'object') return false;
        if (item.is_adult === true || item.adult === true) return true;
        const fields = [item.category, item.genre, item.certification, item.content_type, item.title]
            .map((v) => String(v || ''))
            .join(' ');
        return ADULT_HINT_RE.test(fields) || fields.includes('18+');
    }

    function selectSimilarTitles(currentItem, catalogList, limit = 10) {
        if (!currentItem || !Array.isArray(catalogList) || !catalogList.length) return [];
        const currentIsSeries = isSeriesItem(currentItem);
        const currentIsAdult = isAdultItem(currentItem);
        const currentId = String(currentItem.id ?? '');
        const currentTitle = String(currentItem.title || '').trim().toLowerCase();
        const currentGenres = new Set(tokenizeGenreList(currentItem.genre));
        const currentLang = String(currentItem.language || '').trim().toLowerCase();
        const currentCategory = String(currentItem.category || '').trim().toLowerCase();
        const currentYear = Number(currentItem.year) || 0;

        const seenIds = new Set();
        const seenTitles = new Set();
        const scored = [];

        catalogList.forEach((candidate) => {
            if (!candidate || typeof candidate !== 'object') return;
            const candId = String(candidate.id ?? '');
            const candTitle = String(candidate.title || '').trim().toLowerCase();
            if (!candTitle) return;
            if ((currentId && candId === currentId) || candTitle === currentTitle) return;
            if ((candId && seenIds.has(candId)) || seenTitles.has(candTitle)) return;

            // Strictly isolate Movies -> Movies, Series -> Series, and Mainstream -> Mainstream
            if (isSeriesItem(candidate) !== currentIsSeries) return;
            if (isAdultItem(candidate) !== currentIsAdult) return;

            if (candId) seenIds.add(candId);
            seenTitles.add(candTitle);

            let score = 0;
            const candGenres = tokenizeGenreList(candidate.genre);
            let genreOverlap = 0;
            candGenres.forEach((g) => {
                if (currentGenres.has(g)) genreOverlap += 1;
            });
            score += genreOverlap * 28;

            const candLang = String(candidate.language || '').trim().toLowerCase();
            if (currentLang && candLang && (candLang.includes(currentLang) || currentLang.includes(candLang))) {
                score += 12;
            }

            const candCategory = String(candidate.category || '').trim().toLowerCase();
            if (currentCategory && candCategory && candCategory === currentCategory) {
                score += 10;
            }

            if (candidate.is_available === true || candidate.availability_state === 'available') {
                score += 8;
            }

            const ratingNum = parseFloat(candidate.rating);
            if (Number.isFinite(ratingNum) && ratingNum > 0) {
                score += Math.min(ratingNum, 10) * 1.2;
            }

            const candYear = Number(candidate.year) || 0;
            if (currentYear > 1900 && candYear > 1900 && Math.abs(currentYear - candYear) <= 5) {
                score += 4;
            }

            scored.push({ candidate, score, genreOverlap });
        });

        scored.sort((a, b) => {
            if (b.score !== a.score) return b.score - a.score;
            return b.genreOverlap - a.genreOverlap;
        });

        return scored.slice(0, Math.max(1, limit)).map((entry) => entry.candidate);
    }

    function formatRuntime(rawValue) {
        if (rawValue === null || rawValue === undefined || rawValue === '') return '';
        if (typeof rawValue === 'string' && /\b(h|hr|hrs|min|m)\b/i.test(rawValue)) {
            return rawValue.trim();
        }
        const mins = Number(String(rawValue).replace(/[^\d.]/g, ''));
        if (!Number.isFinite(mins) || mins <= 0) return '';
        const whole = Math.round(mins);
        const hours = Math.floor(whole / 60);
        const rem = whole % 60;
        if (hours > 0 && rem > 0) return `${hours}h ${rem}m`;
        if (hours > 0) return `${hours}h`;
        return `${rem}m`;
    }

    function buildDetailMetadataItems(item, isSeries, seasonCount = 0) {
        const movie = item || {};
        const yearRaw = movie.year || String(movie.release_date || movie.first_air_date || '').slice(0, 4);
        const year = yearRaw && String(yearRaw) !== '0' ? String(yearRaw) : '';
        const runtime = !isSeries ? formatRuntime(movie.runtime || movie.duration) : '';
        const seasonsLabel = isSeries && seasonCount > 0
            ? `${seasonCount} ${seasonCount === 1 ? 'Season' : 'Seasons'}`
            : (isSeries ? 'Series' : '');
        const certification = String(movie.certification || movie.content_rating || movie.maturity_rating || '').trim();
        const genres = tokenizeGenreList(movie.genre)
            .map((g) => g.replace(/\b\w/g, (c) => c.toUpperCase()))
            .slice(0, 3)
            .join(' · ');
        const language = String(movie.language || '').trim();
        const ratingNum = parseFloat(movie.rating);
        const rating = Number.isFinite(ratingNum) && ratingNum > 0 ? ratingNum.toFixed(1) : '';

        return {
            kicker: isSeries ? 'TV SERIES' : 'FEATURE FILM',
            year: year || 'Year unavailable',
            durationOrSeasons: isSeries ? seasonsLabel : runtime,
            certification,
            genres: genres || (isSeries ? 'Series' : 'Cinema'),
            language: language || 'Audio details below',
            rating,
            primaryCtaLabel: isSeries ? 'Browse episodes' : 'Watch & Download',
            shareAriaLabel: isSeries ? 'Share series' : 'Share movie',
        };
    }

    function cleanEpisodeTitle(ep, epDisplayNum) {
        if (!ep) return `Episode ${epDisplayNum}`;
        if (ep.isCompleteSeason) return 'Complete Season Pack';
        if (ep.episodeEnd !== null && ep.episodeEnd !== undefined) {
            return `Episodes ${epDisplayNum} Combined`;
        }
        const qualities = Array.isArray(ep.qualities) ? ep.qualities : [];
        for (let i = 0; i < qualities.length; i += 1) {
            const rawExtra = String(qualities[i]?.extra_info || '').trim();
            if (!rawExtra) continue;
            // Extract named episode title if separated by ' - ' or ' | ' after SxxExx
            const namedMatch = rawExtra.match(/S\d{1,2}\s*E\d{1,3}\s*[-–|:]\s*([A-Za-z0-9][^|[\]()]{2,48})/i);
            if (namedMatch) {
                const candidate = namedMatch[1].replace(/\b(480p|720p|1080p|2160p|4k|web-?dl|hdrip|bluray|hindi|english|dual\s*audio)\b/gi, '').trim();
                if (candidate.length >= 3) return candidate;
            }
        }
        return `Episode ${epDisplayNum}`;
    }

    function buildEpisodeDisplayMeta(ep, seasonNum, epDisplayNum, fallbackThumb) {
        const qualities = Array.isArray(ep?.qualities) ? ep.qualities : [];
        const isComplete = Boolean(ep?.isCompleteSeason);
        const isRange = ep?.episodeEnd !== null && ep?.episodeEnd !== undefined;
        const badgeCode = isComplete
            ? `S${String(seasonNum).padStart(2, '0')} · FULL`
            : isRange
                ? `S${String(seasonNum).padStart(2, '0')} · E${epDisplayNum}`
                : `S${String(seasonNum).padStart(2, '0')} · E${epDisplayNum}`;
        const title = cleanEpisodeTitle(ep, epDisplayNum);
        const sizes = qualities
            .map((q) => String(q?.size || '').trim())
            .filter(Boolean);
        const specs = qualities
            .map((q) => String(q?.extra_info || '').trim())
            .filter(Boolean);
        const subtitle = isComplete
            ? `Full season bundle · ${qualities.length} ${qualities.length === 1 ? 'format' : 'formats'} ready`
            : `${qualities.length} ${qualities.length === 1 ? 'version' : 'versions'} ready${sizes[0] ? ` · ${sizes[0]}` : ''}`;

        return {
            badgeCode,
            title,
            subtitle,
            specLine: specs[0] || '',
            thumbUrl: ep?.still || fallbackThumb || '/static/miniapp/poster-placeholder.svg',
            fileCount: qualities.length,
        };
    }

    function getSynopsisState(text, threshold = 260) {
        const clean = String(text || '').trim();
        return {
            text: clean || 'Story details are not available yet.',
            canToggle: clean.length > threshold,
        };
    }

    const api = {
        isSeriesItem,
        parseCredits,
        selectSimilarTitles,
        formatRuntime,
        buildDetailMetadataItems,
        buildEpisodeDisplayMeta,
        getSynopsisState,
    };

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }
    if (globalScope) {
        globalScope.FlimfyCinema = api;
    }
})(typeof window !== 'undefined' ? window : globalThis);
