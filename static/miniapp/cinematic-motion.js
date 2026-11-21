/* FlimfyBox Cinematic Motion Engine
 * GPU-friendly GSAP choreography, CSS 3D card tilt with specular rim-light,
 * visibility-paused volumetric dust/lightfield canvas, and theatrical scene transitions.
 */
(function () {
    'use strict';

    const reducedMotionQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
    const finePointerQuery = window.matchMedia('(hover: hover) and (pointer: fine)');

    function prefersReducedMotion() {
        return reducedMotionQuery.matches;
    }

    function hasFinePointer() {
        return finePointerQuery.matches;
    }

    const gsapLib = typeof window.gsap !== 'undefined' ? window.gsap : null;
    const scrollTriggerLib = typeof window.ScrollTrigger !== 'undefined' ? window.ScrollTrigger : null;
    if (gsapLib && scrollTriggerLib) {
        gsapLib.registerPlugin(scrollTriggerLib);
    }

    /* ------------------------------------------------------------
     * 1. Floating Header Scroll State & Desktop Command Search
     * ------------------------------------------------------------ */
    const header = document.getElementById('appHeader');
    const searchSection = document.getElementById('searchSection');
    const searchInput = document.getElementById('searchInput');

    function syncHeaderScroll() {
        if (!header) return;
        header.classList.toggle('is-scrolled', window.scrollY > 28);
    }

    window.addEventListener('scroll', syncHeaderScroll, { passive: true });
    syncHeaderScroll();

    window.toggleDesktopSearch = function () {
        const isDesktop = window.innerWidth >= 960;
        if (!isDesktop) {
            if (typeof window.showSearch === 'function') window.showSearch();
            return;
        }
        const opening = !document.body.classList.contains('desktop-search-open');
        document.body.classList.toggle('desktop-search-open', opening);
        if (opening && searchInput) {
            requestAnimationFrame(() => {
                searchInput.focus();
                searchInput.select?.();
            });
        }
    };

    document.addEventListener('keydown', (event) => {
        if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
            event.preventDefault();
            window.toggleDesktopSearch();
        } else if (event.key === '/' && !/INPUT|TEXTAREA|SELECT/.test(document.activeElement?.tagName || '')) {
            event.preventDefault();
            window.toggleDesktopSearch();
        } else if (event.key === 'Escape' && document.body.classList.contains('desktop-search-open')) {
            document.body.classList.remove('desktop-search-open');
            searchInput?.blur();
        }
    });

    document.addEventListener('click', (event) => {
        if (!document.body.classList.contains('desktop-search-open')) return;
        const target = event.target;
        if (!(target instanceof Element)) return;
        if (target.closest('#searchSection') || target.closest('#desktopSearchTrigger')) return;
        document.body.classList.remove('desktop-search-open');
    });

    /* ------------------------------------------------------------
     * 2. Ambient Volumetric Dust & Signature Violet Lightfield Canvas
     *    Paused automatically when hero is offscreen, tab hidden,
     *    or prefers-reduced-motion is enabled.
     * ------------------------------------------------------------ */
    function initHeroAtmosphere() {
        const canvas = document.getElementById('heroAtmosphere');
        const hero = document.getElementById('heroSlider');
        if (!canvas || !hero) return;
        const ctx = canvas.getContext('2d', { alpha: true });
        if (!ctx) return;

        let width = 0;
        let height = 0;
        let dpr = 1;
        let rafId = 0;
        let inViewport = true;
        let pointerX = 0.65;
        let pointerY = 0.35;
        let currentX = 0.65;
        let currentY = 0.35;

        const PARTICLE_COUNT = window.innerWidth < 768 ? 18 : 34;
        const particles = Array.from({ length: PARTICLE_COUNT }, () => ({
            x: Math.random(),
            y: Math.random(),
            r: 0.7 + Math.random() * 1.9,
            vx: (Math.random() - 0.5) * 0.00018,
            vy: -0.00008 - Math.random() * 0.00022,
            alpha: 0.14 + Math.random() * 0.42,
            phase: Math.random() * Math.PI * 2,
            tint: Math.random() > 0.35 ? 'violet' : 'silver',
        }));

        function resize() {
            const rect = hero.getBoundingClientRect();
            width = Math.max(1, Math.floor(rect.width));
            height = Math.max(1, Math.floor(rect.height));
            dpr = Math.min(window.devicePixelRatio || 1, 1.5);
            canvas.width = Math.floor(width * dpr);
            canvas.height = Math.floor(height * dpr);
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }

        function isLightTheme() {
            return document.documentElement.dataset.theme === 'light';
        }

        function render(now) {
            rafId = 0;
            if (!inViewport || document.hidden || prefersReducedMotion()) return;

            currentX += (pointerX - currentX) * 0.045;
            currentY += (pointerY - currentY) * 0.045;

            ctx.clearRect(0, 0, width, height);

            const light = isLightTheme();
            const gx = currentX * width;
            const gy = currentY * height;
            const glowRadius = Math.max(width, height) * 0.55;
            const radial = ctx.createRadialGradient(gx, gy, 0, gx, gy, glowRadius);
            if (light) {
                radial.addColorStop(0, 'rgba(138, 108, 255, 0.16)');
                radial.addColorStop(0.45, 'rgba(101, 68, 200, 0.06)');
                radial.addColorStop(1, 'rgba(101, 68, 200, 0)');
            } else {
                radial.addColorStop(0, 'rgba(181, 161, 255, 0.22)');
                radial.addColorStop(0.42, 'rgba(122, 88, 255, 0.09)');
                radial.addColorStop(1, 'rgba(6, 7, 11, 0)');
            }
            ctx.fillStyle = radial;
            ctx.fillRect(0, 0, width, height);

            const timeSec = now * 0.001;
            for (let i = 0; i < particles.length; i += 1) {
                const p = particles[i];
                p.x += p.vx;
                p.y += p.vy;
                if (p.y < -0.04) {
                    p.y = 1.04;
                    p.x = Math.random();
                }
                if (p.x < -0.04) p.x = 1.04;
                if (p.x > 1.04) p.x = -0.04;

                const twinkle = 0.65 + 0.35 * Math.sin(timeSec * 1.4 + p.phase);
                const px = p.x * width + (currentX - 0.5) * 18 * p.r;
                const py = p.y * height + (currentY - 0.5) * 12 * p.r;
                const opacity = p.alpha * twinkle * (light ? 0.55 : 0.9);

                ctx.beginPath();
                ctx.arc(px, py, p.r, 0, Math.PI * 2);
                ctx.fillStyle = p.tint === 'violet'
                    ? `rgba(196, 178, 255, ${opacity.toFixed(3)})`
                    : `rgba(244, 245, 252, ${(opacity * 0.72).toFixed(3)})`;
                ctx.fill();
            }

            rafId = window.requestAnimationFrame(render);
        }

        function start() {
            if (rafId || !inViewport || document.hidden || prefersReducedMotion()) return;
            rafId = window.requestAnimationFrame(render);
        }

        function stop() {
            if (rafId) {
                window.cancelAnimationFrame(rafId);
                rafId = 0;
            }
        }

        resize();
        window.addEventListener('resize', resize, { passive: true });

        if (hasFinePointer()) {
            hero.addEventListener('pointermove', (event) => {
                const rect = hero.getBoundingClientRect();
                if (!rect.width || !rect.height) return;
                pointerX = Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width));
                pointerY = Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height));
            }, { passive: true });
        }

        if ('IntersectionObserver' in window) {
            const io = new IntersectionObserver((entries) => {
                inViewport = Boolean(entries[0]?.isIntersecting);
                if (inViewport) start();
                else stop();
            }, { threshold: 0.02 });
            io.observe(hero);
        }

        document.addEventListener('visibilitychange', () => {
            if (document.hidden) stop();
            else start();
        });

        reducedMotionQuery.addEventListener?.('change', () => {
            if (prefersReducedMotion()) {
                stop();
                ctx.clearRect(0, 0, width, height);
            } else {
                start();
            }
        });

        start();
    }

    /* ------------------------------------------------------------
     * 3. Hero Cursor Depth Parallax & Kinetic Slide Choreography
     * ------------------------------------------------------------ */
    function initHeroMotion() {
        const hero = document.getElementById('heroSlider');
        const heroArt = document.getElementById('heroArt');
        const heroPoster = document.getElementById('heroPosterFloat');
        if (!hero || !heroArt) return;

        // Cursor-driven layered depth on fine-pointer devices
        if (hasFinePointer() && !prefersReducedMotion()) {
            let targetX = 0;
            let targetY = 0;
            let curX = 0;
            let curY = 0;
            let parallaxRaf = 0;

            function stepParallax() {
                parallaxRaf = 0;
                curX += (targetX - curX) * 0.08;
                curY += (targetY - curY) * 0.08;
                heroArt.style.setProperty('--hero-px', `${(curX * -14).toFixed(2)}px`);
                heroArt.style.setProperty('--hero-py', `${(curY * -10).toFixed(2)}px`);
                if (heroPoster) {
                    heroPoster.style.setProperty('--poster-px', `${(curX * 14).toFixed(2)}px`);
                    heroPoster.style.setProperty('--poster-py', `${(curY * 10).toFixed(2)}px`);
                    heroPoster.style.setProperty('--poster-ry', `${(curX * 9).toFixed(2)}deg`);
                    heroPoster.style.setProperty('--poster-rx', `${(curY * -7).toFixed(2)}deg`);
                }
                if (Math.abs(targetX - curX) > 0.002 || Math.abs(targetY - curY) > 0.002) {
                    parallaxRaf = window.requestAnimationFrame(stepParallax);
                }
            }

            hero.addEventListener('pointermove', (event) => {
                if (prefersReducedMotion()) return;
                const rect = hero.getBoundingClientRect();
                if (!rect.width || !rect.height) return;
                targetX = ((event.clientX - rect.left) / rect.width - 0.5) * 2;
                targetY = ((event.clientY - rect.top) / rect.height - 0.5) * 2;
                if (!parallaxRaf) parallaxRaf = window.requestAnimationFrame(stepParallax);
            }, { passive: true });

            hero.addEventListener('pointerleave', () => {
                targetX = 0;
                targetY = 0;
                if (!parallaxRaf) parallaxRaf = window.requestAnimationFrame(stepParallax);
            }, { passive: true });
        }

        // Scroll-scrubbed backdrop parallax via GSAP ScrollTrigger
        if (gsapLib && scrollTriggerLib && !prefersReducedMotion()) {
            gsapLib.to(heroArt, {
                yPercent: 14,
                ease: 'none',
                scrollTrigger: {
                    trigger: hero,
                    start: 'top top',
                    end: 'bottom top',
                    scrub: 0.6,
                },
            });
        }

        // Kinetic typography & floating poster entrance on every hero slide transition
        window.addEventListener('flimfy:hero-change', () => {
            if (!gsapLib || prefersReducedMotion()) return;
            const targets = [
                hero.querySelector('.eyebrow'),
                document.getElementById('heroTitle'),
                document.getElementById('heroMeta'),
                document.getElementById('heroDescription'),
                hero.querySelector('.hero-actions'),
            ].filter(Boolean);

            gsapLib.killTweensOf(targets);
            gsapLib.fromTo(
                targets,
                { y: 20, opacity: 0 },
                {
                    y: 0,
                    opacity: 1,
                    duration: 0.62,
                    stagger: 0.055,
                    ease: 'power3.out',
                    clearProps: 'transform,opacity',
                }
            );

            if (heroPoster && window.innerWidth >= 960) {
                gsapLib.killTweensOf(heroPoster);
                gsapLib.fromTo(
                    heroPoster,
                    { y: 18, opacity: 0.25, scale: 0.96 },
                    {
                        y: 0,
                        opacity: 1,
                        scale: 1,
                        duration: 0.72,
                        ease: 'power3.out',
                        clearProps: 'transform,opacity',
                    }
                );
            }
        });
    }

    /* ------------------------------------------------------------
     * 4. Delegated 3D Poster Card Tilt & Specular Rim-Light
     *    Zero per-card listeners; works automatically on dynamic rows,
     *    paginated grid cards, search results, and genre artwork cards.
     * ------------------------------------------------------------ */
    function init3DCardTilt() {
        if (!hasFinePointer()) return;

        const TILT_SELECTOR = '.card, .grid-card, .genre-card, .dp-poster-float';
        let activeCard = null;
        let nextEvent = null;
        let frameScheduled = false;

        function resetCard(card) {
            if (!card) return;
            card.classList.remove('is-tilting');
            card.style.removeProperty('--tilt-rx');
            card.style.removeProperty('--tilt-ry');
            card.style.removeProperty('--tilt-mx');
            card.style.removeProperty('--tilt-my');
        }

        function applyTilt() {
            frameScheduled = false;
            if (!activeCard || !nextEvent || prefersReducedMotion()) return;
            const rect = activeCard.getBoundingClientRect();
            if (!rect.width || !rect.height) return;

            const px = Math.max(0, Math.min(1, (nextEvent.clientX - rect.left) / rect.width));
            const py = Math.max(0, Math.min(1, (nextEvent.clientY - rect.top) / rect.height));
            const maxTilt = activeCard.classList.contains('genre-card') ? 6 : 8.5;
            const rx = (0.5 - py) * (maxTilt * 2);
            const ry = (px - 0.5) * (maxTilt * 2);

            activeCard.classList.add('is-tilting');
            activeCard.style.setProperty('--tilt-rx', `${rx.toFixed(2)}deg`);
            activeCard.style.setProperty('--tilt-ry', `${ry.toFixed(2)}deg`);
            activeCard.style.setProperty('--tilt-mx', `${(px * 100).toFixed(1)}%`);
            activeCard.style.setProperty('--tilt-my', `${(py * 100).toFixed(1)}%`);
        }

        document.addEventListener('pointermove', (event) => {
            if (prefersReducedMotion()) return;
            const target = event.target instanceof Element ? event.target.closest(TILT_SELECTOR) : null;
            if (target !== activeCard) {
                resetCard(activeCard);
                activeCard = target;
            }
            if (!activeCard) return;
            nextEvent = event;
            if (!frameScheduled) {
                frameScheduled = true;
                window.requestAnimationFrame(applyTilt);
            }
        }, { passive: true });

        document.addEventListener('pointerout', (event) => {
            if (!activeCard) return;
            const related = event.relatedTarget instanceof Element ? event.relatedTarget.closest(TILT_SELECTOR) : null;
            if (related !== activeCard) {
                resetCard(activeCard);
                activeCard = null;
            }
        }, { passive: true });
    }

    /* ------------------------------------------------------------
     * 5. Scroll-Triggered Chapter Choreography
     *    Ensures content is always reachable and never stuck hidden.
     * ------------------------------------------------------------ */
    function initScrollChapters() {
        if (!gsapLib || !scrollTriggerLib || prefersReducedMotion()) return;

        const revealed = new WeakSet();

        function wireRevealSections() {
            const sections = document.querySelectorAll('.cinema-reveal, .browse-surprise-panel, .genre-section');
            sections.forEach((section) => {
                if (revealed.has(section)) return;
                revealed.add(section);

                gsapLib.fromTo(
                    section,
                    { y: 26, opacity: 0.01 },
                    {
                        y: 0,
                        opacity: 1,
                        duration: 0.72,
                        ease: 'power3.out',
                        clearProps: 'transform,opacity',
                        scrollTrigger: {
                            trigger: section,
                            start: 'top 90%',
                            once: true,
                        },
                    }
                );
            });
        }

        wireRevealSections();

        let refreshTimer = 0;
        const scheduleRefresh = () => {
            window.clearTimeout(refreshTimer);
            refreshTimer = window.setTimeout(() => {
                wireRevealSections();
                scrollTriggerLib.refresh();
            }, 180);
        };

        const mainContent = document.getElementById('mainContent');
        const exploreContent = document.getElementById('exploreContent');
        if ('MutationObserver' in window) {
            const observer = new MutationObserver(scheduleRefresh);
            if (mainContent) observer.observe(mainContent, { childList: true, subtree: true });
            if (exploreContent) observer.observe(exploreContent, { childList: true, subtree: false });
        }
    }

    /* ------------------------------------------------------------
     * 6. Theatrical Movie Detail Scene Entrance & Scroll Parallax
     * ------------------------------------------------------------ */
    function initDetailSceneMotion() {
        const detailsPage = document.getElementById('detailsPage');
        const dpBackdrop = document.getElementById('dpBackdrop');
        if (!detailsPage) return;

        function animateDetailEntrance() {
            if (!gsapLib || prefersReducedMotion()) return;
            const poster = document.getElementById('dpPosterFloat');
            const title = document.getElementById('dpTitle');
            const meta = document.getElementById('dpRichInfo');
            const actions = detailsPage.querySelector('.detail-actions');
            const grid = detailsPage.querySelector('.dp-content-grid');

            if (dpBackdrop) {
                gsapLib.killTweensOf(dpBackdrop);
                gsapLib.fromTo(
                    dpBackdrop,
                    { scale: 1.07, opacity: 0.45 },
                    { scale: 1, opacity: 1, duration: 0.85, ease: 'power3.out', clearProps: 'opacity' }
                );
            }

            const sequence = [poster, title, meta, actions, grid].filter(Boolean);
            gsapLib.killTweensOf(sequence);
            gsapLib.fromTo(
                sequence,
                { y: 24, opacity: 0 },
                {
                    y: 0,
                    opacity: 1,
                    duration: 0.64,
                    stagger: 0.06,
                    ease: 'power3.out',
                    clearProps: 'transform,opacity',
                }
            );
        }

        if ('MutationObserver' in window) {
            let wasOpen = detailsPage.classList.contains('open');
            const observer = new MutationObserver(() => {
                const isOpen = detailsPage.classList.contains('open');
                if (isOpen && !wasOpen) {
                    if (dpBackdrop) dpBackdrop.style.transform = '';
                    animateDetailEntrance();
                }
                wasOpen = isOpen;
            });
            observer.observe(detailsPage, { attributes: true, attributeFilter: ['class'] });
        }

        // Smooth backdrop depth glide while scrolling inside the movie detail scene
        let scrollRaf = 0;
        detailsPage.addEventListener('scroll', () => {
            if (!dpBackdrop || prefersReducedMotion()) return;
            if (scrollRaf) return;
            scrollRaf = window.requestAnimationFrame(() => {
                scrollRaf = 0;
                const y = Math.min(detailsPage.scrollTop, 560);
                const shift = (y * 0.28).toFixed(1);
                const scale = (1 + y * 0.00018).toFixed(4);
                dpBackdrop.style.transform = `translate3d(0, ${shift}px, 0) scale(${scale})`;
            });
        }, { passive: true });
    }

    /* ------------------------------------------------------------
     * Boot
     * ------------------------------------------------------------ */
    function boot() {
        initHeroAtmosphere();
        initHeroMotion();
        init3DCardTilt();
        initScrollChapters();
        initDetailSceneMotion();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', boot, { once: true });
    } else {
        boot();
    }
})();
