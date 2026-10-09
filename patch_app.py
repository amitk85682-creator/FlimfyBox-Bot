import re

def main():
    with open("static/miniapp/app.js", "r", encoding="utf-8") as f:
        content = f.read()

    # The block we want to replace is inside openDetails:
    #                         // Download links
    #                         if (m.files && m.files.length) { ... } else { ... }
    
    start_str = "                        // Download links"
    end_str = "                        document.getElementById('detailsPage').classList.add('open');"
    
    start_idx = content.find(start_str)
    end_idx = content.find(end_str, start_idx)
    
    if start_idx == -1 or end_idx == -1:
        print("Could not find the block to replace")
        return

    new_block = """                        // === PREMIUM SEASON/EPISODE LOGIC ===
                        const seasonsContainer = document.getElementById('dpSeasons');
                        const linksContainer = document.getElementById('dpLinks');
                        seasonsContainer.innerHTML = '';
                        linksContainer.innerHTML = '';

                        if (m.files && m.files.length) {
                            // Parse extra_info for all files
                            let hasSeasons = false;
                            const seasonsMap = {}; // { season_num: { episodes: { ep_num: { title, qualities: [] } } } }
                            const movieFiles = []; // Files with no season

                            m.files.forEach(f => {
                                let info = (f.extra_info || "").toUpperCase();
                                let s = null, e = null, epStr = null;
                                
                                // Parse season
                                let sMatch = info.match(/S(\\d+)|SEASON\\s*(\\d+)/);
                                if (sMatch) {
                                    s = parseInt(sMatch[1] || sMatch[2], 10);
                                }

                                // Parse episode
                                let eMatch = info.match(/E(\\d+(?:-\\d+)?)|EP\\s*(\\d+(?:-\\d+)?)|EPISODE\\s*(\\d+(?:-\\d+)?)/);
                                if (eMatch) {
                                    epStr = eMatch[1] || eMatch[2] || eMatch[3];
                                    e = parseInt(epStr.split('-')[0], 10);
                                }

                                if (s !== null) {
                                    hasSeasons = true;
                                    if (!seasonsMap[s]) seasonsMap[s] = { episodes: {} };
                                    
                                    let sortEp = e !== null ? e : 0;
                                    let displayTitle = e !== null ? `EP ${epStr.padStart(2, '0')}` : `Season ${s} Extras`;
                                    
                                    if (!seasonsMap[s].episodes[sortEp]) {
                                        seasonsMap[s].episodes[sortEp] = { title: displayTitle, qualities: [] };
                                    }
                                    seasonsMap[s].episodes[sortEp].qualities.push(f);
                                } else {
                                    movieFiles.push(f);
                                }
                            });

                            if (hasSeasons) {
                                // Render Seasons Selector
                                const seasonNumbers = Object.keys(seasonsMap).map(Number).sort((a, b) => a - b);
                                
                                let seasonsHtml = `<div class="season-scroll-wrapper"><div class="season-pill-container" id="seasonPillContainer">`;
                                seasonNumbers.forEach(sn => {
                                    seasonsHtml += `<div class="season-pill" data-season="${sn}" onclick="selectSeason(${m.id}, ${sn})">Season ${sn}</div>`;
                                });
                                seasonsHtml += `</div></div>`;
                                seasonsContainer.innerHTML = seasonsHtml;

                                // Expose data globally for fast switching
                                window.currentMovieSeasons = seasonsMap;
                                
                                // Auto-select lowest season
                                selectSeason(m.id, seasonNumbers[0]);
                            } else {
                                // Normal Movie
                                let links = '<div class="dl-heading">AVAILABLE QUALITIES</div>';
                                m.files.forEach(f => {
                                    links += `
                                        <button class="dl-btn" onclick="downloadMovie(${m.id})">
                                            <span class="quality-text">📁 ${f.quality} <span class="file-size">[${f.size || 'N/A'}]</span></span>
                                            <span class="action">Get</span>
                                        </button>
                                    `;
                                });
                                linksContainer.innerHTML = links;
                            }
                        } else {
                            linksContainer.innerHTML = `
                                <div class="dl-heading">DOWNLOAD</div>
                                <button class="dl-btn" onclick="downloadMovie(${m.id})">
                                    <span class="quality-text">📁 1080p Full HD</span>
                                    <span class="action">Get</span>
                                </button>
                            `;
                        }
"""
    
    content = content[:start_idx] + new_block + content[end_idx:]

    # Also add the global selectSeason function
    # Find the end of window.openDetails
    func_end = content.find("        window.closeDetails = function() {")
    
    select_season_func = """
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
                html += `
                <div class="episode-card">
                    <div class="ep-header">
                        <div class="ep-title">${ep.title}</div>
                    </div>
                    <div class="ep-qualities">`;
                
                ep.qualities.forEach(q => {
                    html += `
                        <button class="ep-dl-btn" onclick="downloadMovie(${movieId})">
                            <span class="ep-qtext">${q.quality} <span class="ep-size">${q.size || ''}</span></span>
                            <span class="ep-action"><i class="fas fa-download"></i></span>
                        </button>
                    `;
                });
                
                html += `</div></div>`;
            });
            html += `</div>`;
            
            linksContainer.innerHTML = html;
        };

"""
    content = content[:func_end] + select_season_func + content[func_end:]

    with open("static/miniapp/app.js", "w", encoding="utf-8") as f:
        f.write(content)
        
    print("Patched app.js successfully.")

if __name__ == "__main__":
    main()
