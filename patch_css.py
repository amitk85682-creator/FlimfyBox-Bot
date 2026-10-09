def main():
    css = """

/* === PREMIUM SEASON & EPISODE UI === */
.seasons-container {
    margin: 20px 0 10px 0;
}

.season-scroll-wrapper {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none; /* Firefox */
    padding-bottom: 5px;
}
.season-scroll-wrapper::-webkit-scrollbar {
    display: none; /* Chrome/Safari */
}

.season-pill-container {
    display: flex;
    gap: 12px;
    padding: 0 5px;
}

.season-pill {
    white-space: nowrap;
    background: rgba(255, 255, 255, 0.05);
    color: rgba(255, 255, 255, 0.6);
    padding: 10px 20px;
    border-radius: 20px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid rgba(255, 255, 255, 0.1);
    transition: all 0.3s ease;
    user-select: none;
    -webkit-tap-highlight-color: transparent;
}

.season-pill:hover {
    background: rgba(255, 255, 255, 0.1);
}

.season-pill.active {
    background: var(--primary);
    color: #000;
    border-color: var(--primary);
    box-shadow: 0 4px 15px rgba(var(--primary-rgb, 255,215,0), 0.3);
}

/* Episodes List */
.episodes-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin-top: 10px;
}

.episode-card {
    background: rgba(25, 25, 30, 0.8);
    border-radius: 12px;
    padding: 15px;
    border: 1px solid rgba(255, 255, 255, 0.05);
    backdrop-filter: blur(10px);
}

.ep-header {
    margin-bottom: 12px;
    padding-bottom: 12px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.ep-title {
    font-size: 16px;
    font-weight: 800;
    color: #fff;
    letter-spacing: 0.5px;
}

.ep-qualities {
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.ep-dl-btn {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: #fff;
    padding: 10px 15px;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s ease;
    -webkit-tap-highlight-color: transparent;
}

.ep-dl-btn:hover {
    background: rgba(255, 255, 255, 0.08);
    border-color: rgba(255, 255, 255, 0.2);
}

.ep-dl-btn:active {
    transform: scale(0.98);
}

.ep-qtext {
    display: flex;
    gap: 10px;
    align-items: center;
}

.ep-size {
    color: rgba(255, 255, 255, 0.4);
    font-size: 12px;
    font-weight: 400;
}

.ep-action {
    background: rgba(255, 215, 0, 0.1);
    color: var(--primary);
    width: 28px;
    height: 28px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    transition: all 0.2s ease;
}

.ep-dl-btn:hover .ep-action {
    background: var(--primary);
    color: #000;
}

.empty-season {
    text-align: center;
    padding: 30px 15px;
    color: rgba(255,255,255,0.5);
    font-size: 14px;
    background: rgba(0,0,0,0.2);
    border-radius: 12px;
    margin-top: 15px;
}

/* Ensure global primary color fallback if not defined */
:root {
    --primary-rgb: 255, 215, 0; /* Assuming goldish based on existing UI */
}
"""
    with open("static/miniapp/app.css", "a", encoding="utf-8") as f:
        f.write(css)
    print("Patched app.css successfully.")

if __name__ == "__main__":
    main()
