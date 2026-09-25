import re

def patch_webapp_routes():
    with open('webapp_routes.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update API response to include `id` and `languages`
    old_api = """            cur.execute("SELECT quality, file_size, extra_info FROM movie_files WHERE movie_id = %s", (movie_id,))
            files = [{'quality': f[0], 'size': f[1], 'extra_info': f[2] if len(f) > 2 else ''} for f in cur.fetchall()]"""
            
    new_api = """            cur.execute(\"\"\"
                SELECT id, quality, file_size, extra_info, languages 
                FROM movie_files 
                WHERE movie_id = %s AND (url IS NOT NULL OR file_id IS NOT NULL)
                ORDER BY CASE quality
                    WHEN '4K' THEN 1
                    WHEN 'HD Quality' THEN 2
                    WHEN 'Standart Quality'  THEN 3
                    WHEN 'Low Quality'  THEN 4
                    ELSE 5
                END DESC
            \"\"\", (movie_id,))
            files = [{'id': f[0], 'quality': f[1], 'size': f[2], 'extra_info': f[3] if len(f) > 3 else '', 'languages': f[4] if len(f) > 4 else ''} for f in cur.fetchall()]"""

    if old_api in content:
        content = content.replace(old_api, new_api)
    else:
        print("Could not find old API block in webapp_routes.py")

    # 2. Add /watch/file/<file_id> route
    if "@flask_app.route('/watch/file/<int:file_id>')" not in content:
        # Find where to insert
        insert_idx = content.find("    # 🔐 SECRET LINK GENERATOR API (Auto Delete Logic)")
        if insert_idx != -1:
            new_routes = """
    # 🛡️ MIDDLEMAN REDIRECT PAGE (Anti-Bot) FOR SPECIFIC FILE
    @flask_app.route('/watch/file/<int:file_id>')
    def secure_watch_file(file_id):
        html = \"\"\"
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>FlimfyBox - Verifying Secure Connection...</title>
            <style>
                body { background: #09090b; color: white; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; font-family: sans-serif; }
                .loader { border: 4px solid rgba(255,255,255,0.1); border-top: 4px solid #f43f5e; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin-bottom: 20px; }
                @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
            </style>
        </head>
        <body>
            <div class="loader"></div>
            <h3>Securely verifying your connection...</h3>
            <p style="color: #a1a1aa; font-size: 13px;">Please wait 2 seconds. You will be redirected automatically.</p>
            
            <script>
                setTimeout(() => {
                    fetch('/api/gen_link/file/\"\"\" + str(file_id) + \"\"\"', { method: 'POST' })
                    .then(response => response.json())
                    .then(data => {
                        if(data.url) {
                            window.location.href = data.url; 
                        } else {
                            document.body.innerHTML = "<h3>❌ Server Error. Please try again.</h3>";
                        }
                    }).catch(e => {
                        document.body.innerHTML = "<h3>❌ Connection failed.</h3>";
                    });
                }, 1500); 
            </script>
        </body>
        </html>
        \"\"\"
        return html

    @flask_app.route('/api/gen_link/file/<int:file_id>', methods=['POST'])
    def gen_secure_link_file(file_id):
        token = "tmpf_" + secrets.token_hex(6)
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM temp_links WHERE created_at < NOW() - INTERVAL '1 minute'")
                
                # Check and add file_id column if it doesn't exist
                cur.execute(\"\"\"
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='temp_links' AND column_name='file_id';
                \"\"\")
                if not cur.fetchone():
                    cur.execute("ALTER TABLE temp_links ADD COLUMN IF NOT EXISTS file_id INTEGER;")
                    
                cur.execute("INSERT INTO temp_links (token, file_id) VALUES (%s, %s)", (token, file_id))
                conn.commit()
                cur.close()
            except Exception as e:
                logger.error(f"Token Error: {e}")
            finally:
                close_db_connection(conn)
                
        bot_username = os.environ.get('BOT_USERNAME', 'FlimfyBoxBot')
        tg_url = f"tg://resolve?domain={bot_username}&start={token}"
        return jsonify({"url": tg_url})

"""
            content = content[:insert_idx] + new_routes + content[insert_idx:]
            
    with open('webapp_routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Patched webapp_routes.py")


def patch_main_py():
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    target = '            # --- CASE NAYA: DIRECT FILE CLICK FROM TEXT LINK ---'
    
    if 'payload.startswith("tmpf_")' not in content:
        insert_idx = content.find(target)
        if insert_idx != -1:
            new_logic = """
            # 🔐 NAYA: ANTI-BOT FILE-SPECIFIC LINK SYSTEM
            if payload.startswith("tmpf_"):
                conn = get_db_connection()
                if not conn:
                    await context.bot.send_message(chat_id, "❌ System Error.")
                    return

                try:
                    cur = conn.cursor()
                    cur.execute("SELECT file_id, created_at FROM temp_links WHERE token = %s", (payload,))
                    res = cur.fetchone()
                    
                    cur.execute("DELETE FROM temp_links WHERE token = %s", (payload,))
                    conn.commit()
                    
                    if not res:
                        msg = await context.bot.send_message(chat_id, "❌ <b>Link Expired ya Invalid hai!</b>", parse_mode='HTML')
                        track_message_for_deletion(context, chat_id, msg.message_id, 15)
                        return
                    
                    file_id_pk, created_at = res
                    time_diff = (datetime.now() - created_at).total_seconds()
                    
                    if time_diff > 60:
                        msg = await context.bot.send_message(chat_id, "❌ <b>Link Expired!</b>\\nYeh link sirf 60 seconds ke liye valid tha.", parse_mode='HTML')
                        track_message_for_deletion(context, chat_id, msg.message_id, 15)
                        return
                    
                    # File ID se file ka data fetch karo
                    cur.execute("SELECT movie_id, quality, url, file_id FROM movie_files WHERE id = %s", (file_id_pk,))
                    f_res = cur.fetchone()
                    if not f_res:
                        msg = await context.bot.send_message(chat_id, "❌ <b>File not found in database!</b>", parse_mode='HTML')
                        track_message_for_deletion(context, chat_id, msg.message_id, 15)
                        return
                        
                    movie_id, quality, url, tg_file_id = f_res
                    
                    # Fetch Title
                    cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                    t_res = cur.fetchone()
                    title = t_res[0] if t_res else "Requested File"
                    cur.close()
                    
                    await send_movie_to_user(update, context, movie_id, title, url, tg_file_id, send_warning=True)
                    logger.info(f"✅ Secure file token {payload} used successfully for file pk {file_id_pk}")
                    return

                except Exception as e:
                    logger.error(f"Temp File Link Error: {e}")
                    await context.bot.send_message(chat_id, "❌ Processing error.")
                    return
                finally:
                    close_db_connection(conn)

"""
            content = content[:insert_idx] + new_logic + content[insert_idx:]
            
    with open('main.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Patched main.py")


def patch_app_js():
    with open('static/miniapp/app.js', 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update duplicate checking
    old_dup = "seasonsMap[s].episodes[sortEp].qualities.push(f);"
    new_dup = """                                    const dup = seasonsMap[s].episodes[sortEp].qualities.find(q => q.quality === f.quality && q.size === f.size);
                                    if (!dup) {
                                        seasonsMap[s].episodes[sortEp].qualities.push(f);
                                    }"""
    if old_dup in content:
        content = content.replace(old_dup, new_dup)

    # 2. Update button download function call in selectSeason
    old_btn = 'onclick="downloadMovie(${movieId})"'
    new_btn = 'onclick="downloadFile(${q.id})"'
    if old_btn in content:
        content = content.replace(old_btn, new_btn)

    # 3. Add downloadFile function if missing
    if "window.downloadFile = function(fileId)" not in content:
        new_func = """
        window.downloadFile = function(fileId) {
            tg.HapticFeedback.impactOccurred('heavy');
            tg.openLink(`https://flimfybox-bot-yht0.onrender.com/watch/file/${fileId}`);
        };
"""
        content = content.replace("window.downloadMovie = function(id) {", new_func + "        window.downloadMovie = function(id) {")

    with open('static/miniapp/app.js', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Patched app.js")

if __name__ == '__main__':
    patch_webapp_routes()
    patch_main_py()
    patch_app_js()
