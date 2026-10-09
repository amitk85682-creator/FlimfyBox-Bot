import subprocess
import re
import os

def fix_mojibake():
    # 1. Get original main.py bytes
    result = subprocess.run(["git", "show", "546ab09:main.py"], capture_output=True)
    original_text = result.stdout.decode("utf-8")
    
    # 2. Extract raw HTML block
    start_str = '    html = r"""<!DOCTYPE html>\n'
    end_str = '</html>"""\n'
    start_idx = original_text.find(start_str)
    if start_idx == -1:
        start_str = '    html = r"""<!DOCTYPE html>'
        start_idx = original_text.find(start_str)
        
    end_idx = original_text.find(end_str, start_idx)
    raw_html = original_text[start_idx + len('    html = r"""'):end_idx + len('</html>')]
    
    # 3. Extract original HTML, CSS, JS
    # CSS is between <style> and </style>
    style_start = raw_html.find("<style>")
    style_end = raw_html.find("</style>", style_start)
    original_css = raw_html[style_start + len("<style>"):style_end].strip()
    
    # JS is between <script> and </script> (the one at the bottom, not the telegram web app one)
    # The bottom script usually starts on its own line
    script_start = raw_html.rfind("<script>")
    script_end = raw_html.rfind("</script>")
    original_js = raw_html[script_start + len("<script>"):script_end].strip()
    
    # 4. Regenerate files in UTF-8 (no BOM)
    with open("static/miniapp/app.css", "w", encoding="utf-8") as f:
        f.write(original_css)
        
    with open("static/miniapp/app.js", "w", encoding="utf-8") as f:
        f.write(original_js)
        
    # HTML: Remove inline style and script, inject link and script src
    lines = raw_html.split("\n")
    out_lines = []
    in_style = False
    in_script = False
    style_added = False
    script_added = False
    
    for line in lines:
        if "<style>" in line and not in_script:
            in_style = True
            if not style_added:
                out_lines.append("    <link rel=\"stylesheet\" href=\"{{ url_for('static', filename='miniapp/app.css') }}\">")
                style_added = True
            continue
            
        if "</style>" in line and in_style:
            in_style = False
            continue
            
        if "<script>" in line and not in_style:
            if line.strip() == "<script>":
                in_script = True
                if not script_added:
                    out_lines.append("    <script src=\"{{ url_for('static', filename='miniapp/app.js') }}\"></script>")
                    script_added = True
                continue
                
        if "</script>" in line and in_script:
            if line.strip() == "</script>":
                in_script = False
                continue
                
        if not in_style and not in_script:
            out_lines.append(line)
            
    with open("templates/mini_app.html", "w", encoding="utf-8") as f:
        f.write("\n".join(out_lines))
        
    print("Files regenerated from original UTF-8 source.")

if __name__ == "__main__":
    fix_mojibake()
