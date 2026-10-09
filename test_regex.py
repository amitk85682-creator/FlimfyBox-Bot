import re
s = 'Episode : 12 Language : Hindi Dub Quality :'
p = re.compile(r'^(?:\W|episode\s*\W*\s*\d+|ep\s*\W*\s*\d+|e\d+|season\s*\W*\s*\d+|s\d+|quality\s*\W*\s*|1080p|720p|480p|2160p|4k|web-dl|webrip|bluray|hdrip|camrip|language\s*\W*\s*|hindi dub|dub by\s*\W*\s*|@\w+|join now|subscribe|download|powered by|hindi|tamil|telugu|malayalam|kannada|english|dual audio)+$', re.IGNORECASE)
print(repr(s), '->', bool(p.match(s)))
