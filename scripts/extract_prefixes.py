import html
import re
import urllib.request
from urllib.parse import urljoin
from pathlib import Path

index_url = 'https://www.iana.org/domains/root/db'
with urllib.request.urlopen(index_url, timeout=60) as response:
    index = response.read().decode('utf-8', 'replace')

row_re = re.compile(r'<tr>\s*<td>\s*<span class="domain tld"><a href="([^"]+)">\.([^<]+)</a></span></td>\s*<td>([^<]+)</td>', re.S)
rows = []
seen = set()
for href, _label, tld_type in row_re.findall(index):
    if html.unescape(tld_type).strip() != 'country-code':
        continue

    tld_id = href.rsplit('/', 1)[-1].removesuffix('.html').lower()
    page_url = urljoin(index_url, href)
    with urllib.request.urlopen(page_url, timeout=60) as response:
        page = response.read().decode('utf-8', 'replace')
    comment_match = re.search(r'<!-- \(Country-code top-level domain designated for (.*?)\) -->', page)
    if not comment_match:
        raise SystemExit(f'Missing country designation for {tld_id}')

    country_name = html.unescape(comment_match.group(1)).strip()
    country_key = re.sub(r'[^a-z0-9]+', '-', country_name.lower()).strip('-')
    row = (country_key, country_name, f'.{tld_id}')
    if row not in seen:
        rows.append(row)
        seen.add(row)

rows.sort(key=lambda row: (row[0], row[2]))
content = 'country_key\tcountry_name\tsuffix\n' + '"''"'.join(
    f'{country_key}\t{country_name}\t{suffix}\n'
    for country_key, country_name, suffix in rows
)
Path('src/main/resources/sitemap-filter/country-suffixes.tsv').write_text(content, encoding='utf-8')
print(f'wrote {len(rows)} rows')
