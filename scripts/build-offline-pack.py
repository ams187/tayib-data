"""Offline packs, as Yuka does: per country, the 100,000 most-scanned products sold there, without photos.

Streams the Open Food Facts JSONL dump (ODbL) once and writes one SQLite file per country the app downloads:
  curl -sL https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz | gzip -dc \
    | python3 scripts/build-offline-pack.py out FR=en:france GB=en:united-kingdom ...
France is out/offline-pack.db, the others out/offline-pack-<cc>.db. The countries built are printed on stdout:
they are the ones listed in OFFLINE_COUNTRIES (src/lib/region.ts).
Rows are raw-deflated with a shared dictionary (meta.dictionary). Each row keeps only the fields `parseProduct` (src/lib/open-food-facts.ts) reads, in the API v3.4 shape.
"""
import heapq
import json
import sqlite3
import sys
import time
import zlib

LIMIT = 100_000
FIELDS = ['code', 'product_name', 'product_name_fr', 'brands', 'allergens_tags', 'traces_tags', 'emb_codes_tags', 'origins_tags', 'manufacturing_places_tags',
          'ingredients_text', 'ingredients_text_fr', 'lang', 'nutriscore_grade', 'nova_group', 'environmental_score_grade',
          'additives_tags', 'categories_tags', 'labels_tags', 'packaging_materials_tags', 'serving_quantity', 'last_modified_t']
INGREDIENT_KEYS = ('id', 'text', 'vegan', 'vegetarian', 'percent', 'percent_estimate', 'from_palm_oil', 'additive_class')
COMPONENT_KEYS = ('id', 'points', 'points_max', 'value', 'unit')


def ingredients(items, depth=0):
    if not isinstance(items, list) or depth > 10:
        return None
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        slim = {key: item[key] for key in INGREDIENT_KEYS if item.get(key) not in (None, '')}
        # Size: one decimal is all the app shows, and only « yes » palm oil matters.
        if 'percent' in slim:
            slim.pop('percent_estimate', None)
        elif isinstance(slim.get('percent_estimate'), (int, float)):
            slim['percent_estimate'] = round(slim['percent_estimate'], 1)
        if slim.get('from_palm_oil') != 'yes':
            slim.pop('from_palm_oil', None)
        children = ingredients(item.get('ingredients'), depth + 1)
        if children:
            slim['ingredients'] = children
        out.append(slim)
    return out


def slim(product):
    row = {key: product[key] for key in FIELDS if product.get(key) not in (None, '', [])}
    # The parser reads the French list first: keep just the one it would use.
    if row.get('ingredients_text_fr'):
        row.pop('ingredients_text', None)
    nutriments = product.get('nutriments')
    if isinstance(nutriments, dict):
        row['nutriments'] = {key: value for key, value in nutriments.items() if key.endswith('_100g') and isinstance(value, (int, float))}
    tree = ingredients(product.get('ingredients'))
    if tree:
        row['ingredients'] = tree
    components = (product.get('nutriscore_data') or {}).get('components')
    if isinstance(components, dict):
        row['nutriscore_data'] = {'components': {side: [{key: c.get(key) for key in COMPONENT_KEYS} for c in components.get(side) or [] if isinstance(c, dict)] for side in ('negative', 'positive')}}
    return row


MIN_PRODUCTS = 2_000  # below this a pack would disappoint: the country gets no offline mode (nothing advertises it)


def write_pack(path, top):
    # A dictionary of typical rows makes each tiny row compress ~5x instead of ~3x.
    rows = [(code, zlib.decompress(packed).decode()) for _, code, packed in top]
    sample = [data.encode() for _, data in rows[::max(1, len(rows) // 400)]]
    dictionary = b''.join(sample)[-32768:]

    def deflate(data):
        compressor = zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY, dictionary)
        return compressor.compress(data.encode()) + compressor.flush()

    db = sqlite3.connect(path)
    db.executescript('DROP TABLE IF EXISTS products; DROP TABLE IF EXISTS meta;'
                     'CREATE TABLE products (code TEXT PRIMARY KEY, data BLOB NOT NULL) WITHOUT ROWID;'
                     'CREATE TABLE meta (key TEXT PRIMARY KEY, value NOT NULL);')
    db.executemany('INSERT OR REPLACE INTO products VALUES (?, ?)', [(code, deflate(data)) for code, data in rows])
    db.executemany('INSERT INTO meta VALUES (?, ?)', [('built_at', str(int(time.time() * 1000))), ('count', str(len(rows))), ('dictionary', dictionary)])
    db.commit()
    db.execute('VACUUM')
    db.close()


def main(out_dir, countries):
    """countries: {'FR': 'en:france', ...}. One pass over the dump builds every country's pack."""
    by_tag = {tag: cc for cc, tag in countries.items()}
    tops = {cc: [] for cc in countries}  # per country, a min-heap of (scans, code, zlib(json))
    for line in sys.stdin:
        try:
            product = json.loads(line)
        except ValueError:
            continue
        scans = product.get('unique_scans_n') or 0
        code = product.get('code')
        markets = [by_tag[tag] for tag in (product.get('countries_tags') or []) if tag in by_tag]
        if not scans or not code or not markets:
            continue
        if not (product.get('product_name_fr') or product.get('product_name')):
            continue
        packed = None
        for cc in markets:
            top = tops[cc]
            if len(top) == LIMIT and scans <= top[0][0]:
                continue
            # Kept compressed in memory: every country's heap lives at once.
            packed = packed or zlib.compress(json.dumps(slim(product), ensure_ascii=False, separators=(',', ':')).encode(), 1)
            (heapq.heapreplace if len(top) == LIMIT else heapq.heappush)(top, (scans, code, packed))

    built = []
    for cc, top in tops.items():
        if len(top) < MIN_PRODUCTS:
            print(f'{cc}: {len(top)} products, no pack', file=sys.stderr)
            continue
        name = 'offline-pack.db' if cc == 'FR' else f'offline-pack-{cc.lower()}.db'
        write_pack(f'{out_dir}/{name}', top)
        built.append(cc)
        print(f'{cc}: {len(top)} products, least scanned kept: {top[0][0]}', file=sys.stderr)
    print(' '.join(built))


if __name__ == '__main__':
    main(sys.argv[1], dict(arg.split('=', 1) for arg in sys.argv[2:]))
