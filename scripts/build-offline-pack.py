"""Offline pack, as Yuka does: the 100,000 most-scanned products sold in France, without photos.

Streams the Open Food Facts JSONL dump (ODbL) and writes a SQLite file the app downloads:
  curl -sL https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz | gzip -dc \
    | grep -F '"en:france"' | python3 scripts/build-offline-pack.py offline-pack.db
Rows are raw-deflated with a shared dictionary (meta.dictionary). Each row keeps only the fields `parseProduct` (src/lib/open-food-facts.ts) reads, in the API v3.4 shape.
"""
import heapq
import json
import sqlite3
import sys
import time
import zlib

LIMIT = 100_000
FIELDS = ['code', 'product_name', 'product_name_fr', 'brands', 'allergens_tags', 'traces_tags', 'emb_codes_tags',
          'ingredients_text', 'ingredients_text_fr', 'nutriscore_grade', 'nova_group', 'environmental_score_grade',
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


def main(path):
    top = []  # min-heap of (scans, code, json)
    for line in sys.stdin:
        try:
            product = json.loads(line)
        except ValueError:
            continue
        scans = product.get('unique_scans_n') or 0
        code = product.get('code')
        if not scans or not code or 'en:france' not in (product.get('countries_tags') or []):
            continue
        if not (product.get('product_name_fr') or product.get('product_name')):
            continue
        if len(top) == LIMIT and scans <= top[0][0]:
            continue
        entry = (scans, code, json.dumps(slim(product), ensure_ascii=False, separators=(',', ':')))
        (heapq.heapreplace if len(top) == LIMIT else heapq.heappush)(top, entry)

    # A dictionary of typical rows makes each tiny row compress ~5x instead of ~3x.
    sample = [data.encode() for _, _, data in top[::max(1, len(top) // 400)]]
    dictionary = b''.join(sample)[-32768:]

    def deflate(data):
        compressor = zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY, dictionary)
        return compressor.compress(data.encode()) + compressor.flush()

    db = sqlite3.connect(path)
    db.executescript('DROP TABLE IF EXISTS products; DROP TABLE IF EXISTS meta;'
                     'CREATE TABLE products (code TEXT PRIMARY KEY, data BLOB NOT NULL) WITHOUT ROWID;'
                     'CREATE TABLE meta (key TEXT PRIMARY KEY, value NOT NULL);')
    db.executemany('INSERT OR REPLACE INTO products VALUES (?, ?)', [(code, deflate(data)) for _, code, data in top])
    db.executemany('INSERT INTO meta VALUES (?, ?)', [('built_at', str(int(time.time() * 1000))), ('count', str(len(top))), ('dictionary', dictionary)])
    db.commit()
    db.execute('VACUUM')
    db.close()
    print(f'{len(top)} products, least scanned kept: {top[0][0] if top else 0}', file=sys.stderr)


if __name__ == '__main__':
    main(sys.argv[1])
