#!/usr/bin/env python3
"""Kolektor Reddit via Arctic Shift API (GRATIS, tanpa login, tanpa API key).

Strategi: title search per keyword di r/indonesia -> comments/tree per post.
Dedup by post_id lawan CSV yang sudah ada. Berhenti halus saat budget menit habis.
Skema CSV: 20 kolom, sama dengan dataset_karhutla_reddit_nlp.csv (skripsi).
"""
import csv, json, os, re, sys, time, random, datetime
import urllib.request, urllib.parse

ROOT = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(ROOT, 'dataset_karhutla_reddit_nlp.csv')
STATE_PATH = os.path.join(ROOT, 'reddit_arctic_state.json')
BASE = 'https://arctic-shift.photon-reddit.com'
MAX_MINUTES = float(os.environ.get('MAX_MINUTES', '55'))

COLS = ['post_id', 'parent_post_id', 'post_type', 'platform', 'scraped_at',
        'created_at', 'search_query', 'author_username', 'author_display_name',
        'is_verified', 'raw_text', 'clean_text_initial', 'language', 'hashtags',
        'mentions', 'reply_count', 'repost_count', 'like_count', 'view_count',
        'post_url']

KEYWORDS = [
    'karhutla', 'kebakaran hutan', 'karhutla sumatera', 'karhutla kalimantan',
    'kebakaran hutan riau', 'kebakaran hutan jambi',
    'kebakaran hutan kalimantan tengah', 'kebakaran hutan kalimantan barat',
    'kebakaran lahan gambut', 'titik api karhutla',
    'penanganan karhutla pemerintah', 'bnpb karhutla', 'manggala agni karhutla',
    'klhk kebakaran hutan', 'tersangka pembakar hutan',
    'kebakaran hutan disengaja', 'bakar hutan buka lahan',
    'pembakaran lahan sawit', 'karhutla sengaja korporasi',
    'buka lahan sawit dibakar', 'perusahaan bakar lahan', 'kabut asap karhutla',
]
SUBREDDITS = ['indonesia']

WIB = datetime.timezone(datetime.timedelta(hours=7))
UA = {'User-Agent': 'Mozilla/5.0 (research; skripsi-dataset)'}
URL_RE = re.compile(r'https?://\S+')
HASHTAG_RE = re.compile(r'#(\w+)')
MENTION_RE = re.compile(r'@(\w+)')
T0 = time.time()

def log(m):
    print(f"[{datetime.datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')}] {m}", flush=True)

def budget_left():
    return (T0 + MAX_MINUTES * 60) - time.time()

def api(path, params, timeout=45, max_retries=8):
    """GET dengan retry backoff (Arctic Shift suka throttle: 'Timeout. Maybe slow down a bit')."""
    url = BASE + path + '?' + urllib.parse.urlencode(params)
    for attempt in range(max_retries):
        if budget_left() < 120:
            raise IOError('budget menit hampir habis, stop halus')
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read().decode('utf-8', errors='ignore')
                if r.status == 200:
                    d = json.loads(body)
                    if isinstance(d, dict) and d.get('data') is None and d.get('error'):
                        raise IOError(f"API error: {d.get('error')}")
                    return d
                raise IOError(f'HTTP {r.status}')
        except IOError:
            raise
        except Exception as e:
            wait = (20 * (2 ** attempt)) + random.uniform(0, 8)
            log(f"[RETRY] {path} {type(e).__name__} (coba {attempt+1}/{max_retries}), tidur {wait:.0f}s")
            time.sleep(wait)
    raise IOError(f'gagal {max_retries}x: {path}')

def clean(t):
    t = URL_RE.sub('', t or '')
    return re.sub(r'\s+', ' ', t).strip()

def meaningful(t):
    t = URL_RE.sub(' ', t or '')
    return len(t.split()) >= 3

def ts_iso(ts):
    try:
        return datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    except Exception:
        return ''

def load_state():
    try:
        return json.load(open(STATE_PATH, encoding='utf-8'))
    except Exception:
        return {'done_posts': [], 'total': 0, 'kw_done': []}

def save_state(st):
    json.dump(st, open(STATE_PATH, 'w', encoding='utf-8'))

def load_seen():
    seen = set()
    try:
        with open(CSV_PATH, encoding='utf-8-sig', newline='') as f:
            for row in csv.DictReader(f):
                if row.get('post_id'):
                    seen.add(row['post_id'])
    except FileNotFoundError:
        pass
    return seen

def search_posts(keyword, subreddit, limit=100):
    d = api('/api/posts/search', {
        'subreddit': subreddit, 'title': keyword, 'limit': limit,
        'fields': 'title,id,num_comments,created_utc,author,selftext,score'})
    return (d or {}).get('data') or []

def fetch_tree(post_id):
    d = api('/api/comments/tree', {'link_id': 't3_' + post_id, 'limit': 10000})
    data = (d or {}).get('data') or []
    out = []
    def walk(items):
        for it in items or []:
            out.append(it)
            walk(it.get('replies') or [])
    walk(data)
    return out

def main():
    st = load_state()
    seen = load_seen()
    st['total'] = len(seen)
    done_posts = set(st['done_posts'])
    kw_done = set(st['kw_done'])
    log(f"[START] terlihat {len(seen)} baris di CSV, budget {MAX_MINUTES:.0f} mnt")
    if not seen:
        with open(CSV_PATH, 'w', encoding='utf-8', newline='') as f:
            f.write('\ufeff' + ','.join(COLS) + '\n')
    buf = []

    def flush():
        if not buf:
            return
        with open(CSV_PATH, 'a', encoding='utf-8', newline='') as f:
            csv.DictWriter(f, fieldnames=COLS).writerows(buf)
        st['total'] += len(buf)
        buf.clear()
        save_state(st)
        log(f"[CHECKPOINT] Tersimpan: {st['total']} | Platform: reddit")

    def add(row):
        if not row['post_id'] or row['post_id'] in seen:
            return False
        if not meaningful(row['raw_text']):
            return False
        seen.add(row['post_id'])
        buf.append(row)
        if len(buf) >= 50:
            flush()
        return True

    now = datetime.datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S')
    try:
        for kw in KEYWORDS:
            if budget_left() < 180:
                log('[STOP] budget menit habis, stop halus')
                break
            for sub in SUBREDDITS:
                key = f'{kw}@{sub}'
                if key in kw_done:
                    continue
                log(f"[SEARCH] title={kw} r/{sub}")
                try:
                    posts = search_posts(kw, sub)
                except IOError as e:
                    log(f"[SEARCH-FAIL] {key}: {e}")
                    continue
                log(f"[SEARCH] {key}: {len(posts)} postingan")
                for p in posts:
                    if budget_left() < 180:
                        log('[STOP] budget menit habis, stop halus')
                        break
                    pid = str(p.get('id'))
                    if pid in done_posts:
                        continue
                    title = p.get('title') or ''
                    body = p.get('selftext') or ''
                    txt = clean(title + '\n' + body)
                    row = {'post_id': 't3_' + pid, 'parent_post_id': '', 'post_type': 'post',
                           'platform': 'reddit', 'scraped_at': now,
                           'created_at': ts_iso(p.get('created_utc')), 'search_query': kw,
                           'author_username': p.get('author') or '', 'author_display_name': p.get('author') or '',
                           'is_verified': 'False', 'raw_text': txt, 'clean_text_initial': txt,
                           'language': 'id',
                           'hashtags': json.dumps(['#' + h for h in HASHTAG_RE.findall(txt)], ensure_ascii=False),
                           'mentions': json.dumps(['@' + u for u in MENTION_RE.findall(txt)], ensure_ascii=False),
                           'reply_count': int(p.get('num_comments') or 0), 'repost_count': 0,
                           'like_count': int(p.get('score') or 0), 'view_count': 0,
                           'post_url': 'https://redd.it/' + pid}
                    add(row)
                    tree_ok = True
                    if int(p.get('num_comments') or 0) > 0:
                        try:
                            tree = fetch_tree(pid)
                        except IOError as e:
                            log(f"[TREE-FAIL] {pid}: {e}")
                            tree = []
                            if 'budget' in str(e):
                                tree_ok = False
                        for c in tree:
                            cid = str(c.get('id'))
                            cpid = str(c.get('parent_id') or '')
                            ptype = 'comment' if cpid == 't3_' + pid else 'reply_comment'
                            ctxt = clean(c.get('body'))
                            add({'post_id': cid, 'parent_post_id': cpid, 'post_type': ptype,
                                 'platform': 'reddit', 'scraped_at': now,
                                 'created_at': ts_iso(c.get('created_utc')), 'search_query': kw,
                                 'author_username': c.get('author') or '', 'author_display_name': c.get('author') or '',
                                 'is_verified': 'False', 'raw_text': ctxt, 'clean_text_initial': ctxt,
                                 'language': 'id',
                                 'hashtags': json.dumps(['#' + h for h in HASHTAG_RE.findall(ctxt)], ensure_ascii=False),
                                 'mentions': json.dumps(['@' + u for u in MENTION_RE.findall(ctxt)], ensure_ascii=False),
                                 'reply_count': 0, 'repost_count': 0,
                                 'like_count': int(c.get('score') or 0), 'view_count': 0,
                                 'post_url': 'https://redd.it/' + pid})
                        log(f"[POST] {pid}: {len(tree)} komentar ditarik")
                    if tree_ok:
                        done_posts.add(pid)
                        st['done_posts'] = sorted(done_posts)
                    else:
                        log(f"[POST] {pid}: budget habis, tree dicoba lagi run berikut")
                    save_state(st)
                    flush()
                    time.sleep(12 + random.uniform(0, 6))
                kw_done.add(key)
                st['kw_done'] = sorted(kw_done)
                save_state(st)
                time.sleep(5)
    except KeyboardInterrupt:
        log('[STOP] interupsi')
    st['done_posts'] = sorted(done_posts)
    st['kw_done'] = sorted(kw_done)
    flush()
    save_state(st)
    log(f"[DONE] total={st['total']} -> {CSV_PATH}")

if __name__ == '__main__':
    main()
