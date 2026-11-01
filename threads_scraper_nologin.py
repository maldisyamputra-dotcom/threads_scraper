"""
Scraper Threads TANPA LOGIN berbasis Playwright (untuk riset NLP / skripsi).

Adaptasi dari threads_scraper.py (unggahan user):
- Mode headless (browser tidak tampil di layar, bisa jalan di background).
- TANPA LOGIN sepenuhnya: tidak ada LOGIN_MANUAL, tidak ada state.json,
  tidak ada sesi tersimpan. Script berjalan sebagai tamu.
  Konsekuensinya, jujur saja:
  - Threads menampilkan konten terbatas untuk tamu (catatan script asli:
    ~30-60 post per keyword; tes kami 17 Okt 2026 malah ~20).
  - Modal ajakan login bisa muncul saat scroll; kalau sudah mentok
    (4x scroll tanpa post baru), script pindah ke keyword berikutnya.
  - Filter Recent kemungkinan tidak berlaku untuk tamu (tes kami: tidak ada
    tab Recent saat tidak login), jadi urutan hasil bisa campuran Top/Recent.
- Output mengikuti skema 20 kolom dataset karhutla yang sama dengan
  dataset_karhutla_threads_nlp.csv, supaya bisa digabung langsung.
- Postingan utama (main_post) + balasan (reply_comment, sejak 2 Nov 2026).
  Setelah pass kata kunci (±45 menit), fase balasan berjalan ±12 menit:
  untuk postingan baru run ini dengan reply_count >= 10 (maks 15 postingan,
  maks 30 balasan per postingan, yang reply_count-nya terbesar dulu),
  halaman postingannya dibuka dan balasan yang terlihat dikumpulkan
  (post_type=reply_comment, parent_post_id terisi, search_query = kata kunci
  induknya). Fase ini terjaga penuh: jika kena login wall (3 postingan
  beruntun tanpa balasan baru) ia berhenti sendiri, dan error apa pun tidak
  menggagalkan run — postingan utama yang sudah terkumpul tetap aman karena
  disimpan bertahap tiap kata kunci.
- Kalau file dataset_karhutla_threads_nlp.csv ada di folder yang sama,
  post_id yang sudah ada di sana dilewati (tidak duplikat dengan 213 baris lama).
- File hasil bersifat kumulatif: setiap run MENAMBAH baris baru ke file shard
  (bukan menimpa), dan post_id dari run sebelumnya ikut dilewati. Jadi
  workflow GitHub Actions bisa commit-back file yang sama berulang kali
  tanpa kehilangan hasil run sebelumnya.
- Tidak butuh secret apa pun (tanpa login, tanpa API key).

- Durasi target 60 menit per run: pass kata kunci ±45 menit (pindai berulang,
  jeda 3 menit antar pass), lalu fase balasan ±12 menit; dedup via post_id
  jadi hanya postingan/balasan baru yang ditambah.

- MODE SHARD (4 jalur paralel, sejak 2 Nov 2026): 32 kata kunci dibagi rata
  — env KEYWORD_SHARD="a" memakai 8 kata kunci pertama dan menulis
  threads_data_nologin_a.csv; "b" 8 berikutnya -> threads_data_nologin_b.csv;
  "c" 8 berikutnya -> threads_data_nologin_c.csv; "d" 8 terakhir ->
  threads_data_nologin_d.csv. Dedup post_id bersifat GLOBAL: semua file
  threads_data_nologin*.csv (termasuk file gabungan) dibaca saat start.
  Setelah scrape, workflow menjalankan merge_threads_shards.py untuk
  menggabung keempat shard -> threads_data_nologin.csv (dedup post_id).
  Tanpa env (""), perilaku lama: semua 32 kata kunci -> threads_data_nologin.csv.

Cara pakai (di komputer sendiri, sekali saja):
    pip install playwright
    playwright install chromium
    python threads_scraper_nologin.py

Hasil: threads_data_nologin_{a,b,c,d}.csv per shard + threads_data_nologin.csv
gabungan (UTF-8-SIG, 20 kolom).
"""

import asyncio
import csv
import glob
import json
import os
import random
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

from playwright.async_api import async_playwright

# ===================== PENGATURAN =====================
# Daftar kata kunci Threads (kabut asap karhutla paling akhir, sesuai spek).
KEYWORDS = [
    "karhutla",
    "kebakaran hutan",
    "karhutla sumatera",
    "karhutla kalimantan",
    "kebakaran hutan riau",
    "kebakaran hutan jambi",
    "kebakaran hutan kalimantan tengah",
    "kebakaran hutan kalimantan barat",
    "kebakaran lahan gambut",
    "titik api karhutla",
    "penanganan karhutla pemerintah",
    "bnpb karhutla",
    "manggala agni karhutla",
    "klhk kebakaran hutan",
    "tersangka pembakar hutan",
    "kebakaran hutan disengaja",
    "bakar hutan buka lahan",
    "pembakaran lahan sawit",
    "karhutla sengaja korporasi",
    "buka lahan sawit dibakar",
    "perusahaan bakar lahan",
    "asap karhutla",
    "kebakaran hutan sumatera",
    "kebakaran hutan kalimantan",
    "hotspot karhutla",
    "titik panas karhutla",
    "kabut asap riau",
    "kabut asap kalimantan",
    "karhutla 2025",
    "hutan terbakar",
    "kebakaran lahan",
    "kabut asap karhutla",
]

# --- Mode shard: 4 jalur paralel, masing-masing 8 kata kunci ---
_SHARD = os.environ.get("KEYWORD_SHARD", "").strip().lower()
if _SHARD == "a":
    KEYWORDS = KEYWORDS[:8]
    OUTPUT_CSV = "threads_data_nologin_a.csv"
elif _SHARD == "b":
    KEYWORDS = KEYWORDS[8:16]
    OUTPUT_CSV = "threads_data_nologin_b.csv"
elif _SHARD == "c":
    KEYWORDS = KEYWORDS[16:24]
    OUTPUT_CSV = "threads_data_nologin_c.csv"
elif _SHARD == "d":
    KEYWORDS = KEYWORDS[24:]
    OUTPUT_CSV = "threads_data_nologin_d.csv"
else:
    OUTPUT_CSV = "threads_data_nologin.csv"

TARGET_TOTAL = 5000        # batas atas; tanpa login realistisnya jauh di bawah ini
MAX_SCROLL = 40            # scroll maksimum per keyword (berhenti dini kalau mentok)
SORT_RECENT = True         # True = terbaru; kemungkinan diabaikan Threads untuk tamu
EXISTING_CSV = "dataset_karhutla_threads_nlp.csv"  # opsional, untuk lewati duplikat lama
HEADLESS = True            # True = browser tidak tampil di layar
MIN_RUNTIME_MINUTES = 60   # target total per run (pass ±45 mnt + balasan ±12 mnt + overhead)
MAIN_BUDGET_SECONDS = 45 * 60   # budget pass kata kunci per run
REPLY_BUDGET_SECONDS = 12 * 60  # budget fase balasan per run
PASS_SLEEP_SECONDS = 180   # jeda antar pass (detik)
# --- Pengaturan fase balasan (sejak 2 Nov 2026) ---
REPLY_MIN_COUNT = 10       # hanya postingan dengan reply_count >= ini yang diambil balasannya
MAX_REPLY_POSTS = 15       # maks postingan yang diambil balasannya per run (0 = nonaktif)
MAX_REPLIES_PER_POST = 30  # maks balasan per postingan (sesuai spek kolektor)
REPLY_MAX_SCROLL = 8       # scroll per halaman postingan saat ambil balasan
# ======================================================

# Skema 20 kolom, sama persis dengan dataset karhutla lainnya.
FIELDS = ["post_id", "parent_post_id", "post_type", "platform", "scraped_at",
          "created_at", "search_query", "author_username", "author_display_name",
          "is_verified", "raw_text", "clean_text_initial", "language", "hashtags",
          "mentions", "reply_count", "repost_count", "like_count", "view_count",
          "post_url"]

posts = {}           # id -> data BARU run ini (bukan dari run sebelumnya)
seen_ids = set()     # id yang sudah ada (dataset lama + hasil run sebelumnya)
saved = 0            # jumlah baris baru yang sudah ditulis ke OUTPUT_CSV
current_keyword = ""
reply_ctx = None     # {"parent_id","keyword"} saat fase balasan; None saat cari post utama
reply_counts = {}    # parent_id -> jumlah balasan yang sudah dikumpulkan run ini


def load_existing_ids():
    """Muat post_id yang sudah ada supaya tidak dikumpulkan ulang.

    Sumbernya: dataset lama + SEMUA file threads_data_nologin*.csv yang ada
    (file shard a/b/c/d dan file gabungan), jadi dedup bersifat global antar
    shard. Dengan begitu run berulang hanya menambah baris baru.
    """
    paths = [EXISTING_CSV] + sorted(glob.glob("threads_data_nologin*.csv"))
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    if row.get("post_id"):
                        seen_ids.add(row["post_id"])
        except Exception as e:
            print(f"[INFO] gagal baca {path} ({e}), lewati.")
    print(f"[INFO] {len(seen_ids)} post_id lama dimuat dari {len(paths)} file, "
          f"akan dilewati. (shard={_SHARD or 'semua'}, {len(KEYWORDS)} keyword)")


def build_row(obj, pid, cap, *, is_reply):
    """Bangun satu baris 20 kolom dari objek post JSON. None jika < 3 kata."""
    user = obj.get("user") or {}
    username = user.get("username", "")
    text = cap["text"].replace("\n", " ").strip()
    # Filter kualitas: lewati post di bawah 3 kata (sama seperti kolektor X).
    if len(text.split()) < 3:
        return None
    ts = obj.get("taken_at")
    created = (datetime.fromtimestamp(ts, timezone.utc)
               .strftime("%Y-%m-%dT%H:%M:%S") if ts else "")
    return {
        "post_id": pid,
        "parent_post_id": reply_ctx["parent_id"] if is_reply else "",
        "post_type": "reply_comment" if is_reply else "main_post",
        "platform": "threads",
        "scraped_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "created_at": created,
        "search_query": reply_ctx["keyword"] if is_reply else current_keyword,
        "author_username": username,
        "author_display_name": user.get("full_name", "") or username,
        "is_verified": "True" if user.get("is_verified") else "False",
        "raw_text": text,
        "clean_text_initial": text,
        "language": "id",
        "hashtags": json.dumps(re.findall(r"#\w+", text),
                               ensure_ascii=False),
        "mentions": json.dumps(["@" + m for m in
                                re.findall(r"@(\w+)", text)],
                               ensure_ascii=False),
        "reply_count": (obj.get("text_post_app_info") or {})
                       .get("direct_reply_count") or 0,
        "repost_count": obj.get("repost_count") or 0,
        "like_count": obj.get("like_count") or 0,
        "view_count": obj.get("view_count") or
                      obj.get("play_count") or 0,
        "post_url": f"https://www.threads.com/@{username}/post/{pid}",
    }


def extract(obj):
    """Cari objek post secara rekursif di dalam JSON apa pun.

    Mode ganda: saat reply_ctx None, objek yang ditemukan dicatat sebagai
    main_post; saat fase balasan (reply_ctx terisi), dicatat sebagai
    reply_comment dengan parent_post_id + cap per postingan.
    """
    if isinstance(obj, dict):
        cap = obj.get("caption")
        if isinstance(cap, dict) and cap.get("text") and obj.get("code"):
            # post_id dataset memakai shortcode (bukan pk numerik).
            pid = str(obj["code"])
            if pid not in posts and pid not in seen_ids:
                is_reply = reply_ctx is not None
                if is_reply:
                    parent_id = reply_ctx["parent_id"]
                    if reply_counts.get(parent_id, 0) < MAX_REPLIES_PER_POST:
                        row = build_row(obj, pid, cap, is_reply=True)
                        if row:
                            posts[pid] = row
                            reply_counts[parent_id] = reply_counts.get(parent_id, 0) + 1
                else:
                    row = build_row(obj, pid, cap, is_reply=False)
                    if row:
                        posts[pid] = row
        for v in obj.values():
            extract(v)
    elif isinstance(obj, list):
        for v in obj:
            extract(v)


async def on_response(resp):
    url = resp.url
    if "graphql" in url or "/api/" in url:
        try:
            extract(await resp.json())
        except Exception:
            pass


async def read_embedded_json(page):
    """Data awal halaman disisipkan di <script type="application/json">."""
    texts = await page.eval_on_selector_all(
        'script[type="application/json"]', "els => els.map(e => e.textContent)"
    )
    for t in texts:
        try:
            extract(json.loads(t))
        except Exception:
            pass


def save():
    """Tulis baris baru ke OUTPUT_CSV (append, bukan overwrite).

    Hanya baris yang belum ditulis yang ditambahkan, jadi pemanggilan
    save() tiap keyword tidak menduplikasi. Header + BOM ditulis hanya
    kalau file belum ada (append pakai utf-8 biasa supaya BOM tidak
    muncul di tengah file).
    """
    global saved
    rows = list(posts.values())[saved:]
    if not rows:
        return
    new_file = not os.path.exists(OUTPUT_CSV) or os.path.getsize(OUTPUT_CSV) == 0
    if new_file:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
    else:
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writerows(rows)
    saved += len(rows)


async def scrape_keyword(page, kw):
    """Satu kata kunci: buka halaman pencarian, scroll, simpan."""
    global current_keyword
    current_keyword = kw
    before = len(posts)
    url = f"https://www.threads.com/search?q={quote(kw)}&serp_type=default"
    if SORT_RECENT:
        url += "&filter=recent"
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)
    await read_embedded_json(page)

    stale = 0
    for _ in range(MAX_SCROLL):
        n = len(posts)
        await page.mouse.wheel(0, random.randint(2500, 4000))
        await page.wait_for_timeout(random.randint(1500, 3500))
        stale = stale + 1 if len(posts) == n else 0
        if stale >= 4 or len(posts) >= TARGET_TOTAL:
            break

    print(f"[{kw}] +{len(posts) - before} post | total {len(posts)}")
    save()  # simpan bertahap supaya aman kalau terputus
    await page.wait_for_timeout(random.randint(5000, 10000))  # jeda antar keyword


async def scrape_replies(page, deadline):
    """Fase balasan: buka halaman postingan kandidat dan kumpulkan balasannya.

    Kandidat = postingan utama BARU run ini dengan reply_count >=
    REPLY_MIN_COUNT, diurutkan dari reply_count terbesar, dibatasi
    MAX_REPLY_POSTS. Berhenti saat: kandidat habis, cap tercapai, budget
    waktu habis, atau login wall terdeteksi (3 postingan beruntun tanpa
    balasan baru). Fungsi ini tidak melempar error ke pemanggil — semua
    kegagalan per postingan ditangkap dan dicatat.
    """
    global reply_ctx
    candidates = [r for r in posts.values()
                  if r["post_type"] == "main_post"
                  and r["reply_count"] >= REPLY_MIN_COUNT]
    candidates.sort(key=lambda r: r["reply_count"], reverse=True)
    candidates = candidates[:MAX_REPLY_POSTS]
    if not candidates:
        print("[BALASAN] tidak ada kandidat (reply_count >= "
              f"{REPLY_MIN_COUNT}), lewati fase balasan.")
        return
    print(f"[BALASAN] {len(candidates)} kandidat | "
          f"budget {REPLY_BUDGET_SECONDS // 60} menit | "
          f"maks {MAX_REPLIES_PER_POST} balasan/postingan")
    empty_streak = 0
    done = 0
    for cand in candidates:
        if time.monotonic() >= deadline or len(posts) >= TARGET_TOTAL:
            print("[BALASAN] budget waktu / batas total tercapai, berhenti.")
            break
        pid = cand["post_id"]
        before = len(posts)
        try:
            reply_ctx = {"parent_id": pid, "keyword": cand["search_query"]}
            await page.goto(cand["post_url"], wait_until="domcontentloaded",
                            timeout=30000)
            await page.wait_for_timeout(3500)
            await read_embedded_json(page)
            stale = 0
            for _ in range(REPLY_MAX_SCROLL):
                if time.monotonic() >= deadline:
                    break
                n = len(posts)
                await page.mouse.wheel(0, random.randint(2000, 3500))
                await page.wait_for_timeout(random.randint(1200, 2500))
                stale = stale + 1 if len(posts) == n else 0
                if stale >= 2:
                    break
            got = len(posts) - before
            print(f"[BALASAN] {pid}: +{got} balasan "
                  f"(terkumpul {reply_counts.get(pid, 0)}/{MAX_REPLIES_PER_POST})")
            empty_streak = 0 if got > 0 else empty_streak + 1
            if empty_streak >= 3:
                print("[BALASAN] 3 postingan beruntun tanpa balasan baru — "
                      "kemungkinan login wall / balasan tak terlihat tamu. "
                      "Hentikan fase balasan.")
                break
        except Exception as e:
            print(f"[BALASAN] {pid}: gagal ({type(e).__name__}), lanjut.")
            empty_streak += 1
            if empty_streak >= 3:
                print("[BALASAN] 3 kegagalan beruntun, hentikan fase balasan.")
                break
        finally:
            reply_ctx = None
        done += 1
        save()  # simpan bertahap
    print(f"[BALASAN] selesai: {done} postingan diproses.")


async def main():
    load_existing_ids()
    start = time.monotonic()
    main_deadline = start + MAIN_BUDGET_SECONDS
    hard_deadline = start + 80 * 60  # total run jangan lewati 80 menit (job timeout 90)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            locale="id-ID",
            viewport={"width": 1280, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/141.0.0.0 Safari/537.36"),
        )
        page = await context.new_page()
        page.on("response", on_response)

        pass_no = 0
        while time.monotonic() < main_deadline:
            pass_no += 1
            print(f"[PASS {pass_no}] mulai")
            for kw in KEYWORDS:
                if len(posts) >= TARGET_TOTAL or time.monotonic() >= main_deadline:
                    break
                await scrape_keyword(page, kw)
            print(f"[PASS {pass_no}] selesai | total {len(posts)}")
            if len(posts) >= TARGET_TOTAL:
                break
            remaining = main_deadline - time.monotonic()
            if remaining <= 0:
                break
            sleep_s = min(PASS_SLEEP_SECONDS, remaining)
            print(f"[TIDUR-ANTAR-PASS] {sleep_s:.0f} detik")
            await asyncio.sleep(sleep_s)

        # Fase balasan — terjaga penuh: error apa pun tidak boleh menggagalkan run.
        if MAX_REPLY_POSTS > 0 and len(posts) < TARGET_TOTAL:
            try:
                reply_deadline = min(time.monotonic() + REPLY_BUDGET_SECONDS,
                                     hard_deadline)
                if reply_deadline > time.monotonic() + 60:
                    await scrape_replies(page, reply_deadline)
                else:
                    print("[BALASAN] sisa waktu < 1 menit, lewati fase balasan.")
            except Exception as e:
                print(f"[BALASAN] fase balasan error tak terduga "
                      f"({type(e).__name__}: {e}), lanjut simpan.")

        await browser.close()
    save()
    n_reply = sum(1 for r in posts.values() if r["post_type"] == "reply_comment")
    print(f"Selesai: {len(posts)} baris BARU "
          f"({len(posts) - n_reply} postingan utama + {n_reply} balasan) "
          f"ditambahkan ke {OUTPUT_CSV} (file bersifat kumulatif).")


if __name__ == "__main__":
    asyncio.run(main())
