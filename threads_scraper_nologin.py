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
- Hanya postingan utama (main_post). Balasan TIDAK diambil script ini,
  karena membuka tiap postingan tanpa login jauh lebih sering kena login wall.
- Kalau file dataset_karhutla_threads_nlp.csv ada di folder yang sama,
  post_id yang sudah ada di sana dilewati (tidak duplikat dengan 213 baris lama).
- File hasil bersifat kumulatif: setiap run MENAMBAH baris baru ke
  threads_data_nologin.csv (bukan menimpa), dan post_id dari run sebelumnya
  ikut dilewati. Jadi workflow GitHub Actions bisa commit-back file yang sama
  berulang kali tanpa kehilangan hasil run sebelumnya.
- Tidak butuh secret apa pun (tanpa login, tanpa API key).

Cara pakai (di komputer sendiri, sekali saja):
    pip install playwright
    playwright install chromium
    python threads_scraper_nologin.py

Hasil: threads_data_nologin.csv (UTF-8-SIG, 20 kolom).
"""

import asyncio
import csv
import json
import os
import random
import re
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
    "kabut asap karhutla",
]
TARGET_TOTAL = 5000        # batas atas; tanpa login realistisnya jauh di bawah ini
MAX_SCROLL = 40            # scroll maksimum per keyword (berhenti dini kalau mentok)
SORT_RECENT = True         # True = terbaru; kemungkinan diabaikan Threads untuk tamu
OUTPUT_CSV = "threads_data_nologin.csv"
EXISTING_CSV = "dataset_karhutla_threads_nlp.csv"  # opsional, untuk lewati duplikat lama
HEADLESS = True            # True = browser tidak tampil di layar
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


def load_existing_ids():
    """Muat post_id yang sudah ada supaya tidak dikumpulkan ulang.

    Sumbernya dua file (kalau ada): dataset lama dan file hasil run
    sebelumnya. Dengan begitu run berulang hanya menambah baris baru.
    """
    for path in (EXISTING_CSV, OUTPUT_CSV):
        if not os.path.exists(path):
            continue
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    if row.get("post_id"):
                        seen_ids.add(row["post_id"])
        except Exception as e:
            print(f"[INFO] gagal baca {path} ({e}), lewati.")
    print(f"[INFO] {len(seen_ids)} post_id lama dimuat, akan dilewati.")


def extract(obj):
    """Cari objek post secara rekursif di dalam JSON apa pun."""
    if isinstance(obj, dict):
        cap = obj.get("caption")
        if isinstance(cap, dict) and cap.get("text") and obj.get("code"):
            # post_id dataset memakai shortcode (bukan pk numerik).
            pid = str(obj["code"])
            if pid not in posts and pid not in seen_ids:
                user = obj.get("user") or {}
                username = user.get("username", "")
                text = cap["text"].replace("\n", " ").strip()
                # Filter kualitas: lewati post di bawah 3 kata (sama seperti kolektor X).
                if len(text.split()) >= 3:
                    ts = obj.get("taken_at")
                    created = (datetime.fromtimestamp(ts, timezone.utc)
                               .strftime("%Y-%m-%dT%H:%M:%S") if ts else "")
                    posts[pid] = {
                        "post_id": pid,
                        "parent_post_id": "",
                        "post_type": "main_post",
                        "platform": "threads",
                        "scraped_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        "created_at": created,
                        "search_query": current_keyword,
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


async def main():
    global current_keyword
    load_existing_ids()
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

        for kw in KEYWORDS:
            if len(posts) >= TARGET_TOTAL:
                break
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

        await browser.close()
    save()
    print(f"Selesai: {len(posts)} post BARU ditambahkan ke {OUTPUT_CSV} "
          f"(file bersifat kumulatif).")


if __name__ == "__main__":
    asyncio.run(main())
