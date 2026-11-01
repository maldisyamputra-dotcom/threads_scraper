"""Gabung file shard Threads -> threads_data_nologin.csv (dedup by post_id).

Dijalankan tiap run workflow setelah scrape, sebelum commit-back.

MONOTON (tidak pernah mengurangi baris): file gabungan yang sudah ada
dibaca DULU, baru kemudian shard a/b digabung dengan dedup post_id.
Ini krusial untuk bootstrap — pada run 2-shard pertama, file shard belum
ada di repo (belum pernah di-commit) sehingga hanya berisi baris baru dari
run itu saja; kalau file gabungan lama tidak ikut dibaca, ribuan baris
lama akan hilang tertimpa oleh merge. (Insiden yang dicegah: run #62,
1 Nov 2026 — 1.464 baris nyaris ter-overwrite oleh merge.)

Idempoten: isi gabungan = union semua sumber yang ada, hanya bergantung
pada isi file, bukan urutan run. File shard bersifat kumulatif (di-append
tiap run oleh scraper) dan TIDAK PERNAH ditulis ulang oleh script ini;
yang ditulis ulang hanya file gabungan.
"""

import csv
import glob
import os

FIELDS = ["post_id", "parent_post_id", "post_type", "platform", "scraped_at",
          "created_at", "search_query", "author_username", "author_display_name",
          "is_verified", "raw_text", "clean_text_initial", "language", "hashtags",
          "mentions", "reply_count", "repost_count", "like_count", "view_count",
          "post_url"]

OUT = "threads_data_nologin.csv"


def main():
    # Urutan baca: file gabungan lama DULU (pertahankan semua baris lama),
    # lalu shard a/b (tambah baris baru). Dedup by post_id -> hasil monoton.
    paths = []
    if os.path.exists(OUT):
        paths.append(OUT)
    paths += [p for p in sorted(glob.glob("threads_data_nologin_[ab].csv"))
              if os.path.exists(p)]

    seen = set()
    rows = []
    for path in paths:
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    pid = row.get("post_id")
                    if pid and pid not in seen:
                        seen.add(pid)
                        rows.append(row)
        except Exception as e:
            print(f"[MERGE] gagal baca {path} ({e}), lewati.")

    with open(OUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})

    print(f"[MERGE] {len(rows)} baris unik dari {len(paths)} file "
          f"-> {OUT}")


if __name__ == "__main__":
    main()
