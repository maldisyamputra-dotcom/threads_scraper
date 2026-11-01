"""Gabung file shard Threads -> threads_data_nologin.csv (dedup by post_id).

Dijalankan tiap run workflow setelah scrape, sebelum commit-back.
Idempoten: isi gabungan hanya bergantung pada isi file shard yang ada,
bukan urutan run. File shard (threads_data_nologin_a.csv /
threads_data_nologin_b.csv) bersifat kumulatif dan TIDAK PERNAH ditulis
ulang oleh script ini; yang ditulis ulang hanya file gabungan.
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
    shard_files = [p for p in sorted(glob.glob("threads_data_nologin_[ab].csv"))
                   if os.path.exists(p)]
    seen = set()
    rows = []
    for path in shard_files:
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
    print(f"[MERGE] {len(rows)} baris unik dari {len(shard_files)} file shard "
          f"-> {OUT}")


if __name__ == "__main__":
    main()
