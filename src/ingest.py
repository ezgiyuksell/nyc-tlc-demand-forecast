"""
Aylik HVFHV ingest: indir -> zone x saat agregasyonu -> ham dosyayi sil.

Kullanim (C:\\zone_time_project klasorunde, Anaconda Prompt'ta):
    python src\\ingest.py --start 2024-02 --end 2024-02          # tek ay
    python src\\ingest.py --start 2024-01 --end 2026-08          # tum donem
    python src\\ingest.py --start 2024-02 --end 2024-02 --force  # varsa yeniden isle

Idempotent: her ay kendi dosyasina yazilir (data/agg/YYYY-MM.parquet).
Ayni ay ikinci kez calisirsa ya atlanir ya da (--force ile) uzerine yazilir; asla iki kez eklenmez.
"""
import argparse
import os
import urllib.request
from datetime import date

import duckdb

ROOT = r"C:\zone_time_project"
RAW_DIR = os.path.join(ROOT, "data", "raw")
AGG_DIR = os.path.join(ROOT, "data", "agg")
# Ctrl+J ile indirdigin dosyanin kaynak adresiyle karsilastir; farkliysa sadece bu satiri degistir.
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{ym}.parquet"
EXCLUDED_ZONES = (264, 265)  # Unknown / Outside of NYC


def month_range(start: str, end: str):
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def next_month_first_day(ym: str) -> str:
    y, m = map(int, ym.split("-"))
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return date(y, m, 1).isoformat()


def download(ym: str) -> str:
    path = os.path.join(RAW_DIR, f"fhvhv_tripdata_{ym}.parquet")
    if os.path.exists(path):
        print(f"  ham dosya zaten var, indirme atlandi: {path}")
        return path
    tmp = path + ".part"  # yarida kalan indirme gercek dosya sanilmasin
    print(f"  indiriliyor: {URL.format(ym=ym)}")
    urllib.request.urlretrieve(URL.format(ym=ym), tmp)
    os.replace(tmp, path)
    return path


def aggregate(raw_path: str, ym: str) -> tuple[int, int, int]:
    out = os.path.join(AGG_DIR, f"{ym}.parquet")
    start = f"{ym}-01"
    end = next_month_first_day(ym)
    con = duckdb.connect()
    try:
        raw_n = con.execute(f"SELECT count(*) FROM '{raw_path}'").fetchone()[0]
        con.execute(f"""
            COPY (
                SELECT PULocationID AS zone,
                       date_trunc('hour', request_datetime) AS hour,
                       count(*) AS trips
                FROM '{raw_path}'
                WHERE request_datetime >= '{start}'
                  AND request_datetime <  '{end}'
                  AND PULocationID NOT IN {EXCLUDED_ZONES}
                GROUP BY zone, hour
            ) TO '{out}' (FORMAT parquet)
        """)
        rows, trips = con.execute(f"SELECT count(*), sum(trips) FROM '{out}'").fetchone()
    finally:
        con.close()  # Windows'ta dosya kilidi kalmasin, yoksa silme hata verir
    return raw_n, rows, trips


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM")
    p.add_argument("--end", required=True, help="YYYY-MM")
    p.add_argument("--force", action="store_true", help="agregat varsa yeniden olustur")
    p.add_argument("--keep-raw", action="store_true", help="ham dosyayi silme")
    a = p.parse_args()

    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(AGG_DIR, exist_ok=True)

    for ym in month_range(a.start, a.end):
        out = os.path.join(AGG_DIR, f"{ym}.parquet")
        print(f"[{ym}]")
        if os.path.exists(out) and not a.force:
            print("  agregat zaten var, atlandi")
            continue
        raw = download(ym)
        raw_n, rows, trips = aggregate(raw, ym)
        dropped = raw_n - trips
        print(f"  ham={raw_n:,}  agregat_satir={rows:,}  trips={trips:,}  "
              f"dusen={dropped:,} ({dropped / raw_n:.3%})")
        if dropped / raw_n > 0.01:
            print("  UYARI: %1'den fazla satir filtrelendi, kontrol et")
        if not a.keep_raw:
            os.remove(raw)
            print("  ham dosya silindi")


if __name__ == "__main__":
    main()
