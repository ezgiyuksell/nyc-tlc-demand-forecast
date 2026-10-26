"""
Hedef tablo: tam zone x saat izgarasi.

Ne yapar:
  1. data/agg/*.parquet okur (ingest.py ciktilari).
  2. Veride en az bir kez gorulen her zone x donemdeki her saat icin satir uretir.
  3. Yolculuk olmayan zone-saatleri 0 ile doldurur.
  4. Yaz saati (DST) gecislerini isaretler:
       - is_dst_gap : ileri alinan saat (Mart, 02:00). Bu saat yerel saatte hic yasanmaz.
                      trips = NULL yapilir; 0 degil, cunku "talep yok" degil "saat yok".
       - is_dst_dup : geri alinan saat (Kasim, 01:00). Iki gercek saat tek satira duser,
                      sayi ~2 kat cikar. Satir kalir, bayrakla isaretlenir.
  5. Modelin kullanacagi duzeltilmis kolonu uretir:
       - trips       : ham sayi (seffaflik icin; dokunulmaz)
       - trips_model : feature ve hedef icin kullanilacak kolon
                       bosluk saati -> NULL, cift saat -> trips / 2, diger saatler -> trips
     Neden: cift saatteki ~2 kat deger birakilirsa lag_24, lag_168 ve rolling ortalamalar
     uzerinden sonraki gun ve haftalara yayilir ve o saatlerde tahmini sisirir.
  6. data/processed/target.parquet yazar ve kontrol raporu basar.

Kullanim (proje kokunde):
    python src\\build_target.py
"""
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
AGG_GLOB = (ROOT / "data" / "agg" / "*.parquet").as_posix()
OUT_DIR = ROOT / "data" / "processed"
OUT = OUT_DIR / "target.parquet"
TZ = "America/New_York"


def dst_hours(hours: pd.DatetimeIndex) -> tuple[list, list]:
    """Naive yerel saatler icinden DST bosluk ve cift saatleri bulur."""
    gap = hours[hours.tz_localize(TZ, nonexistent="NaT", ambiguous="NaT").isna()
                & hours.tz_localize(TZ, nonexistent="shift_forward", ambiguous="NaT").notna()]
    dup = hours[hours.tz_localize(TZ, nonexistent="shift_forward", ambiguous="NaT").isna()]
    return list(gap), list(dup)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE agg AS SELECT zone, hour, trips FROM read_parquet('{AGG_GLOB}')").fetchall()

    # Ayni zone-saat iki kez var mi? (ingest bir ayi iki kez yazdiysa burada yakalanir)
    dups = con.execute("SELECT count(*) FROM (SELECT zone, hour FROM agg GROUP BY 1,2 HAVING count(*) > 1)").fetchall()[0][0]
    if dups:
        raise SystemExit(f"HATA: agg icinde {dups} tekrar eden zone-saat var. data/agg dosyalarini kontrol et.")

    hmin, hmax = con.execute("SELECT min(hour), max(hour) FROM agg").fetchall()[0]
    hours = pd.date_range(hmin, hmax, freq="h")
    gap, dup = dst_hours(hours)
    con.register("dst_gap", pd.DataFrame({"hour": pd.to_datetime(gap)}) if gap else pd.DataFrame({"hour": pd.Series(dtype="datetime64[us]")}))
    con.register("dst_dup", pd.DataFrame({"hour": pd.to_datetime(dup)}) if dup else pd.DataFrame({"hour": pd.Series(dtype="datetime64[us]")}))

    con.execute(f"""
        CREATE TABLE target AS
        WITH zones AS (SELECT DISTINCT zone FROM agg),
             hrs   AS (SELECT unnest(range(TIMESTAMP '{hmin}', TIMESTAMP '{hmax}' + INTERVAL 1 HOUR, INTERVAL 1 HOUR)) AS hour),
             grid  AS (SELECT z.zone, h.hour FROM zones z CROSS JOIN hrs h)
        SELECT g.zone,
               g.hour,
               CASE WHEN dg.hour IS NOT NULL THEN NULL
                    ELSE coalesce(a.trips, 0) END        AS trips,
               CASE WHEN dg.hour IS NOT NULL THEN NULL
                    WHEN dd.hour IS NOT NULL THEN coalesce(a.trips, 0) / 2.0
                    ELSE coalesce(a.trips, 0) END::DOUBLE AS trips_model,
               (dg.hour IS NOT NULL)                       AS is_dst_gap,
               (dd.hour IS NOT NULL)                       AS is_dst_dup
        FROM grid g
        LEFT JOIN agg a      ON a.zone = g.zone AND a.hour = g.hour
        LEFT JOIN dst_gap dg ON dg.hour = g.hour
        LEFT JOIN dst_dup dd ON dd.hour = g.hour
        ORDER BY g.zone, g.hour
    """).fetchall()
    con.execute(f"COPY target TO '{OUT.as_posix()}' (FORMAT parquet)").fetchall()

    # ---------------- Kontrol raporu ----------------
    n_zones = con.execute("SELECT count(DISTINCT zone) FROM agg").fetchall()[0][0]
    n_hours = len(hours)
    n_rows = con.execute("SELECT count(*) FROM target").fetchall()[0][0]
    agg_rows, agg_sum = con.execute("SELECT count(*), sum(trips) FROM agg").fetchall()[0]
    gap_trips = con.execute("SELECT coalesce(sum(a.trips),0) FROM agg a JOIN dst_gap g USING (hour)").fetchall()[0][0]
    tgt_sum = con.execute("SELECT sum(trips) FROM target").fetchall()[0][0]
    zero_share = con.execute("SELECT avg((trips = 0)::INT) FROM target WHERE NOT is_dst_gap").fetchall()[0][0]

    print(f"Donem          : {hmin} -> {hmax}")
    print(f"Zone sayisi    : {n_zones}")
    print(f"Saat sayisi    : {n_hours:,}")
    print(f"Hedef satir    : {n_rows:,}  (beklenen {n_zones * n_hours:,})  {'OK' if n_rows == n_zones * n_hours else 'HATA'}")
    filled = con.execute("SELECT count(*) FROM target WHERE trips = 0").fetchall()[0][0]
    print(f"agg satir      : {agg_rows:,}")
    print(f"0 ile doldurulan: {filled:,} satir")
    print(f"Sifir orani    : {zero_share:.1%}")
    print(f"Toplam trips   : agg={agg_sum:,}  target={tgt_sum:,}  (fark = DST bosluk saatine dusen {gap_trips:,})")

    print("\nDST kontrolu (tum zone'lar toplami; bir onceki ve sonraki saatle karsilastir):")
    for label, hs in (("ileri (bosluk)", gap), ("geri (cift)", dup)):
        for h in hs:
            row = con.execute(f"""
                SELECT sum(trips) FILTER (WHERE hour = TIMESTAMP '{h}' - INTERVAL 1 HOUR),
                       sum(trips) FILTER (WHERE hour = TIMESTAMP '{h}'),
                       sum(trips) FILTER (WHERE hour = TIMESTAMP '{h}' + INTERVAL 1 HOUR),
                       sum(trips_model) FILTER (WHERE hour = TIMESTAMP '{h}')
                FROM target
            """).fetchall()[0]
            print(f"  {label:15s} {h}   once={row[0]}  o saat={row[1]}  sonra={row[2]}  "
                  f"-> trips_model={row[3]}")

    print("\nEn dusuk hacimli 10 zone (EDA'da tutulacak mi karar verilecek):")
    print(con.execute("""
        SELECT zone, sum(trips) AS toplam, round(avg((trips = 0)::INT), 3) AS sifir_orani
        FROM target WHERE NOT is_dst_gap
        GROUP BY zone ORDER BY toplam LIMIT 10
    """).df().to_string(index=False))
    print(f"\nYazildi: {OUT}")
    con.close()


if __name__ == "__main__":
    main()
