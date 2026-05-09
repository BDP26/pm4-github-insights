# Konsolidierte Findings: LaTeX Report vs. Implementierung

**Datum:** 2026-05-09
**Quelle:** `docs/CONSISTENZPRUEFUNG.md`, `docs/REPORT_TODOS.md`
**Status:** Alle Findings konsolidiert

---

## Übersicht

| Schweregrad | Anzahl |
|-------------|--------|
| 🔴 Kritisch | 2 |
| 🟡 Hoch | 10 |
| 🟢 Niedrig | 8 |

---

## Kapitel 2 — Related Work

### B3 · Related Work vollständig leer

**Schweregrad:** 🟡 Hoch
**Aufwand:** Hoch

**Was:** Section 2 enthält nur eine TODO-Box, kein Inhalt.

**Warum:** ca. 300–400 Wörter zu verwandten Systemen (GH Archive, GHTorrent, Lambda/Kappa-Architektur, TimescaleDB-Vergleiche) fehlen komplett.

**Lösung:** Related Work Section mit Inhalt füllen über:
- GH Archive / GHTorrent als verwandte Projekte
- Lambda/Kappa Architektur Vergleiche
- TimescaleDB vs. andere Time-Series DBs

---

## Kapitel 3 — Architecture

### A1 · Docker-Service-Tabelle unvollständig

**Schweregrad:** 🟡 Mittel
**Aufwand:** Minimal
**Report-Zeile:** Section 3, Tabelle "Expanded System Component Overview"

**Was:** Die Tabelle listet 8 Services. `kafka-ui` (Port 8080) und `kafka-init` fehlen.

**Warum:** Diese Services existieren in `docker-compose.yml` und müssen dokumentiert werden.

**Lösung:** In der Tabelle Zeilen für `kafka-ui` und `kafka-init` ergänzen.

---

### A2 · Consumer als einzelner Service dargestellt

**Schweregrad:** 🟡 Mittel
**Aufwand:** Minimal
**Report-Zeile:** Section 3.3

**Was:** Text und Tabelle beschreiben "den Consumer" als eine Einheit.

**Warum:** Tatsächlich laufen drei unabhängige Instanzen: `consumer-0`, `consumer-1`, `consumer-2` (je einer pro Kafka-Partition).

**Lösung:** Service in der Tabelle als `consumer-{0,1,2}` kennzeichnen und im Text klarstellen, dass es sich um drei parallel laufende Instanzen handelt.

---

### A3 · Kafka-Topic `geocoder.requests` nicht erwähnt

**Schweregrad:** 🟢 Niedrig
**Aufwand:** Minimal
**Report-Zeile:** Section 3.2 (Message Broker / Kafka)

**Was:** Der Report nennt nur drei Topics (`github.events.raw`, `github.events.status`, `github.ratelimit`).

**Warum:** Das vierte Topic `geocoder.requests` (1 Partition, für Geocoder-Request-Logs) fehlt.

**Lösung:** Topic `geocoder.requests` mit seiner Funktion ergänzen.

---

### A4 · Geocoding-Service in Kap. 3.6 korrekt, in Kap. 4.2 falsch

**Schweregrad:** 🔴 Kritisch
**Aufwand:** Minimal
**Report-Zeilen:** Kapitel 3.6 (korrekt) vs. Kapitel 4.2 (fehlerhaft)

**Was:** Kapitel 3.6 beschreibt Photon korrekt. Kapitel 4.2 beschreibt fälschlich Nominatim.

**Warum:** Die Implementierung (`geocoder/geocoder.py`) nutzt **Photon** (`PHOTON_URL = "http://photon:2322/api"`), nicht Nominatim.

**Lösung:** In Kapitel 4.2 "Nominatim" durch "Photon" ersetzen.

---

### A5 · Rate-Limiter (1 req/s) für Geocoding nicht implementiert

**Schweregrad:** 🟡 Hoch
**Aufwand:** Minimal
**Report-Zeile:** Kapitel 4.2

**Was:** Der Report behauptet: *"A rate limiter enforces the required 1 req/s ceiling."*

**Warum:** In `geocoder/geocoder.py` gibt es keinen Rate-Limiter. Bei verfügbaren Requests wird sofort angefragt – ohne expliziten Delay. Nur `time.sleep(30)` wenn keine ausstehenden Anfragen.

**Lösung:** Beschreibung korrigieren: kein 1 req/s Limit; Verarbeitungslogik akkurat beschreiben.

---

### A6 · LRU-Cache (1 Stunde) für Geocoding nicht implementiert

**Schweregrad:** 🟡 Hoch
**Aufwand:** Minimal
**Report-Zeile:** Kapitel 4.2

**Was:** Der Report behauptet: *"a one-hour LRU cache avoids re-geocoding identical strings."*

**Warum:** Kein LRU-Cache in `geocoder.py`. Deduplication erfolgt über DB-Feld `geo_claimed_at`.

**Lösung:** LRU-Cache-Aussage entfernen; stattdessen das DB-basierte `geo_claimed_at`-Claim-Verfahren beschreiben.

---

### A9 · Continuous Aggregate `actor_stats_1h` existiert nicht

**Schweregrad:** 🟡 Hoch
**Aufwand:** Mittel
**Report-Zeilen:** Kapitel 4.3, Tabelle "TimescaleDB continuous aggregates"

**Was:** Tabelle listet drei Aggregates: `event_stats_5m`, `country_ids_5m`, `actor_stats_1h`.

**Warum:** `actor_stats_1h` ist weder in `db/init.sql` noch in `db/migrations/` definiert.

**Lösung:**
- **Option A:** Aus Tabelle entfernen (wenn nicht benötigt)
- **Option B:** In `db/init.sql` implementieren:
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS actor_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    actor_username,
    count(*) AS event_count
FROM events
WHERE actor_username IS NOT NULL
GROUP BY bucket, actor_username;
```

---

### A12 · Grafana vs. Next.js Formulierung unklar

**Schweregrad:** 🟢 Niedrig
**Aufwand:** Minimal
**Report-Zeile:** Section 3.5 (Visualization)

**Was:** Abschnitt 3.5 bezeichnet Grafana als "central visualization hub" und "primary platform".

**Warum:** Kapitel 4.5 beschreibt ein vollwertiges Next.js-Dashboard. Beide sind gleichwertige Visualisierungsschichten mit verschiedenen Zielgruppen.

**Lösung:** Formulierung anpassen: Grafana für System-Monitoring/Ops, Next.js-Dashboard für Endnutzer-Analyse.

---

### B4 · Datenbankschema-Diagramm fehlt

**Schweregrad:** 🟢 Niedrig
**Aufwand:** Klein
**Report-Zeile:** Section 3.4, letztes Item

**Was:** `\TODO{Add db scheme graph}` ist noch sichtbar.

**Warum:** DB-Schema-Diagramm wurde nicht eingefügt. Datei `db/schema.dbml` existiert bereits.

**Lösung:** Diagramm aus `db/schema.dbml` erstellen und einfügen.

---

### B5 · Architekturdiagramm fehlt

**Schweregrad:** 🟡 Mittel
**Aufwand:** Mittel
**Report-Zeile:** Figure 1, Section 3

**Was:** `\TODO{Insert architecture diagram here.}` – Figure ist ein Placeholder.

**Warum:** Echtes Architekturdiagramm wurde nicht eingefügt.

**Lösung:** Diagramm erstellen und als Figure 1 einfügen.

---

## Kapitel 4 — Implementation

### A7 · In-Memory-Dictionary-Cache für User-Profile nicht vorhanden

**Schweregrad:** 🟡 Hoch
**Aufwand:** Minimal
**Report-Zeile:** Kapitel 4.2, erster Unterpunkt

**Was:** Der Report schreibt: *"Results are cached in a dictionary for the process lifetime to avoid redundant API calls."*

**Warum:** `Enricher` in `consumer/enricher.py` verwendet keinen In-Memory-Dictionary. Stattdessen wird per DB-Query geprüft (`SELECT 1 FROM users WHERE username = %s AND fetched_at IS NOT NULL`).

**Lösung:** Dictionary-Cache-Aussage entfernen; DB-basierte `fetched_at`-Prüfung als Deduplication-Mechanismus beschreiben.

---

### A8 · Beschreibung "two enrichment passes" vereinfacht/ungenau

**Schweregrad:** 🟢 Niedrig
**Aufwand:** Klein
**Report-Zeile:** Kapitel 4.2 Einleitung

**Was:** Der Report beschreibt zwei sequentielle Enrichment-Passes.

**Warum:** Tatsächlich hat der `Enricher` zwei **parallele** Batch-Queues (User+Org / Repo) mit je eigenem Token-Pool.

**Lösung:** Enrichment-Architektur als zwei parallele Queues beschreiben.

---

### A10 · API-Endpoint-Tabelle unvollständig

**Schweregrad:** 🟡 Mittel
**Aufwand:** Klein
**Report-Zeile:** Section 4.4, Tabelle "FastAPI endpoint reference"

**Was:** Tabelle listet 6 Endpoints.

**Warum:** Die tatsächliche API hat mehr Endpoints über zwei Router:
- `GET /api/hidden-gems/*` (Filter, Rankings, Scoring)
- `GET /api/overview/heatmap`
- `GET /api/overview/globe-heatmap`
- `GET /api/activity/leaderboard`

**Lösung:** Tabelle erweitern oder Hinweis auf vollständige Referenz unter `/docs` (Swagger UI).

---

### A11 · SSE Keep-alive Kommentar falsch beschriftet

**Schweregrad:** 🟢 Niedrig
**Aufwand:** Minimal
**Report-Zeile:** Section 4.4, SSE-Endpoint-Beschreibung

**Was:** Der Report schreibt: *"Keep-alive comments (`: ping`) are sent between data frames."*

**Warum:** Code (`api/main.py`) sendet tatsächlich `: heartbeat\n\n` (initial) und `: keep-alive\n\n` (zwischen Events).

**Lösung:** „`: ping`" durch „`: keep-alive`" ersetzen.

---

### A13 · Kernel-Version in Kap. 5 wirkt wie Platzhalter

**Schweregrad:** 🟢 Niedrig
**Aufwand:** Minimal
**Report-Zeile:** Section 5.1.1, Tabelle "System Specifications"

**Was:** Kernel ist `Linux 6.17.0-20-generic`.

**Warum:** Ubuntu 24.04 LTS nutzt typischerweise Kernel 6.8.x (GA) oder 6.11.x (HWE). Version 6.17 existierte nicht als stabiler Ubuntu-Kernel.

**Lösung:** Tatsächliche Kernel-Version vom Server eintragen (`uname -r`).

---

## Kapitel 5 — Evaluation

### B6 · Alle Messwerte in Kapitel 5.2–5.4 sind TODO

**Schweregrad:** 🟡 Hoch
**Aufwand:** Hoch
**Report-Zeilen:** Sections 5.2, 5.3, 5.4

**Was:** Throughput, Latenz, Data-Coverage-Metriken, Storage-Effizienz – alles mit `\TODO{N/X}` belegt.

**Warum:** Ein echter System-Run (24–48 Stunden) wurde nicht durchgeführt/dokumentiert.

**Lösung:**
1. System mindestens 24-48 Stunden laufen lassen
2. Metriken aus Docker Logs, Grafana Dashboards und DB-Queries sammeln
3. Werte in Report einsetzen

**Nützliche Queries:**
```sql
-- Events gesamt
SELECT COUNT(*) FROM events;

-- Durchschnittliche Ingestionsrate (Events pro Minute)
SELECT COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(time) - MIN(time))) / 60, 1) FROM events;

-- Compression stats
SELECT * FROM chunk_compression_stats('events');
```

---

### B6 (Additional) · TODO-Platzhalter in Kapitel 5 (Evaluation)

**Schweregrad:** 🟡 Hoch
**Aufwand:** Hoch
**Report-Zeilen:** 468-503

**Offene TODOs:**
| Zeile | TODO-Inhalt |
|-------|-------------|
| 468 | Events ingested (total) |
| 469 | Average ingestion rate |
| 470 | Peak ingestion rate |
| 471 | Median end-to-end latency |
| 472 | Kafka consumer lag (steady) |
| 488 | Unique repositories |
| 489 | Unique contributors |
| 490 | Countries represented |
| 491 | Geocoding success rate |
| 492 | Event types observed |
| 493 | Most active programming language |
| 500-502 | Storage efficiency / chunk compression stats |

---

## Kapitel 8 — Conclusion

### B7 · Conclusion teilweise TODO

**Schweregrad:** 🟢 Niedrig
**Aufwand:** Variabel
**Report-Zeile:** Section 8

**Was:** Erster Paragraph ist als TODO markiert (`\TODO{Write 1–2 paragraphs…}`).

**Warum:** Einleitungsparagraph für Conclusion fehlt.

**Lösung:** 1–2 einleitende Paragraphen zur Conclusion hinzufügen.

---

## Verschiedene TODO-Platzhalter

### B1 · Abstract TODO-Marker vorhanden

**Schweregrad:** 🟢 Niedrig
**Report-Zeile:** Abstract, letzte Zeile
**Was:** `\TODO{Refine after report completion}` noch sichtbar.

---

### B2 · Empfangsdatum (Revision) fehlt

**Schweregrad:** 🟢 Niedrig
**Report-Zeile:** Header nach `\maketitle`
**Was:** `\received[revised]{\TODO{Fill in revision date}}`

---

### B8 · Acknowledgments: Supervisor-Namen fehlen

**Schweregrad:** 🟢 Niedrig
**Report-Zeile:** Acknowledgments-Block
**Was:** `\TODO{Add names of supervisors…}`

---

### B9 · Anhang: Timeline-Tabelle ohne Verantwortliche

**Schweregrad:** 🟢 Niedrig
**Report-Zeile:** Appendix, Tabelle "Actual project timeline"
**Was:** Alle "Owner"-Spalten enthalten `\TODO{name(s)}`

---

## Abhängigkeiten

```
┌─────────────────────────────────────────────────────────────┐
│  Fix A4 (Nominatim → Photon in Kap. 4)                      │
│  └── Behebt auch: A5 (Rate-Limiter), A6 (LRU-Cache)        │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  Fix A9 (actor_stats_1h)                                    │
│  └── Entscheidung: Entfernen ODER Implementieren            │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  Fix B6 (Kapitel 5 Evaluation)                              │
│  └── Erfordert: System-Run 24-48h                           │
└─────────────────────────────────────────────────────────────┘
```

---

## Checkliste

- [ ] **A1:** Service-Tabelle um kafka-ui/kafka-init ergänzen
- [ ] **A2:** Consumer als 3 Instanzen darstellen
- [ ] **A3:** geocoder.requests Topic erwähnen
- [ ] **A4:** Nominatim in Kap. 4.2 durch Photon ersetzen (→ behebt A5, A6)
- [ ] **A7:** Dictionary-Cache-Aussage korrigieren
- [ ] **A8:** Enrichment-Architektur als zwei parallele Queues beschreiben
- [ ] **A9:** actor_stats_1h - Entscheidung treffen (entfernen oder implementieren)
- [ ] **A10:** Endpoint-Tabelle vervollständigen
- [ ] **A11:** SSE `: ping` durch `: keep-alive` ersetzen
- [ ] **A12:** Grafana vs. Next.js Formulierung schärfen
- [ ] **A13:** Kernel-Version verifizieren
- [ ] **B1-B2, B7-B9:** Restliche TODO-Platzhalter befüllen
- [ ] **B3:** Related Work schreiben
- [ ] **B4:** DB-Schema-Diagramm einfügen
- [ ] **B5:** Architekturdiagramm erstellen
- [ ] **B6:** Messwerte aus echtem Systemlauf eintragen
