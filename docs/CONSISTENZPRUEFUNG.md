# Konsistenzprüfung: LaTeX Report vs. Implementierung

**Datum:** 2026-05-09
**Status:** Offene Punkte identifiziert

---

## Übersicht

| Schweregrad | Anzahl |
|-------------|--------|
| 🔴 Kritisch (muss behoben werden) | 1 |
| 🟡 Hoch (sollte behoben werden) | 4 |
| 🟢 Niedrig (Kann ignoriert werden) | 1 |

---

## 🔴 Kritisch

### 1. Falscher Geocoding-Service im Report

**Datei:** `report/report.tex`
**Zeilen:** 335-337 (Kapitel 4 Implementation)

**Problem:**
Der Report beschreibt **Nominatim** als Geocoding-Lösung mit Rate-Limiting von 1 req/s:

```latex
Geocoding. Free-text location strings are resolved to
WGS-84 coordinates via Nominatim~\cite{nominatim}. A rate limiter
enforces the required 1\,req/s ceiling; a one-hour LRU cache avoids
re-geocoding identical strings.
```

Die Implementierung nutzt jedoch **Photon** (lokaler OpenStreetMap-basierter Service) ohne Rate-Limiting.

**Betroffene Dateien:**
- `report/report.tex:335-337`
- `geocoder/geocoder.py` (verwendet Photon, nicht Nominatim)
- `docker-compose.yml:330-347` (Photon Service)

**Fix:**
Ersetze den Geocoding-Abschnitt in Kapitel 4 mit:

```latex
\subsection{Geocoding (Geocoder \& Photon)}
Free-text location strings from user profiles are resolved to WGS-84
coordinates via a local \textbf{Photon} instance (OpenStreetMap-based).
Photon runs as a dedicated Docker container and provides a REST API
queried by the \texttt{geocoder} service. Unlike external geocoding APIs
(such as Nominatim), the local instance has no rate limits and
minimizes latency for the enrichment pipeline. A dedicated
\texttt{geocoder} worker consumes users and organizations with pending
location data from the database and updates their coordinates and
country codes asynchronously.
```

**Referenz:** `report/report.tex:284-292` (Kapitel 3 Architecture) - hier wird Photon korrekt beschrieben, Kapitel 4 muss angepasst werden.

---

## 🟡 Hoch

### 2. Fehlender Continuous Aggregate: `actor_stats_1h`

**Problem:**
Der Report erwähnt in Tabelle `tab:aggregates` (Zeile 365) drei Continuous Aggregates:
- `event_stats_5m`
- `country_ids_5m`
- `actor_stats_1h`

In der Datenbank (`db/init.sql`) existieren jedoch nur zwei:
- `event_stats_5m` (Zeile 111-118)
- `country_ids_5m` (Zeile 123-132)
- `actor_stats_1h` → **FEHLT**

**Betroffene Dateien:**
- `report/report.tex:365`
- `db/init.sql` (muss ergänzt werden)

**Option A - Aus Report entfernen (wenn nicht benötigt):**
Entferne `actor_stats_1h` aus Tabelle `\ref{tab:aggregates}`.

**Option B - In DB hinzufügen (wenn benötigt):**
Füge in `db/init.sql` nach `country_ids_5m` hinzu:

```sql
-- 3. Actor-Statistiken per 1h
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

**Empfehlung:** Prüfe ob `actor_stats_1h` irgendwo in der Implementierung verwendet wird (z.B. in API, Frontend oder Grafana). Wenn nicht, aus Report entfernen.

---

### 3. TODO-Platzhalter in Kapitel 5 (Evaluation)

**Problem:**
Kapitel 5 (`report/report.tex:408-503`) enthält nur TODO-Platzhalter - keine einzige echte Metrik.

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

**Empfehlung:**
1. System mindestens 24-48 Stunden laufen lassen
2. Metriken aus Docker Logs, Grafana Dashboards und DB-Queries sammeln
3. Werte in Report einsetzen

**Nützliche DB-Queries:**
```sql
-- Events gesamt
SELECT COUNT(*) FROM events;

-- Durchschnittliche Ingestionsrate (Events pro Minute)
SELECT 
    COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(time) - MIN(time))) / 60, 1)
FROM events;

-- Kafka consumer lag (via Kafka-UI oder JMX)
-- Compression stats
SELECT * FROM chunk_compression_stats('events');
```

---

### 4. LRU Cache Erwähnung entfernen

**Problem:**
Report erwähnt "one-hour LRU cache" für Nominatim (Zeile 336-337), aber:
- Nominatim wird nicht verwendet (Photon wird genutzt)
- Kein LRU Cache in `geocoder.py` implementiert

**Betroffene Dateien:**
- `report/report.tex:336-337`

**Fix:**
Diese Erwähnung sollte bereits durch Fix #1 (Geocoding-Abschnitt) mitkorrigiert werden, wenn der gesamte Abschnitt neu geschrieben wird.

---

### 5. Batch Flush Intervall undokumentiert

**Problem:**
`enricher.py:16` definiert `BATCH_FLUSH_INTERVAL_S = 10.0` (10 Sekunden), aber der Report erwähnt dieses Intervall nicht.

**Betroffene Dateien:**
- `report/report.tex` (Kapitel 4 Implementation)
- `consumer/enricher.py:16`

**Empfehlung:**
Dokumentiere das 10-Sekunden Flush-Intervall im Implementation-Abschnitt:

```latex
\item \textbf{GraphQL Batch Processing.} The \texttt{Enricher}
  accumulates up to 20 items per batch and flushes automatically
  either when the batch size is reached or after a 10-second time
  window, whichever comes first.
```

---

## 🟢 Niedrig

### 6. TODO-Platzhalter in Abstract und anderen Stellen

| Zeile | TODO-Inhalt |
|-------|-------------|
| 94 | Refine after report completion |
| 142 | Fill in revision date |
| 201 | Write Related Work section |
| 261 | Add db scheme graph |
| 566-569 | Additional challenges |
| 575-578 | Write Conclusion section |
| 609 | Add acknowledgments |
| 629-637 | Project timeline owner names |

**Empfehlung:**
Diese TODOs nach und nach ausfüllen - keine technischen Widersprüche, nur fehlende Inhalte.

---

## Abhänigkeiten

```
┌─────────────────────────────────────────────────────────────┐
│  Fix #1 (Geocoding Abschnitt)                               │
│  └── Behebt auch:                                           │
│      - Widerspruch in Kapitel 3 vs Kapitel 4                 │
│      - LRU Cache Erwähnung (Fix #4)                          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Fix #2 (actor_stats_1h)                                     │
│  └── Entscheidung: Entfernen ODER Implementieren             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Fix #3 (Kapitel 5 Evaluation)                              │
│  └── Erfordert: System-Run 24-48h                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Checkliste

- [ ] **Fix #1:** Geocoding-Abschnitt in Kapitel 4 korrigieren (Nominatim → Photon)
- [ ] **Fix #2:** `actor_stats_1h` - Entscheidung treffen (entfernen oder implementieren)
- [ ] **Fix #3:** Kapitel 5 mit echten Metriken füllen (nach System-Run)
- [ ] **Fix #4:** LRU Cache Erwähnung entfernen (automatisch durch Fix #1)
- [ ] **Fix #5:** Batch Flush Intervall dokumentieren
- [ ] **Niedrig:** Alle TODO-Platzhalter im Report ausfüllen

---

## Notizen

- Die Architektur in Kapitel 3 ist weitgehend korrekt und konsistent mit der Implementierung
- Die Hauptunstimmigkeit liegt in Kapitel 4 (Implementation), wo Nominatim statt Photon beschrieben wird
- Alle anderen Komponenten (Kafka, TimescaleDB, FastAPI, Next.js, Consumer) stimmen mit dem Report überein
