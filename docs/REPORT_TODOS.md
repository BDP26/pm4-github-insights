# Report TODO-Liste: Kapitel 1–5

Dieses Dokument listet alle identifizierten Unstimmigkeiten zwischen dem LaTeX-Report (`report/report.tex`) und der tatsächlichen Implementierung im Repository sowie alle offenen Platzhalter/TODOs innerhalb des Reports selbst.

---

## A — Inhaltliche Unstimmigkeiten (Report ≠ Code)

### A1 · Docker-Service-Tabelle in Kap. 3 unvollständig
**Ort:** Section 3 (Architecture), Tabelle „Expanded System Component Overview"  
**Problem:** Die Tabelle listet 8 Services. In `docker-compose.yml` existieren zusätzlich `kafka-ui` (Kafka-Web-UI auf Port 8080) und `kafka-init` (einmaliger Container zur Topic-Erstellung). Beide fehlen vollständig.  
**Fix:** Zeilen für `kafka-ui` und `kafka-init` ergänzen.

---

### A2 · Consumer als einzelner Service dargestellt
**Ort:** Section 3.3 und Tabelle in Section 3  
**Problem:** Text und Tabelle beschreiben „den Consumer" als eine Einheit. Tatsächlich laufen in der Standardkonfiguration drei unabhängige Instanzen: `consumer-0`, `consumer-1`, `consumer-2` (je einer pro Kafka-Partition).  
**Fix:** In der Tabelle den Service als `consumer-{0,1,2}` kennzeichnen und im Text klarstellen, dass es sich um drei parallel laufende Instanzen handelt.

---

### A3 · Kafka-Topic `geocoder.requests` nicht erwähnt
**Ort:** Section 3.2 (Message Broker / Kafka)  
**Problem:** Der Report nennt drei Kafka-Topics (`github.events.raw`, `github.events.status`, `github.ratelimit`). Das vierte Topic `geocoder.requests` (1 Partition, genutzt für Geocoder-Request-Logs) fehlt in der Beschreibung und in der Übersicht.  
**Fix:** Topic `geocoder.requests` mit seiner Funktion ergänzen.

---

### A4 · Geocoding-Service in Kap. 4.2 fälschlich als Nominatim bezeichnet
**Ort:** Section 4.2, zweiter Unterpunkt  
**Problem:** Der Report schreibt:  
> *„Free-text location strings are resolved to WGS-84 coordinates via **Nominatim**."*  
Tatsächlich nutzt `geocoder/geocoder.py` **Photon** (`PHOTON_URL = "http://photon:2322/api"`). Nominatim wird nicht verwendet. (Abschnitt 3.6 ist korrekt und nennt bereits Photon.)  
**Fix:** „Nominatim" in Abschnitt 4.2 durch „Photon" ersetzen; Konsistenz mit Abschnitt 3.6 herstellen.

---

### A5 · Rate-Limiter (1 req/s) für Geocoding nicht implementiert
**Ort:** Section 4.2, zweiter Unterpunkt  
**Problem:** Der Report behauptet:  
> *„A rate limiter enforces the required 1 req/s ceiling."*  
In `geocoder/geocoder.py` gibt es keinen Rate-Limiter. Wenn keine ausstehenden Anfragen vorhanden sind, schläft der Service 30 Sekunden (`time.sleep(30)`). Bei verfügbaren Requests wird sofort angefragt – ohne expliziten Delay.  
**Fix:** Beschreibung korrigieren: kein 1 req/s Limit; Verarbeitungslogik akkurat beschreiben.

---

### A6 · LRU-Cache (1 Stunde) für Geocoding nicht implementiert
**Ort:** Section 4.2, zweiter Unterpunkt  
**Problem:** Der Report behauptet:  
> *„a one-hour LRU cache avoids re-geocoding identical strings."*  
Kein LRU-Cache in `geocoder.py`. Deduplication erfolgt über das Datenbankfeld `geo_claimed_at`: ein User/Org wird nur einmal beansprucht und danach nicht erneut angefragt.  
**Fix:** LRU-Cache-Aussage entfernen; stattdessen das DB-basierte `geo_claimed_at`-Claim-Verfahren beschreiben.

---

### A7 · In-Memory-Dictionary-Cache für User-Profile nicht vorhanden
**Ort:** Section 4.2, erster Unterpunkt  
**Problem:** Der Report schreibt:  
> *„Results are cached in a dictionary for the process lifetime to avoid redundant API calls."*  
Der `Enricher` in `consumer/enricher.py` verwendet keinen In-Memory-Dictionary. Stattdessen wird vor jeder Anreicherung per DB-Query geprüft (`SELECT 1 FROM users WHERE username = %s AND fetched_at IS NOT NULL`), ob der User bereits angereichert wurde.  
**Fix:** Dictionary-Cache-Aussage entfernen; DB-basierte `fetched_at`-Prüfung als Deduplication-Mechanismus beschreiben.

---

### A8 · Beschreibung „two enrichment passes" vereinfacht/ungenau
**Ort:** Section 4.2 Einleitung  
**Problem:** Der Report beschreibt zwei sequentielle Enrichment-Passes. Tatsächlich hat der `Enricher` zwei **parallele** Batch-Queues mit je eigenem Token-Pool:  
- Queue 1: Users (und implizit Organisations über `repositoryOwner`)  
- Queue 2: Repos  

Jede Queue hat GraphQL-Batching (bis 20 Items) und REST-Fallback. Organisations werden als Nebeneffekt der User-Queue erkannt (über `__typename == "Organization"`), nicht in einem separaten Pass.  
**Fix:** Enrichment-Architektur als zwei parallele Queues (User+Org / Repo) beschreiben.

---

### A9 · Continuous Aggregate `actor_stats_1h` existiert nicht im Code
**Ort:** Section 4.3, Tabelle „TimescaleDB continuous aggregates"  
**Problem:** Die Tabelle listet drei Aggregates:
- `event_stats_5m` ✓ (in `db/init.sql` vorhanden)
- `country_ids_5m` ✓ (in `db/init.sql` vorhanden)
- `actor_stats_1h` ✗ — **nicht definiert**, weder in `db/init.sql` noch in irgendeiner Migration (`db/migrations/`)

**Fix:** Entweder `actor_stats_1h` implementieren und in `db/migrations/` hinzufügen, oder die Tabellenzeile entfernen.

---

### A10 · API-Endpoint-Tabelle in Kap. 4.4 unvollständig
**Ort:** Section 4.4, Tabelle „FastAPI endpoint reference"  
**Problem:** Die Tabelle listet 6 Endpoints. Die tatsächliche API hat deutlich mehr, die über zwei Router (`routers/hidden_gems.py`, `routers/activity.py`) exponiert werden:
- `GET /api/hidden-gems/*` (Filter, Rankings, Scoring für Repos/Users/Orgs)
- `GET /api/overview/heatmap`
- `GET /api/overview/globe-heatmap`
- `GET /api/activity/leaderboard`

**Fix:** Tabelle um die zusätzlichen Endpoints erweitern, oder einen Hinweis einfügen, dass die vollständige Referenz unter `/docs` (Swagger UI) verfügbar ist.

---

### A11 · SSE Keep-alive Kommentar falsch beschriftet
**Ort:** Section 4.4, Beschreibung des SSE-Endpoints  
**Problem:** Der Report schreibt:  
> *„Keep-alive comments (`: ping`) are sent between data frames."*  
Im Code (`api/main.py`) werden tatsächlich `: heartbeat\n\n` (initial) und `: keep-alive\n\n` (zwischen Events) gesendet – nicht `: ping`.  
**Fix:** „`: ping`" durch „`: keep-alive`" ersetzen.

---

### A12 · Grafana als „primary platform" vs. Next.js-Dashboard
**Ort:** Section 3.5 (Visualization)  
**Problem:** Abschnitt 3.5 bezeichnet Grafana als die „central visualization hub" und „primary platform". Kapitel 4.5 beschreibt jedoch ein vollwertiges Next.js-Dashboard mit eigenen Endpoints und SSE-Stream. Beide Plattformen sind gleichwertige Visualisierungsschichten mit verschiedenen Zielgruppen (operatives Monitoring vs. interaktive Analyse).  
**Fix:** Formulierung anpassen: Grafana für System-Monitoring/Ops, Next.js-Dashboard für Endnutzer-Analyse.

---

### A13 · Kernel-Version in Kap. 5 wirkt wie Platzhalter
**Ort:** Section 5.1.1, Tabelle „System Specifications"  
**Problem:** Der eingetragene Kernel ist `Linux 6.17.0-20-generic`. Ubuntu 24.04 LTS (Noble) nutzt in der Praxis typischerweise Kernel 6.8.x (GA) oder 6.11.x (HWE). Version 6.17 existierte zum Zeitpunkt des Projekts nicht als stabiler Ubuntu-Kernel. Möglicherweise ein falscher/selbst-ausgedachter Wert.  
**Fix:** Tatsächliche Kernel-Version vom Produktions-Server eintragen (`uname -r`).

---

## B — Offene TODO-Platzhalter im Report (Kapitel 1–5)

### B1 · Abstract: TODO-Marker vorhanden
**Ort:** Abstract, letzte Zeile  
**Problem:** `\TODO{Refine after report completion}` ist noch sichtbar.

---

### B2 · Empfangsdatum (Revision) fehlt
**Ort:** Header nach `\maketitle`  
**Problem:** `\received[revised]{\TODO{Fill in revision date}}` — Revisionsdatum nicht eingetragen.

---

### B3 · Kapitel 2 (Related Work) vollständig leer
**Ort:** Section 2  
**Problem:** Nur eine TODO-Box vorhanden, kein einziger Satz Inhalt. Ca. 300–400 Wörter zu verwandten Systemen (GH Archive, GHTorrent, Lambda/Kappa-Architektur, TimescaleDB-Vergleiche) fehlen komplett.

---

### B4 · Datenbankschema-Diagramm fehlt (Kap. 3.4)
**Ort:** Section 3.4 (Data Storage), letztes Item  
**Problem:** `\TODO{Add db scheme graph}` — Das DB-Schema-Diagramm ist nicht eingefügt. Eine `schema.dbml`-Datei existiert bereits unter `db/schema.dbml` und könnte als Basis dienen.

---

### B5 · Architekturdiagramm fehlt (Kap. 3)
**Ort:** Figure 1, Section 3  
**Problem:** `\TODO{Insert architecture diagram here.}` — Die Figure ist ein Placeholder-Rahmen ohne echtes Diagramm.

---

### B6 · Alle Messwerte in Kap. 5.2–5.4 sind TODO
**Ort:** Sections 5.2, 5.3, 5.4  
**Problem:** Throughput, Latenz, Data-Coverage-Metriken, Storage-Effizienz – alles mit `\TODO{N/X}` belegt. Ein echter System-Run (24–48 Stunden) ist laut Report-Text empfohlen, wurde aber nicht durchgeführt/dokumentiert.

---

### B7 · Kapitel 8 (Conclusion) teilweise TODO
**Ort:** Section 8  
**Problem:** Der erste Paragraph ist als TODO markiert (`\TODO{Write 1–2 paragraphs…}`). Der zweite Paragraph und die Future-Work-Liste sind vorhanden, aber der Einleitungsparagraph fehlt.

---

### B8 · Acknowledgments: Supervisor-Namen fehlen
**Ort:** Acknowledgments-Block  
**Problem:** `\TODO{Add names of supervisors…}` — Namen der Betreuer nicht eingetragen.

---

### B9 · Anhang: Timeline-Tabelle ohne Verantwortliche
**Ort:** Appendix, Tabelle „Actual project timeline"  
**Problem:** Alle „Owner"-Spalten enthalten `\TODO{name(s)}` — keine Zuweisung zu Teammitgliedern.

---

## Priorisierungs-Empfehlung

| Priorität | Item | Aufwand |
|-----------|------|---------|
| Hoch | A4 – Nominatim→Photon korrigieren | minimal |
| Hoch | A5 – Rate-Limiter-Aussage entfernen | minimal |
| Hoch | A6 – LRU-Cache-Aussage entfernen | minimal |
| Hoch | A7 – Dictionary-Cache-Aussage entfernen | minimal |
| Hoch | A9 – `actor_stats_1h` implementieren oder Tabelle korrigieren | mittel |
| Hoch | B3 – Related Work schreiben | hoch |
| Hoch | B6 – Messwerte aus echtem Systemlauf eintragen | hoch |
| Mittel | A1 – Service-Tabelle um kafka-ui/kafka-init ergänzen | minimal |
| Mittel | A2 – Consumer als 3 Instanzen darstellen | minimal |
| Mittel | A10 – Endpoint-Tabelle vervollständigen | klein |
| Mittel | B5 – Architekturdiagramm erstellen und einfügen | mittel |
| Mittel | B4 – DB-Schema-Diagramm einfügen | klein |
| Niedrig | A3 – geocoder.requests Topic erwähnen | minimal |
| Niedrig | A8 – Enrichment-Architektur präzisieren | klein |
| Niedrig | A11 – SSE-Kommentar-String korrigieren | minimal |
| Niedrig | A12 – Grafana vs. Next.js Formulierung schärfen | minimal |
| Niedrig | A13 – Kernel-Version verifizieren | minimal |
| Niedrig | B1–B2, B7–B9 – Restliche TODO-Platzhalter befüllen | variabel |
