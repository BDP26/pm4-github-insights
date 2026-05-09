# Offene Punkte: LaTeX Report

**Stand:** 2026-05-09 — nur noch pendente Einträge

---

## Daten aus Systemlauf

### TODO-4 · §5.2 Throughput-Tabelle: Fehlende Werte

Bereits eingetragen: Events total (21 748 215), Avg rate (10.7 ev/min).
Noch ausstehend:

| Zeile | Inhalt |
|-------|--------|
| Peak ingestion rate | ev/min — aus Grafana/Logs |
| Median end-to-end latency | s — Poll → Dashboard-Update |
| Kafka consumer lag (steady) | msgs — aus Kafka-UI |

---

### TODO-7 · §5.5 Hidden Gem Cohort-Ergebnisse

Tabelle (24 h / 168 h / 730 h): Snapshots evaluated, total flagged (≥1.5), sustained, dropped, precision.
Aus `/api/hidden-gems/snapshots/{id}/cohort` lesbar oder direkt aus dem Frontend (Evaluation Reports View).

---

### TODO-8 · §5.5 Top-5 Representative Detections

Top-5 Repos nach sig\_score aus dem Live-Dashboard (Repository-Scope, höchster sig\_score über alle Fenster).
Pro Repo: `full_name`, Sprache, sig\_score, stars/forks im Fenster, Einzeiler-Beschreibung.

---

## Personenabhängige TODOs

| ID | Stelle | Inhalt |
|----|--------|--------|
| TODO-1 | Abstract, letzte Zeile | `\TODO{Refine after report completion}` |
| TODO-2 | Header nach `\maketitle` | `\TODO{Fill in revision date}` |
| TODO-9 | §8 Conclusion, erster Paragraph | `\TODO{Write 1–2 paragraphs…}` + Findings-Satz nach §5.5 |
| TODO-HG | §4.6.5 | `\TODO{Insert table of Hidden Gem API endpoints…}` (11 Endpoints, Format wie Tab. 2) |
| TODO-10 | Acknowledgments | `\TODO{Add names of supervisors…}` |
| TODO-11 | Appendix Timeline | Alle `\TODO{name(s)}` in der Owner-Spalte |
| TODO-Challenges | §7, letzter Paragraph | `\TODO{Additional challenges…}` |
