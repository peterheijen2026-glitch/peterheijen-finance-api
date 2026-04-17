# Sprint V3 — Van Gokken naar Reconciliatie

## Status: IN UITVOERING
## Datum: 17 april 2026

---

## Sprint 1: Anti-Gok Fundament ✅ COMPLEET

### Gedaan:
- [x] `source_family` veld op elke transactie
- [x] `party_type` gating — household_related_party en own_account kunnen NOOIT salary zijn
- [x] MERCHANT_MAPPING uitgebreid: VvE, Levensverzekering/ORV, Mobiliteit vast, Cash opname
- [x] Household-transacties → `onderling_neutraal` sectie
- [x] Post-classificatie reconciliatie + periodeberekening
- [x] Commit + push naar Railway (489a9e9)

## Sprint 2: Ground Truth + Excel Output (huidige sprint)

### Gedaan:
- [x] `_bouw_ground_truth()` — bevroren payload met combined totalen, income_sources, saldo
- [x] `_genereer_reconciliatie_excel()` — openpyxl Excel in Shortcut.ai Cashflow Overzicht format
- [x] `_bouw_ground_truth_prompt_sectie()` — formatteert ground truth voor AI-prompt
- [x] AI prompt herschreven: ground truth sectie meegegeven, AI mag niet zelf rekenen
- [x] Pipeline geherstructureerd: rule_totalen + voorlopige GT vóór AI-call, definitieve GT na merge
- [x] Reconciliatie Excel als email-bijlage (naast PDF)
- [x] Inkomstenbronopbouw via source_family in ground truth

### Nog te doen in Sprint 2:
- [ ] Commit + push naar Railway
- [ ] Test met Peter's bankdata
- [ ] Verificatie: Controle(=0) in Excel, ground truth in AI-tekst

## Sprint 3: QA Gate + Audit Package (huidige sprint)

### Gedaan:
- [x] `_no_send_gate()` — RED/ORANGE/GREEN besluit op basis van:
  - Reconciliatie status (saldo mismatch → RED)
  - >5% in Overig-categorieën → ORANGE, >15% → RED
  - AI-samenvatting met getallen buiten ground truth → ORANGE
  - Lage rule-based coverage (<40%) → ORANGE
- [x] `_bouw_audit_package()` — compleet dossier voor ChatGPT review
- [x] Pipeline blokkering: bij RED geen PDF/rapport verzonden
- [x] Blokkade-email naar Peter met reconciliatie Excel voor review
- [x] `/rapport/{job_id}/audit` endpoint voor audit package ophalen
- [x] Gate-info in `/rapport/{job_id}/status` response
- [x] Job status 'geblokkeerd' als gate BLOCK geeft

- [x] PDF page-1 op reconciled cijfers: n_maanden uit ground truth, periode-info, kwaliteitsindicator

### Nog te doen:
- [ ] Commit + push naar Railway
- [ ] Test met Peter's bankdata
