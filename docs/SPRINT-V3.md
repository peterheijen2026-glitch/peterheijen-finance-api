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

## Sprint 3: QA Gate + Sobere Rapportversie
- [ ] RED/ORANGE/GREEN no-send gate
- [ ] Audit package JSON voor ChatGPT
- [ ] PDF page-1 op reconciled cijfers
- [ ] Automatisch blokkeren bij RED
