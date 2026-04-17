# Sprint V3 — Van Gokken naar Reconciliatie

## Status: IN UITVOERING
## Datum: 17 april 2026

---

## Sprint 1: Anti-Gok Fundament (huidige sprint)

### Gedaan:
- [x] `source_family` veld op elke transactie (salary_employment, management_fee, rental_income, benefits_uwv, child_benefit, tax_refund, household_transfer, internal_transfer, broker_return, wealth_allocation, creditcard_settlement, neutral_technical)
- [x] `party_type` gating in `_detecteer_vast_inkomen()` — household_related_party en own_account kunnen NOOIT salary zijn
- [x] Logging bij geblokkeerde salary-classificaties
- [x] Management_fee als aparte source_family (niet meer onder salary_employment)
- [x] MERCHANT_MAPPING uitgebreid: VvE apart, Levensverzekering/ORV apart, Mobiliteit vast, Cash opname
- [x] Household-transacties → `onderling_neutraal` sectie (niet meer is_intern)
- [x] Post-classificatie reconciliatie functie (`_post_classificatie_reconciliatie()`)
- [x] Periodeberekening: onvolledige maanden detectie (`_bepaal_rapportperiode()`)
- [x] Reconciliatie en rapportperiode in pipeline geïntegreerd

### Nog te doen in Sprint 1:
- [ ] Commit + push naar Railway
- [ ] Test met Peter's bankdata
- [ ] Verificatie: Esther's overboekingen in onderling_neutraal

## Sprint 2: Ground Truth + Excel Output
- [ ] Frozen ground truth payload na reconciliatie
- [ ] Reconciliatie Excel generator (Shortcut.ai format)
- [ ] AI prompt herschrijven: alleen ground truth
- [ ] Inkomstenbronopbouw i.p.v. "Netto salaris"

## Sprint 3: QA Gate + Sobere Rapportversie
- [ ] RED/ORANGE/GREEN no-send gate
- [ ] Audit package JSON voor ChatGPT
- [ ] PDF page-1 op reconciled cijfers
- [ ] Automatisch blokkeren bij RED
