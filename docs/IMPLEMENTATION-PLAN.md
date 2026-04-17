# Output C — Implementatieplan

## Versie 1.0 — April 2026

---

## 1. Overzicht van wijzigingen

De implementatie vervangt zes losse classificatiefuncties door een drielaags decision engine. De code wordt herschreven in drie fases, waarbij elke fase zelfstandig testbaar is.

---

## 2. Nieuwe functies

### 2.1 Laag A — Hard Deterministic Rules

| Functie | Wat het doet | Vervangt |
|---------|-------------|----------|
| `_classificeer_deterministic()` | Alle harde regels uit Bijlage A van de architectuurnotitie. Één functie die in vaste volgorde alle deterministische checks doorloopt. | `_classificeer_rule_based()` (gedeeltelijk) |
| `_bouw_eigen_financieel_domein()` | Ongewijzigd — detecteert eigen FI IBANs | Bestaand (behouden) |
| `_markeer_interne_transfers()` | Ongewijzigd — markeert eigen-rekening transfers | Bestaand (behouden) |
| `_detecteer_huishoudleden()` | Ongewijzigd — markeert huishoudleden | Bestaand (behouden) |

**`_classificeer_deterministic()`** — Nieuwe functie

Combineert de harde regels die nu verspreid zitten over `_classificeer_rule_based()`, `_detecteer_vast_inkomen()` (keyword-laag), en het overheid-deel van `_classificeer_inflow_type()`.

Volgorde van checks:
1. Interne transfers (al gemarkeerd door eerdere functies)
2. Belastingdienst (positief/negatief, met subtype-detectie)
3. Overheid-keywords (UWV, SVB, toeslagen, DUO, gemeente, CJIB)
4. Salaris/Loon-keyword (SALARIS, LOON, SALARY)
5. Pensioen-keyword (PENSIOEN, AOW)
6. Merchant mapping (200+ merchants)
7. Eigen financieel domein (IBAN match)
8. FI-keyword naam match (zonder IBAN match)
9. Dividend/investment-income keywords (alleen bij FI-tegenpartij)
10. Refund-keywords (RETOUR, STORNO, etc.)
11. Transfer-keywords (TIKKIE, SPAARREKENING)
12. Lening-keywords (HYPOTHEEK UITBETALING, LENING)

Elke check zet: `regel_sectie`, `regel_categorie`, `regel_confidence`, `classificatie_bron='rule'`

Output: DataFrame met alle deterministisch geclassificeerde transacties gemarkeerd.

### 2.2 Laag B — Evidence-Based Classifiers

| Functie | Wat het doet | Vervangt |
|---------|-------------|----------|
| `_classify_positive_inflows()` | Orchestrator: roept classifiers aan in volgorde | `_classificeer_inflow_type()` |
| `_rent_classifier()` | 9-check rent income classifier | `_detecteer_huurinkomsten()` |
| `_salary_classifier()` | 8-check salary pattern classifier | `_detecteer_vast_inkomen()` (patroon-lagen) |
| `_refund_matcher()` | 5-check refund matcher | Refund-deel van `_classificeer_inflow_type()` |
| `_uncertainty_gate()` | Subcategoriseert en markeert uncertain inflows | catch-all van `_classificeer_inflow_type()` |

**`_classify_positive_inflows()`** — Orchestrator

Input: DataFrame na Laag A, met `eigen_fi_ibans` set.

Logica:
1. Selecteer alle ongeclass positieve transacties (`classificatie_bron.isna() & bedrag > 0 & ~is_intern`)
2. Groepeer op tegenpartij (IBAN primair, naam fallback)
3. Voor elke groep:
   a. `_refund_matcher()` → als LIKELY: classificeer, volgende groep
   b. Als tegenpartij rechtsvorm heeft: `_salary_classifier()` → als LIKELY: classificeer
   c. Als tegenpartij GEEN rechtsvorm: `_rent_classifier()` → als LIKELY: classificeer
   d. `_uncertainty_gate()` → subcategoriseer als uncertain
4. Losse transacties (niet in groep): direct naar `_uncertainty_gate()`

Output: Alle positieve transacties geclassificeerd (LIKELY of UNCERTAIN).

**`_rent_classifier(groep, alle_tx_tegenpartij, eigen_rekeningen, eigen_fi_ibans)`**

Input: groep (positieve transacties van één tegenpartij), alle transacties van die tegenpartij, sets.

Output: `('likely', 'Huurinkomsten')` of `('uncertain', 'Onzeker positief')` of `('reject', None)`

Implementatie: exact de 9 evidence checks uit het classifier-ontwerp.

Per check wordt het resultaat opgeslagen in een dict voor logging:
```python
evidence = {
    'E1_recurring': (True, '13 transacties'),
    'E2_externality': (True, 'IBAN niet in eigen rekeningen'),
    ...
}
```

**`_salary_classifier(groep, alle_tx_tegenpartij, eigen_rekeningen, eigen_fi_ibans)`**

Input/output: zelfde structuur als rent classifier.

**`_refund_matcher(transactie, df_negatief)`**

Input: enkele positieve transactie, DataFrame met alle negatieve transacties.

Output: `('likely', 'Terugbetaling/Refund')` of `('uncertain', ...)` of `('reject', None)`

**`_uncertainty_gate(transactie_of_groep, context)`**

Input: transactie(s) die geen classifier kon plaatsen.

Output: subcategorie (`Onzeker positief`, `Onzeker positief (bidirectioneel)`, etc.)

### 2.3 Laag C — AI Narrative (aanpassing)

| Functie | Wijziging |
|---------|-----------|
| `bouw_prompt()` | Filter uitbreiden: stuur GEEN positieve transacties meer naar AI. Alleen negatieve ongeclass transacties. |
| `_merge_rule_en_ai_totalen()` | Ongewijzigd — merge werkt al correct |

### 2.4 Quality Gates (aanpassing)

| Functie | Wijziging |
|---------|-----------|
| `_rapport_kwaliteitscheck()` | Toevoegen: uncertain-percentage check (>50% = blokkade, >20% = waarschuwing) |
| Page-1 berekening | Whitelist-based: alleen expliciet goedgekeurde categorieën |

### 2.5 Consistentie

| Functie | Status |
|---------|--------|
| `_afdwing_iban_consistentie()` | Behouden — draait NA Laag B, propageert classificaties naar resterende transacties met zelfde IBAN |

---

## 3. Bestaande code die aangepast wordt

| Huidig | Actie | Reden |
|--------|-------|-------|
| `_classificeer_rule_based()` | Vervangen door `_classificeer_deterministic()` | Combinatie van alle harde regels in één functie |
| `_detecteer_vast_inkomen()` | Keyword-deel → `_classificeer_deterministic()`. Patroon-deel → `_salary_classifier()` | Scheiding deterministic vs evidence-based |
| `_detecteer_huurinkomsten()` | Vervangen door `_rent_classifier()` | Transparanter met evidence checks |
| `_classificeer_inflow_type()` | Vervangen door `_classify_positive_inflows()` + sub-classifiers | Orchestrator ipv monolithische functie |
| `bouw_prompt()` | Filter aanscherpen: alleen negatieve tx naar AI | AI mag niet over positieve tx beslissen |
| `_rapport_kwaliteitscheck()` | Uncertain checks toevoegen | Nieuwe quality gate |
| Page-1 berekening (in PDF-generatie) | Whitelist ipv blacklist | Principieel: alleen goedgekeurde categorieën tellen |

---

## 4. Pipeline — Nieuwe volgorde

```python
# Fase 1: Data parsing & enrichment (ongewijzigd)
df = _normaliseer(df, formaat)
df = _verrijk_transactie_velden(df)
eigen_rekeningen = _bouw_huishoudregister(df)
df = _markeer_interne_transfers(df, eigen_rekeningen)
df = _detecteer_huishoudleden(df)

# Fase 2: Laag A — Deterministic Rules
eigen_fi_ibans = _bouw_eigen_financieel_domein(df)
df = _classificeer_deterministic(df, eigen_fi_ibans)

# Fase 3: Laag B — Evidence-Based Classifiers
df = _classify_positive_inflows(df, eigen_fi_ibans, eigen_rekeningen)

# Fase 4: Consistentie
df = _afdwing_iban_consistentie(df)

# Fase 5: Deterministic totals
feiten = bereken_feiten(df)
top = bereken_top(df)
rule_totalen = _bereken_rule_based_totalen(df)

# Fase 6: Laag C — AI Narrative (alleen negatieve ongeclass tx)
prompt = bouw_prompt(df, feiten, top, eigen_rekeningen)
ai_result = vraag_openai(prompt)  # of vraag_claude()

# Fase 7: Merge + Quality Gates
merged = _merge_rule_en_ai_totalen(rule_totalen, ai_result)
kwaliteit = _rapport_kwaliteitscheck(merged, df)

# Fase 8: Page-1 Metrics (whitelist)
structural_income = _bereken_structural_income(merged)  # NIEUW
```

---

## 5. Regressietests

### 5.1 Unit tests per classifier

| Test | Input | Verwacht |
|------|-------|----------|
| `test_rent_likely` | 9x €2.500 + 3x €2.600 + 1x €100 van particulier | LIKELY |
| `test_rent_quarterly` | 4x €3.000 per kwartaal van particulier | LIKELY |
| `test_rent_reject_employer` | 10x €2.500 van "Jansen B.V." | REJECT (E6 faalt) |
| `test_rent_reject_merchant` | 5x €500 van "Albert Heijn" | REJECT (E7 faalt) |
| `test_rent_reject_bidirectional` | 6x €1.000 ontvangen + 4x €800 betaald aan zelfde IBAN | REJECT (E5 faalt: 40% negatief) |
| `test_rent_uncertain_unstable` | 5x bedragen €500, €2.000, €100, €3.000, €800 | UNCERTAIN (E3 faalt) |
| `test_rent_reject_low_amount` | 8x €50 van particulier | REJECT (E4 faalt: mediaan < €300) |
| `test_salary_likely_bv` | 10x €4.000 van "Heijen Holding B.V." | LIKELY als DGA-loon |
| `test_salary_likely_stichting` | 12x €3.500 van "Stichting OLVG" | LIKELY als Netto salaris |
| `test_salary_uncertain_no_form` | 5x €2.000 van "Jan de Vries" | Doorschuiven naar rent classifier |
| `test_refund_keyword` | €50 met "RETOUR" in omschrijving | LIKELY als refund |
| `test_refund_amount_match` | €29.99 ontvangen, €29.99 betaald aan zelfde partij 5 dagen eerder | LIKELY als refund |
| `test_refund_no_match` | €500 van onbekende partij, geen keyword | REJECT |
| `test_uncertain_verzekeraar` | €3.000 van "Achmea" zonder uitkering-keyword | UNCERTAIN (verzekeraar) |
| `test_uncertain_bidirectioneel` | €1.000 van IBAN dat ook €500 uitgaand heeft | UNCERTAIN (bidirectioneel) |

### 5.2 Integratie-test: Peter Heijen dossier

| Metric | Verwacht | Tolerantie |
|--------|----------|------------|
| Structureel inkomen page-1 | ~€13.700/mnd | ±€500 |
| Huurinkomsten | ~€2.533/mnd | ±€100 |
| Netto salaris | ~€8.500/mnd | ±€500 |
| Onzeker positief | Lager dan huidige €105K | Maximaal €80K |
| Belastingteruggave | ~€7.100/jaar | ±€500 |
| Asset withdrawals | Correct in sparen_beleggen | Geen als inkomen |
| AI ontvangt | 0 positieve transacties | Exact 0 |

### 5.3 Before/After vergelijking

Na implementatie draai ik het dossier en documenteer:
- Elke transactie die anders geclassificeerd wordt dan voorheen
- Waarom (welke evidence check, welke laag)
- Effect op page-1 structural income
- Effect op uncertain bucket grootte
- Eventuele nieuwe false positives/negatives

---

## 6. Implementatievolgorde

### Stap 1: `_classificeer_deterministic()` bouwen
- Combineer alle harde regels uit bestaande functies
- Test: draai op Peter's dossier, verificeer dat alle deterministisch-zekere transacties correct geclassificeerd worden
- Verwacht: ~60-70% van transacties geclassificeerd

### Stap 2: `_rent_classifier()` bouwen
- 9 evidence checks, driedelige uitkomst
- Test: Peter's huurder (De Monnink) moet LIKELY scoren
- Test: synthetische cases (kwartaalhuur, bidirectioneel, lage bedragen)

### Stap 3: `_salary_classifier()` bouwen
- 8 evidence checks
- Test: OLVG moet LIKELY scoren (maar wordt al door keyword in Laag A gevangen)
- Test: B.V. zonder salaris-keyword

### Stap 4: `_refund_matcher()` bouwen
- 5 evidence checks
- Test: keyword-based refunds, bedrag-match refunds

### Stap 5: `_classify_positive_inflows()` orchestrator bouwen
- Roept classifiers aan in volgorde
- Test: alle positieve tx krijgen een classificatie (LIKELY of UNCERTAIN)
- Test: 0 positieve tx ongeclass na deze stap

### Stap 6: `_uncertainty_gate()` bouwen
- Subcategorisering
- Review flags
- Test: uncertain transacties krijgen juiste subcategorie

### Stap 7: `bouw_prompt()` aanpassen
- Filter: alleen negatieve ongeclass tx naar AI
- Test: AI prompt bevat 0 positieve bedragen

### Stap 8: Page-1 whitelist implementeren
- `_bereken_structural_income()` functie
- Test: page-1 getal matcht exact de whitelist

### Stap 9: Quality gates uitbreiden
- Uncertain-percentage check
- Test: rapport met >50% uncertain wordt geblokkeerd

### Stap 10: Before/after vergelijking (Output D)
- Draai compleet dossier
- Documenteer alle verschillen
- Verifieer page-1 structural income

---

## 7. Wat NIET verandert

- Data parsing & enrichment (formaat-detectie, normalisatie, IBAN-extractie)
- Intern-detectie (eigen rekeningen, huishoudleden)
- Eigen financieel domein register (werkt goed)
- MERCHANT_MAPPING (200+ merchants)
- Keyword-lijsten (OVERHEID, FI, REFUND, etc.)
- AI prompt structuur (wordt alleen gefilterd, niet herschreven)
- PDF-generatie (alleen page-1 berekening wijzigt)
- Email-verzending
- API endpoints
