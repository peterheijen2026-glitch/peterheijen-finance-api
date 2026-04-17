# Related Party Resolution V1 — Ontwerp

## Versie 1.3 — April 2026

### Changelog
- **v1.3**: Laatste aanscherping voor implementatie:
  - unresolved_private_counterparty: alleen rent classifier, NIET salary/benefits
  - Open vragen beantwoord door Peter
  - Goedgekeurd voor implementatie
- **v1.2**: Fundamentele correctie op basis van Peter's review:
  - `unknown_counterparty` blokkeerde ten onrechte de rent classifier → echte huurders (De Monnink) belandden in uncertain
  - Opgelost door party resolution en income classification als twee gescheiden lagen te behandelen
  - `unknown_counterparty` vervangen door `unresolved_private_counterparty` — mag wél door income classifiers, maar met strenge evidence
  - `known_external_counterparty` toegevoegd voor bewezen externe partijen (merchant, overheid, etc.)
  - Nieuwe 3-staps pipeline: hard exclusions → income classifiers → page-1 whitelist
- **v1.1**: S1 nooit doorslaggevend, signaalklassen, business_related voorzichtiger
- **v1.0**: Initieel ontwerp

---

## 1. Kernprincipe: twee gescheiden lagen

### Laag 1: Party Resolution
Bepaalt WIE de tegenpartij is (relatie tot het huishouden).
Output: `party_type` label per tegenpartij.

### Laag 2: Economic Inflow Classification  
Bepaalt WAT de transactie economisch is (huur, salaris, uitkering, etc.).
Output: `regel_categorie` + `regel_sectie` per transactie.

**De fout in v1.1**: party resolution blokkeerde income classification voor onopgeloste tegenpartijen. "We weten niet wie je bent" werd "je mag geen inkomen zijn". Dat is verkeerd — een huurder die we niet als extern kunnen bewijzen is nog steeds een huurder als de transactie-evidence sterk genoeg is.

**De juiste regel**: party resolution mag alleen income classification blokkeren voor bewezen interne partijen (own_account, household). Voor alle andere party_types moet de income classifier zelf bepalen of er voldoende evidence is.

---

## 2. Party types (herzien)

| Label | Definitie | Voorbeeld |
|-------|-----------|-----------|
| `own_account` | Eigen rekening van de klant | Eigen spaarrekening, eigen beleggingsrekening |
| `household_related_party` | Bewezen economisch onderdeel van het huishouden | Partner (bidirectioneel bewezen), partner's 2e IBAN (multi-IBAN linked) |
| `business_related_party` | Bewezen gerelateerde zakelijke entiteit | DGA's eigen holding B.V. (met salaris-keyword of bidirectioneel) |
| `known_external_counterparty` | Bewezen externe partij | Merchant (via MERCHANT_MAPPING), overheid, werkgever (rechtsvorm zonder naamoverlap) |
| `unresolved_private_counterparty` | Privépersoon/entiteit zonder bewezen relatie tot huishouden | Particulier die geld stuurt — kan huurder zijn, kan familie zijn, kan kennis zijn |

### Wat is er veranderd t.o.v. v1.1?
- `unknown_counterparty` → gesplitst in `known_external_counterparty` en `unresolved_private_counterparty`
- `candidate_business_related` → samengevoegd: zonder extra bewijs wordt het `known_external_counterparty` (behandel als normale werkgever)
- Het cruciale verschil: `unresolved_private_counterparty` mag WÉL door de income classifiers

---

## 3. Classifier-toegang per party_type

Dit is de kern van het herziene ontwerp:

| party_type | Rent classifier? | Salary classifier? | Benefits classifier? | Mag structural income? | Wanneer uncertain? |
|------------|-----------------|--------------------|--------------------|----------------------|-------------------|
| `own_account` | **NEE** | **NEE** | **NEE** | **NEE** — altijd is_intern | Nooit — is definitief intern |
| `household_related_party` | **NEE** | **NEE** | **NEE** | **NEE** — altijd is_intern | Nooit — is definitief intern |
| `business_related_party` | **NEE** | **JA** | **NEE** | **JA** — alleen als salary classifier LIKELY geeft | Als salary classifier UNCERTAIN of REJECT |
| `known_external_counterparty` | **JA** | **JA** | **JA** | **JA** — als classifier LIKELY geeft | Als geen classifier LIKELY geeft |
| `unresolved_private_counterparty` | **JA** | **NEE** | **NEE** | **JA** — alleen via rent classifier LIKELY | Als rent classifier niet LIKELY geeft |

### Verschil tussen known_external en unresolved_private

Het verschil is nu concreet in V1:
- `known_external`: alle classifiers (rent, salary, benefits) → resultaat direct bruikbaar
- `unresolved_private`: alleen rent classifier → salary en benefits zijn te risicovol voor onbekende privérekeningen

Reden: unresolved_private is precies de bak waar echte particuliere huurders in zitten. Maar salary en benefits van onbekende privérekeningen openen creëert false positives op structural income. Rent heeft 9 strenge evidence checks — die zijn voldoende om false positives te voorkomen. Salary/benefits missen die strengheid voor privépersonen.

---

## 4. Pipeline: 3 stappen

### Stap 1: Hard exclusions (blokkeren income classifiers)

```
ALS own_account                → is_intern = True, STOP
ALS household_related_party    → is_intern = True, STOP
ALS eigen_fi_domein withdrawal → sparen_beleggen, STOP
ALS belastingteruggave         → Belastingteruggave (apart, niet structural), STOP
ALS refund (keyword/match)     → Terugbetaling/Refund (apart), STOP
ALS lening-inflow              → Lening (apart), STOP
ALS interne transfer           → is_intern = True, STOP
```

Alles wat hier doorheen komt is een positieve inflow die door de income classifiers mag.

### Stap 2: Income classifiers (bepalen WAT het is)

Alle resterende positieve inflows gaan door de classifiers, ongeacht party_type:

```
VOOR ELKE ongeclass positieve transactie/groep:
  1. _refund_matcher()     → als LIKELY: Terugbetaling/Refund
  2. _salary_classifier()  → als LIKELY: Netto salaris / DGA-loon
  3. _rent_classifier()    → als LIKELY: Huurinkomsten
  4. _uncertainty_gate()   → subcategoriseer als uncertain
```

De classifiers gebruiken party_type als context maar NIET als blokkade:
- `_rent_classifier()` heeft E10 check: `party_type != household_related_party`. Maar household is al in Stap 1 eruit gefilterd, dus E10 is een safety net.
- `_salary_classifier()` weegt `business_related_party` mee als bevestigend signaal
- Geen classifier wordt geblokkeerd door `unresolved_private_counterparty`

### Stap 3: Page-1 whitelist (bepaalt WAT structural income is)

```
_STRUCTURAL_INCOME_WHITELIST = {
    'Netto salaris', 'DGA-loon/Managementfee', 'Huurinkomsten',
    'UWV/Uitkeringen', 'Kinderbijslag/Kindregelingen', 'Toeslagen',
    'Freelance/Opdrachten', 'Pensioen/AOW', 'Studiefinanciering',
    'Overheid overig',
}

Alleen LIKELY-classificaties in de whitelist tellen als structural income.
Alles wat UNCERTAIN is of niet in de whitelist: apart, niet structural.
```

---

## 5. Signalen (ongewijzigd t.o.v. v1.1)

### Signaalklassen

| Klasse | Signalen | Rol |
|--------|----------|-----|
| **STERK** | S2 (multi-IBAN linking), S4 (bidirectioneel), S8 (eigen FI domein), S10 (merchant mapping) | Mag household of own_account of external triggeren |
| **MIDDEL** | S6 (cross-account), S9 (rechtsvorm) | Bevestigt in combinatie; alleen = onvoldoende voor household |
| **ZWAK** | S1 (achternaam), S3 (adres-hint), S5 (transfer-achtig), S7 (geen eco. relatie) | Mag NOOIT de classificatie dragen |

### Kernregel
`household_related_party` vereist minimaal één STERK signaal (S2 of S4). Geen enkele combinatie van zwakke en/of middel signalen mag household triggeren.

---

## 6. Party Resolution beslislogica

### Fase 1: Definitieve labels

```
ALS IBAN ∈ eigen_rekeningen           → own_account              (DEFINITIEF)
ALS S8: eigen financieel domein       → own_account              (DEFINITIEF)
ALS S10: merchant mapping match       → known_external_counterparty (DEFINITIEF)
```

### Fase 2: Household (vereist STERK signaal)

```
ALS S4: bidirectioneel patroon        → household_related_party
ALS S2: multi-IBAN linking            → household_related_party
```

Expliciet NIET household: S1 alleen, S1+S5, S1+S6, S1+S7, S5+S6, S1+S5+S6+S7.

### Fase 3: Business-related

```
ALS S9 + S1 + extra bewijs            → business_related_party
   (extra = S4/S6/salaris-keyword)

ALS S9 + S1 (zonder extra)            → known_external_counterparty
   (behandel als normale werkgever)
```

### Fase 4: Default

```
ALS S9 (rechtsvorm, geen naamoverlap) → known_external_counterparty
ANDERS                                 → unresolved_private_counterparty
```

---

## 7. Beslismatrix (herzien)

### Party Resolution matrix

| Signaalcombinatie | party_type | Reden |
|-------------------|------------|-------|
| IBAN ∈ eigen_rekeningen | `own_account` | Definitief eigen |
| S8 (eigen FI domein) | `own_account` | Definitief eigen FI |
| S10 (merchant mapping) | `known_external` | Definitief bekende partij |
| S4 (bidirectioneel) | `household` | Sterk bewijs |
| S2 (multi-IBAN linking) | `household` | Sterk: afgeleid van bewezen HH-lid |
| S4 + S1 | `household` | Sterk + zwak bevestigt |
| S9 alleen (rechtsvorm) | `known_external` | Organisatie = extern |
| S9 + S1 + keyword | `business_related` | Candidate + bewijs = definitief |
| S9 + S1 + S4/S6 | `business_related` | Candidate + sterk/middel bewijs |
| S9 + S1 (geen extra) | `known_external` | Candidate zonder bewijs = extern |
| S1 alleen | `unresolved_private` | Zwak signaal, niet bewezen |
| S1 + S5 | `unresolved_private` | Twee zwakke signalen |
| S1 + S6 | `unresolved_private` | Zwak + middel |
| S1 + S5 + S6 + S7 | `unresolved_private` | Alle zwak/middel: geen sterk |
| Geen signalen | `unresolved_private` | Onvoldoende informatie |

### Classifier-toegang matrix (de kern)

| party_type | Hard exclusion? | Rent? | Salary? | Benefits? | Structural mogelijk? |
|------------|----------------|-------|---------|-----------|---------------------|
| `own_account` | **JA** → is_intern | — | — | — | Nee |
| `household_related_party` | **JA** → is_intern | — | — | — | Nee |
| `business_related_party` | Nee | Nee | **JA** | Nee | Ja, als LIKELY |
| `known_external_counterparty` | Nee | **JA** | **JA** | **JA** | Ja, als LIKELY |
| `unresolved_private_counterparty` | Nee | **JA** | **JA** | **JA** | Ja, als LIKELY |

---

## 8. Regressietests (herzien)

### 8.1 Unit tests

| Test | Scenario | Verwacht party_type | Verwacht classificatie |
|------|----------|--------------------|-----------------------|
| `test_partner_bidi` | Partner heen-en-weer via één IBAN | `household` (S4) | is_intern — niet in inkomsten |
| `test_partner_2nd_account` | Partner op 2e IBAN, 1e al household via S4 | `household` (S2) | is_intern — niet in inkomsten |
| `test_echte_huurder_geen_overlap` | 12x €2.500 van particulier, geen naamoverlap, stabiel, unidirectioneel | `unresolved_private` | Rent classifier → LIKELY → **Huurinkomsten** (structural) |
| `test_echte_huurder_met_naamoverlap` | 12x €950 "HUUR KAMER" van zelfde achternaam, unidirectioneel | `unresolved_private` (S1=zwak) | Rent classifier → LIKELY → **Huurinkomsten** (structural) |
| `test_naamgenoot_transfer_patroon` | 6x €1.000 ronde bedragen van naamgenoot, geen huur-keyword | `unresolved_private` (S1+S5=zwak) | Rent classifier → evidence checks bepalen: als E3 faalt (instabiel) of E9 absent → UNCERTAIN |
| `test_dga_eigen_bv_keyword` | 12x €8.500 van "Jansen Holding B.V." + "MANAGEMENTFEE" | `business_related` | Salary classifier → DGA-loon (structural) |
| `test_dga_bv_zonder_keyword` | 12x €8.500 van "Jansen Holding B.V." zonder keyword | `known_external` (candidate zonder bewijs) | Salary classifier → Netto salaris (structural) |
| `test_externe_bv` | 12x €4.000 van "De Vries B.V.", geen naamoverlap | `known_external` (S9) | Salary classifier → Netto salaris (structural) |
| `test_merchant` | Betaling van Albert Heijn | `known_external` (S10) | Merchant-classificatie (variabele_kosten) |
| `test_onbekende_particulier` | 1x €500, geen signalen, niet recurring | `unresolved_private` | Rent classifier → E1 faalt (<3 tx) → REJECT → uncertainty gate → uncertain |
| `test_unknown_recurring_geen_evidence` | 10x €800/mnd van onbekende, wisselende bedragen, geen keyword | `unresolved_private` | Rent classifier → E3 faalt (instabiel) → UNCERTAIN → niet structural |

### 8.2 Extra tests (v1.1 aanscherpingen, behouden)

| Test | Scenario | Verwacht |
|------|----------|----------|
| `test_achternaam_transfer_niet_household` | 6x €1.000 rond van naamgenoot | `unresolved_private` → classifiers bepalen (S1+S5 = twee zwakke, geen household) |
| `test_huurder_toevallige_naamoverlap` | 12x €950 "HUUR KAMER" van zelfde achternaam | `unresolved_private` → rent classifier → LIKELY → Huurinkomsten (naamoverlap blokkeert NIET) |
| `test_unknown_recurring_niet_structural` | 10x €800/mnd zonder evidence | `unresolved_private` → rent classifier → UNCERTAIN → niet structural |

### 8.3 Integratie-test: 3 hypothetische huishoudens

#### Huishouden A: DGA-gezin met meerdere rekeningen
- Partner "M. Jansen-Bakker" — ABN (bidirectioneel) + Rabobank (niet bidirectioneel)
- BV: "Jansen Holding B.V." + "MANAGEMENTFEE" → €8.500/mnd
- Huurder: "P. van Dijk" → €1.800/mnd
- Verwacht:
  - Partner ABN → `household` (S4) → is_intern
  - Partner Rabo → `household` (S2) → is_intern
  - BV → `business_related` (S9+S1+keyword) → salary → DGA-loon (structural)
  - Huurder → `unresolved_private` → rent classifier → LIKELY → Huurinkomsten (structural)

#### Huishouden B: Tweeverdieners met studerende kinderen
- Kind 1: "T. de Vries" stuurt €300/mnd terug (eenrichting)
- Kind 2: "S. Smit" stuurt incidenteel €100-500
- Verwacht:
  - Kind 1 → `unresolved_private` (S1=zwak, geen S4) → rent classifier: E4 faalt (mediaan €300, grensgeval), E1 ok → waarschijnlijk UNCERTAIN → niet structural
  - Kind 2 → `unresolved_private` → rent classifier: E1 faalt (<3 tx) → REJECT → uncertain
  - Werkgevers → `known_external` (S9) → salary → structural

#### Huishouden C: Alleenstaande met huurders
- Huurder 1: "F. Bakker" (zelfde achternaam) — 12x €950 "HUUR KAMER"
- Huurder 2: "Y. Özdemir" — 12x €950 "HUUR"
- Verwacht:
  - Huurder 1 → `unresolved_private` (S1=zwak, geen sterk) → rent classifier → LIKELY → **Huurinkomsten** (structural)
  - Huurder 2 → `unresolved_private` → rent classifier → LIKELY → **Huurinkomsten** (structural)
  - Naamoverlap blokkeert NIET — het is een zwak signaal dat de rent classifier niet beïnvloedt

### 8.4 Before/after op Peter Heijen dossier

| Metric | Vóór RPR | Na RPR v1.2 | Reden |
|--------|----------|-------------|-------|
| Esther BICK → party_type | (niet bepaald) | `household` | S2: multi-IBAN linking (ING al household via S4) |
| Esther BICK → classificatie | Huurinkomsten (€26.800) | is_intern | Household → hard exclusion |
| De Monnink → party_type | (niet bepaald) | `unresolved_private` | Geen sterk signaal, geen naamoverlap |
| De Monnink → classificatie | Huurinkomsten (€30.400) | **Huurinkomsten (€30.400)** | Rent classifier → LIKELY (alle 9 checks passen) |
| Structureel inkomen/jaar | €195.780 | ~€168.980 | Alleen Esther eruit (-€26.800) |
| Structureel inkomen/mnd | €16.315 | ~€14.082 | Dichter bij benchmark |

**Dit is het juiste resultaat**: Esther (household) eruit, De Monnink (echte huurder) erin. De rent classifier bepaalt op basis van transactie-evidence, niet op basis van party_type.

---

## 9. Wat NIET verandert

- Data parsing & enrichment
- Merchant mapping (200+ merchants)
- Eigen financieel domein register
- Rent classifier (9 evidence checks) — ongewijzigd, draait nu voor alle niet-excluded party_types
- Salary classifier
- Refund matcher
- Uncertainty gate
- AI narrative laag (alleen negatieve tx)
- PDF-generatie
- Page-1 whitelist
- Email-verzending
- API endpoints

---

## 10. Open vragen — beantwoord door Peter (17 april 2026)

1. **V2 strenger voor unresolved**: Ja, maar via strengere evidence-combinaties, niet simpelweg threshold 3→6.
2. **business_related zonder keyword**: Voor V1 prima als known_external → salary. Economische juistheid > perfect label.
3. **Verschillende achternamen bij partners**: Acceptabele V1-beperking. S4 vangt het op bij bidirectioneel.
4. **Kinderen zonder bidirectioneel**: E4 NIET verlagen. Dat is een apart probleem en mag de rent logic niet vervormen.

### Status: GOEDGEKEURD VOOR IMPLEMENTATIE (v1.3)
