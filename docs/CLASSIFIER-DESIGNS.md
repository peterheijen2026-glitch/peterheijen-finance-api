# Output B — Classifier Ontwerpen

## Versie 1.0 — April 2026

---

## 1. Rent Income Classifier

### 1.1 Doel

Bepaal of een groep positieve transacties van dezelfde tegenpartij huurinkomsten zijn. De classifier werkt voor alle typen verhuur: reguliere woningverhuur, kamerverhuur, garageverhuur. Het werkt NIET voor platform-verhuur (Airbnb) — dat is een apart classificatieprobleem.

### 1.2 Input

Alle positieve transacties die na Laag A ongeclass zijn, gegroepeerd per tegenpartij.

Groepering:
1. **Primair**: Op genormaliseerde Tegenrekening (IBAN)
2. **Fallback**: Op genormaliseerde tegenpartijnaam (als IBAN ontbreekt)
3. **Laatste fallback**: Op eerste 3 woorden van omschrijving (UPPERCASE)

Elke groep wordt apart beoordeeld.

### 1.3 Evidence Checks

#### E1 — Recurring Counterparty

**Vraag**: Komt deze tegenpartij regelmatig voor?

**Criterium**: ≥ 3 positieve transacties binnen de analyseperiode (typisch 12 maanden).

**Rationale**: Huur is per definitie recurring. Met 3 als minimum pakken we ook kwartaalbetalingen. Onder de 3 is er niet genoeg data om een patroon te herkennen.

**Implementatie**:
```
count = len(groep)
E1 = count >= 3
```

#### E2 — Externality

**Vraag**: Is dit een externe partij (niet eigen geld, niet eigen financiële instelling)?

**Criterium**: IBAN ∉ eigen_rekeningen EN IBAN ∉ eigen_fi_ibans

**Rationale**: Geld van eigen rekeningen of eigen broker is nooit huur. Dit is een exclusion check — als deze faalt, is het per definitie geen huur.

**Implementatie**:
```
iban = genormaliseerde tegenrekening
E2 = (iban not in eigen_rekeningen) and (iban not in eigen_fi_ibans)
```

#### E3 — Amount Stability

**Vraag**: Zijn de bedragen stabiel genoeg voor huur?

**Criterium**: IQR < 30% van mediaan, na outlier-filter.

**Outlier-filter**: Bedragen < 25% van mediaan worden uitgesloten. Dit vangt eenmalige kleine correcties op (borg-verhoging, overschrijving). Na filter moeten minimaal 3 transacties overblijven.

**Rationale**: Huur is een (semi-)vast bedrag. Kleine variatie is normaal (jaarlijkse verhoging), maar grote variatie wijst op iets anders (leningen tussen vrienden, wisselende vergoedingen).

**Implementatie**:
```
bedragen = groep['bedrag']
mediaan = bedragen.median()
kern = bedragen[bedragen >= mediaan * 0.25]
if len(kern) < 3:
    E3 = False
else:
    q1, q3 = kern.quantile(0.25), kern.quantile(0.75)
    iqr = q3 - q1
    variatie = iqr / mediaan if mediaan > 0 else 1.0
    E3 = variatie < 0.30
```

#### E4 — Minimum Amount

**Vraag**: Is het bedrag realistisch voor huur?

**Criterium**: Mediaan ≥ €300 per transactie.

**Rationale**: €300/maand is het laagste realistische huurbedrag in Nederland (kamer in studentenhuis). Bedragen onder €300 zijn waarschijnlijk iets anders (terugbetalingen, bijdragen, Tikkies).

**Implementatie**:
```
E4 = mediaan >= 300
```

#### E5 — Predominantly Unidirectional

**Vraag**: Stroomt het geld overwegend één kant op?

**Criterium**: ≤ 15% van ALLE transacties met deze tegenpartij (positief + negatief) is negatief.

**Rationale**: Bij huur betaalt de huurder aan de verhuurder. Er mag een enkele terugbetaling zijn (borg terug, correctie), maar als er veel geld beide kanten op gaat, is het waarschijnlijk geen huur (eerder een vriend/familielid met leningen).

**Implementatie**:
```
alle_tx = alle transacties met deze IBAN (zowel positief als negatief)
n_negatief = (alle_tx['bedrag'] < 0).sum()
pct_negatief = n_negatief / len(alle_tx)
E5 = pct_negatief <= 0.15
```

#### E6 — Not an Employer

**Vraag**: Is dit geen werkgever?

**Criterium**: Geen rechtsvorm-markers in naam/omschrijving: B.V., N.V., STICHTING, HOLDING, HLDG, VERENIGING, GEMEENTE, MINISTERIE, UNIVERSITEIT.

**Rationale**: Werkgevers betalen salaris, niet huur. Ze zijn al door Laag A of de Salary Classifier afgevangen. Als een tegenpartij een rechtsvorm heeft, is het per definitie geen huurder (particulier).

**Implementatie**:
```
tekst = alle omschrijvingen van groep samengevoegd, UPPERCASE
rechtsvorm_markers = ['B.V.', ' BV ', ' BV,', 'B.V ', ' B.V',
                      'HOLDING', 'HLDG', 'STICHTING', 'VERENIGING',
                      'N.V.', ' NV ', 'GEMEENTE', 'MINISTERIE', 'UNIVERSITEIT']
E6 = not any(m in tekst for m in rechtsvorm_markers)
```

#### E7 — Not a Known Merchant

**Vraag**: Is dit geen bekende merchant?

**Criterium**: Geen match in MERCHANT_MAPPING.

**Rationale**: Als Albert Heijn je elke maand €500 overmaakt, is dat een refund of cashback, niet huur.

**Implementatie**:
```
E7 = not any(zoekterm in tekst for zoekterm, _, _, _ in MERCHANT_MAPPING)
```

#### E8 — Not a Financial Institution

**Vraag**: Is dit geen financiële instelling?

**Criterium**: Geen match in FINANCIELE_INSTELLINGEN_KEYWORDS en geen match in VERZEKERAAR_NAMEN.

**Rationale**: Uitkeringen van verzekeraars/pensioenfondsen zijn geen huur. Ze worden door andere classifiers of de multi-role logica afgehandeld.

**Implementatie**:
```
E8 = (not any(kw in tekst for kw in FINANCIELE_INSTELLINGEN_KEYWORDS)
      and not any(v in tekst for v in VERZEKERAAR_NAMEN))
```

#### E9 — Description Hint (optioneel)

**Vraag**: Bevat de omschrijving huur-gerelateerde termen?

**Criterium**: HUUR, RENT, KAMER, WONING, HUURPENNING, KAMERHUUR in omschrijving.

**Rationale**: Dit is een versterkend signaal. De transactie "DHR M J C DE MONNINK — Huur" scoort hier positief. Het is niet verplicht (veel huurbetalingen bevatten geen huur-keyword), maar als het er staat, verhoogt het het vertrouwen.

**Implementatie**:
```
huur_keywords = ['HUUR', 'RENT', 'KAMER', 'WONING', 'HUURPENNING', 'KAMERHUUR']
E9 = any(kw in tekst for kw in huur_keywords)
```

### 1.4 Beslislogica

```
STAP 1: EXCLUSION CHECKS (verplicht, allemaal ja)
  E2 (externality)          — VERPLICHT
  E6 (not employer)         — VERPLICHT
  E7 (not merchant)         — VERPLICHT
  E8 (not FI)               — VERPLICHT

  Als één van deze NEE → REJECT
  Reden: het is per definitie geen huurder

STAP 2: EVIDENCE CHECKS
  E1 (recurring)            — VERPLICHT
  E3 (amount stability)     — VERPLICHT
  E4 (minimum amount)       — VERPLICHT
  E5 (unidirectional)       — VERPLICHT

  Alle 4 JA → ga naar stap 3
  E1 NEE → REJECT (te weinig data)
  E4 NEE → REJECT (bedrag te laag voor huur)
  E3 NEE of E5 NEE → UNCERTAIN

STAP 3: CLASSIFICATIE
  Alle exclusion checks JA + alle evidence checks JA → LIKELY
  
  LIKELY + E9 JA → extra bevestiging (log als "strong match")
  LIKELY + E9 NEE → gewoon LIKELY (huur-keyword is niet verplicht)

UITZONDERING:
  Als E3 NEE of E5 NEE, maar E9 JA (huur-keyword in omschrijving):
  → heroverweeg: E3 NEE + E9 JA → UNCERTAIN (niet promoten naar LIKELY)
  → E5 NEE + E9 JA → UNCERTAIN (bidirectioneel met huur-keyword is nog steeds verdacht)
  Reden: het keyword alleen is niet genoeg om een zwak patroon te redden
```

### 1.5 Output

| Uitkomst | Classificatie | Telt mee op pagina 1? |
|----------|-------------|---------------------|
| LIKELY | `inkomsten / Huurinkomsten` (confidence 0.85) | JA |
| UNCERTAIN | `inkomsten / Onzeker positief` | NEE |
| REJECT | Doorschuiven naar volgende classifier | N.v.t. |

### 1.6 Explainability

Bij elke LIKELY-classificatie logt het systeem:
```
RENT CLASSIFIER: LIKELY — NL26ABNA0455992339 (DHR M J C DE MONNINK)
  E1 recurring: JA (13 transacties)
  E2 externality: JA
  E3 amount stability: JA (IQR-variatie 1.0%, mediaan €2.500)
  E4 minimum amount: JA (€2.500)
  E5 unidirectional: JA (0% negatief)
  E6 not employer: JA
  E7 not merchant: JA
  E8 not FI: JA
  E9 description hint: JA ("Huur" in omschrijving)
  → 13 transacties, totaal €30.400
```

Bij elke UNCERTAIN-classificatie logt het systeem welke check faalde en waarom.

### 1.7 False Positive Bescherming

De classifier kan fout-positief zijn in deze scenario's:

| Scenario | Risico | Bescherming |
|----------|--------|-------------|
| Alimentatie van ex-partner | Recurring, vast bedrag, particulier | Geen — wordt als huur geclassificeerd. Acceptabel: het IS structureel inkomen, alleen de categorie-naam is fout. Page-1 totaal klopt wél. |
| Terugbetaling lening aan vriend | Recurring, vast bedrag, particulier | E5 (unidirectional) vangt dit als er ook uitgaande betalingen zijn |
| Kostgeld van inwonend kind | Recurring, vast bedrag, particulier | Geen — wordt als huur geclassificeerd. Acceptabel: economisch vergelijkbaar met huur. |
| Structurele schenking van ouder | Recurring, vast bedrag, particulier | Geen — wordt als huur geclassificeerd. Acceptabel voor page-1 totaal (het IS inkomen). |

Alle false positives zijn gevallen van "het IS structureel inkomen, maar met een verkeerde categorie-naam". Het page-1 totaal (structural income) is in al deze gevallen correct. De categorie-naam "Huurinkomsten" is een best-effort label.

---

## 2. Salary Pattern Classifier

### 2.1 Doel

Detecteer salaris/DGA-loon van werkgevers die geen expliciet SALARIS/LOON-keyword gebruiken in hun transactieomschrijving.

### 2.2 Evidence Checks

| # | Check | Criterium |
|---|-------|-----------|
| E1 | Recurring | ≥ 3 positieve transacties in analyseperiode |
| E2 | Externality | IBAN ∉ eigen_rekeningen EN IBAN ∉ eigen_fi_ibans |
| E3 | Amount stability | IQR < 25% van mediaan |
| E4 | Minimum amount | Mediaan ≥ €500 |
| E5 | Predominantly unidirectional | ≤ 10% negatieve transacties |
| E6 | Legal form present | Rechtsvorm-marker in naam (B.V., N.V., Stichting, etc.) |
| E7 | Not a financial institution | Geen match in FI-keywords of VERZEKERAAR_NAMEN |
| E8 | Not a known non-income merchant | Geen match in MERCHANT_MAPPING tenzij sectie = 'inkomsten' |

### 2.3 Beslislogica

```
EXCLUSION (verplicht ja):
  E2, E7, E8

EVIDENCE (verplicht ja voor LIKELY):
  E1 + E3 + E4 + E5 → alle vier ja

CLASSIFICATIE:
  Alle exclusion + alle evidence JA + E6 JA:
    → LIKELY als "DGA-loon/Managementfee" (bij B.V./Holding)
    → LIKELY als "Netto salaris" (bij Stichting/Gemeente/N.V.)
    
  Alle exclusion + alle evidence JA + E6 NEE:
    → LIKELY als "Netto salaris" (maar alleen als ≥ 6 transacties
       en variatie < 15% — striktere drempel zonder rechtsvorm)
    → UNCERTAIN als < 6 transacties of variatie 15-25%

  Exclusion JA maar evidence onvolledig:
    → UNCERTAIN
```

### 2.4 False Positive Bescherming

| Scenario | Bescherming |
|----------|-------------|
| Maandelijkse huurinkomsten van B.V. | E6 (rechtsvorm) → wordt salaris. Economisch correct: het IS inkomen van een bedrijf. |
| Maandelijkse aflossing van een lening door een B.V. | E5 (unidirectional) + geen "LENING"-keyword → UNCERTAIN als bidirectioneel |
| Periodieke dividenduitkering | Kwartaal-frequentie (4x/jaar) + keyword check in Laag A vangt dit eerst |

---

## 3. Refund Matcher

### 3.1 Doel

Identificeer positieve transacties die terugbetalingen zijn van eerdere kosten.

### 3.2 Evidence Checks

| # | Check | Criterium |
|---|-------|-----------|
| E1 | Amount match | Bestaat er een negatieve transactie met bedrag binnen ±30% van deze positieve transactie? |
| E2 | Same counterparty | Zelfde IBAN of zelfde genormaliseerde tegenpartijnaam als de negatieve transactie |
| E3 | Time window | Negatieve transactie was binnen 90 dagen vóór de positieve transactie |
| E4 | Refund keyword | RETOUR, REFUND, STORNO, CREDITNOTA, etc. in omschrijving |
| E5 | Known expense merchant | Tegenpartij staat in MERCHANT_MAPPING als variabele_kosten of vaste_lasten |

### 3.3 Beslislogica

```
E4 JA → LIKELY (keyword is sterk genoeg bewijs op zichzelf)

E4 NEE + E1 JA + E2 JA + E3 JA → LIKELY
  (exacte match: zelfde partij, zelfde bedrag, binnen 90 dagen)

E4 NEE + E1 JA + E5 JA + E3 JA → LIKELY
  (bedragmatch + bekende expense merchant + recent)

E4 NEE + E1 JA maar E2 NEE en E5 NEE → UNCERTAIN
  (alleen een bedragmatch is niet genoeg)

Geen E1 en geen E4 → REJECT
  (geen aanleiding om dit als refund te zien)
```

---

## 4. Positive Inflow Classifier (Orchestrator)

### 4.1 Doel

Dit is de orchestrator die alle Laag B classifiers aanroept voor elke ongeclass positieve transactie, en de uiteindelijke classificatie bepaalt.

### 4.2 Volgorde

```
Voor elke ongeclass positieve transactie-groep:

1. Refund Matcher
   → als LIKELY: classificeer als refund, stop
   → als UNCERTAIN: noteer, ga door
   → als REJECT: ga door

2. Salary Pattern Classifier (alleen als tegenpartij rechtsvorm heeft)
   → als LIKELY: classificeer als salaris/DGA-loon, stop
   → als UNCERTAIN: noteer, ga door
   → als REJECT: ga door

3. Rent Income Classifier (alleen als tegenpartij GEEN rechtsvorm heeft)
   → als LIKELY: classificeer als huurinkomsten, stop
   → als UNCERTAIN: noteer, ga door
   → als REJECT: ga door

4. Multi-Role Institution Check
   → als tegenpartij in VERZEKERAAR_NAMEN en geen uitkering-keyword:
     → UNCERTAIN (verzekeraar)
   → als tegenpartij FI-achtig maar niet in eigen domein:
     → UNCERTAIN (financiële instelling)

5. Bidirectional Check
   → als IBAN heeft zowel in- als uitgaande transacties:
     → UNCERTAIN (bidirectioneel)

6. Default
   → UNCERTAIN (onbekend)
```

### 4.3 Prioriteitsregel

Als meerdere classifiers een mening hebben:
- LIKELY van welke classifier dan ook wint (eerste LIKELY stopt de keten)
- UNCERTAIN + UNCERTAIN = UNCERTAIN (niet stapelen tot LIKELY)
- Een enkele REJECT blokkeert die classifier, maar andere classifiers mogen nog proberen

---

## 5. Uncertainty Gate — Volledig Ontwerp

### 5.1 Wat er binnenkomt

| Type | Bron |
|------|------|
| Classifier output = UNCERTAIN | Rent, Salary, of Refund classifier had onvoldoende bewijs |
| Positieve tx van verzekeraar zonder uitkering-keyword | Multi-role check |
| Positieve tx van FI-achtige partij niet in eigen domein | Multi-role check |
| Bidirectionele IBAN zonder classifier-match | Bidirectional check |
| Positieve tx van onbekende partij, < 3 keer | Te weinig data voor welke classifier dan ook |

### 5.2 Subcategorisering

| Subcategorie | Criterium | Doel |
|--------------|-----------|------|
| `Onzeker positief` | Default, geen nadere info | Catch-all |
| `Onzeker positief (bidirectioneel)` | IBAN met zowel in- als uitgaand verkeer | Onderscheid van zuiver onbekend |
| `Onzeker positief (verzekeraar)` | Tegenpartij ∈ VERZEKERAAR_NAMEN, geen uitkering-keyword | Signaleer mogelijke uitkering |
| `Onzeker positief (financiële instelling)` | Tegenpartij lijkt op FI, niet in eigen domein | Signaleer mogelijke vermogensmutatie |

### 5.3 Gedrag in het rapport

**Pagina 1 (overzicht)**:
- Structureel inkomen: EXCLUSIEF uncertain
- Aparte regel: "Niet-geverifieerd positief: €X/jaar (€Y/mnd)"
- Als totaal uncertain > 20% van totaal positief → review flag

**Detailpagina (maandoverzicht)**:
- Uncertain transacties worden getoond in de inkomsten-sectie
- Met markering: "[niet-geverifieerd]"
- Gegroepeerd per subcategorie

**AI narrative**:
- AI mag uncertain benoemen als: "Er zijn €X aan positieve transacties die niet met zekerheid geclassificeerd konden worden"
- AI mag suggesties doen: "Dit zou huurinkomsten, alimentatie, of een terugbetaling kunnen zijn"
- AI mag NIET zeggen: "Uw inkomen inclusief deze transacties is €Y"

### 5.4 Review Flags

| Flag | Criterium | Actie |
|------|-----------|-------|
| `HIGH_UNCERTAIN_VOLUME` | Uncertain > 20% van totaal positief | Prominent vermeld op pagina 1 |
| `LARGE_UNCERTAIN_SINGLE` | Enkele uncertain transactie > €5.000 | Specifiek benoemd in rapport |
| `RECURRING_UNCERTAIN` | Dezelfde uncertain tegenpartij ≥ 3x | Suggestie: "Dit zou structureel inkomen kunnen zijn — neem contact op als u weet wat dit is" |
| `UNCERTAIN_INSURER` | Verzekeraar in uncertain > €2.000/jaar | "Mogelijke verzekeringsuitkering — controleer of dit een lopende claim is" |

### 5.5 Harde regels

1. Uncertain positive inflows tellen NOOIT mee als external structural income
2. Uncertain mag NOOIT door AI herclassificeerd worden
3. Uncertain verdwijnt NOOIT stilzwijgend uit het rapport
4. De som van uncertain wordt ALTIJD expliciet vermeld
5. Bij uncertain > 50% van totaal positief → quality gate blokkade (rapport niet betrouwbaar)

---

## 6. Page-1 Metrics — Definitief Ontwerp

### 6.1 Structureel Inkomen (whitelist)

```
structural_income = som van:
  ✅ Netto salaris
  ✅ DGA-loon/Managementfee
  ✅ Huurinkomsten (alleen als rent classifier = LIKELY)
  ✅ UWV/Uitkeringen
  ✅ Kinderbijslag/Kindregelingen
  ✅ Toeslagen
  ✅ Freelance/Opdrachten (alleen als salary classifier = LIKELY)
  ✅ Pensioen/AOW
```

### 6.2 Niet meegeteld, apart vermeld

```
apart_vermeld:
  📊 Belastingteruggave: €X/jaar
  📊 Beleggingsinkomen (dividend/rente): €X/jaar
  📊 Verzekeringsuitkering: €X/jaar (als incidenteel → niet structureel)
  ⚠️  Niet-geverifieerd positief: €X/jaar
  📊 Vermogensmutaties: €X/jaar (terugstortingen brokers etc.)
```

### 6.3 Nooit op pagina 1 als inkomen

```
uitgesloten:
  ❌ Asset withdrawals
  ❌ Tax refunds
  ❌ Refunds/terugbetalingen
  ❌ Loan inflows
  ❌ Internal transfers
  ❌ Uncertain positive inflows
  ❌ "Overig inkomen" (AI-geclassificeerd, niet geverifieerd)
```

### 6.4 Berekening maandelijks inkomen

```
maandelijks_inkomen = structural_income / aantal_maanden_in_analyse
```

Dit getal is conservatief. Het onderschat liever dan dat het overschat. Bij twijfel gaat iets naar uncertain, niet naar inkomen.

---

## 7. Regressietests

### 7.1 Test-dossier 1: Peter Heijen (huidig dossier)

| Wat | Verwacht resultaat |
|-----|-------------------|
| OLVG Salaris (12x ~€3.700) | Netto salaris via Laag A (keyword) |
| Esther Heijen-Kop (5x wisselend) | Intern (eigen rekening of huishoudlid) |
| DHR M J C DE MONNINK (13x ~€2.500) | Huurinkomsten via rent classifier (LIKELY) |
| UWV betalingen | UWV/Uitkeringen via Laag A (overheid) |
| Belastingdienst positief | Belastingteruggave via Laag A |
| SVB Kinderbijslag | Kinderbijslag via Laag A |
| DeGiro/broker terugstortingen | Asset withdrawal via eigen FI domein |
| Onbekende kleine positieve tx | Uncertain |
| Page-1 structureel inkomen | ~€13.700/mnd (salaris + huur + UWV + toeslagen + kinderbijslag) |

### 7.2 Synthetische testcases

| Case | Beschrijving | Verwacht |
|------|-------------|----------|
| Kwartaalhuurder | 4x €3.000 per kwartaal van particulier | Rent classifier: LIKELY (E1=4 ≥ 3) |
| Airbnb-verhuurder | 20x wisselend €100-€500 van "AIRBNB" | Rent classifier: E7 REJECT (merchant). Laag A: merchant match → inkomsten. |
| Alimentatie | 12x €800 van ex-partner | Rent classifier: LIKELY (alle checks positief). Acceptabel: IS structureel inkomen. |
| Vriend leent geld | 3x €2.000 ontvangen, 2x €1.500 terugbetaald | Rent classifier: E5 REJECT (>15% negatief). → Uncertain. |
| Erfenis broker | 1x €50.000 van DeGiro (nooit eerder geld gestuurd) | Laag A: IBAN niet in eigen FI domein. Laag B: E1 REJECT (1x). → Uncertain. Correct: eenmalig, niet structureel. |
| Verzekeringsuitkering | 1x €8.000 van Achmea | Laag A: verzekeraar + geen uitkering-keyword → uncertain (verzekeraar). Review flag: LARGE_UNCERTAIN_SINGLE. |
| Verzekeringsuitkering met keyword | 1x €8.000 van Achmea "SCHADEVERGOEDING" | Laag A: verzekeraar + uitkering-keyword → Verzekeringsuitkering. Apart vermeld, niet structureel. |
| DGA zonder keyword | 10x €4.000 van "Heijen Holding B.V." zonder "SALARIS" | Salary classifier: LIKELY als DGA-loon (E6=rechtsvorm, alle checks ja). |
| Pensioen | 12x €1.200 van "ABP PENSIOEN" | Laag A: keyword PENSIOEN → Pensioen/AOW. Structureel inkomen. |
