# Decision Engine — Architectuurnotitie

## Versie 1.0 — April 2026

---

## 1. Waarom losse detectors niet genoeg zijn

De huidige codebase bevat zes classificatiefuncties die na elkaar draaien:
`_classificeer_rule_based()` → `_detecteer_vast_inkomen()` → `_detecteer_huurinkomsten()` →
`_bouw_eigen_financieel_domein()` → `_classificeer_inflow_type()` → `_afdwing_iban_consistentie()`.

Elke functie is gebouwd als zelfstandige detector met eigen logica, eigen thresholds, en eigen edge cases. Dat heeft drie structurele problemen:

**Probleem 1: Geen gedeeld besliskader.** Elke detector beslist zelfstandig of iets "huur" of "salaris" of "refund" is. Er is geen overkoepelend framework dat zegt: op basis van welk type bewijs mag welk type conclusie getrokken worden? De median/IQR-fix voor huurdetectie is hier het perfecte voorbeeld — het loste één outlier op, maar het veranderde niets aan de fundamentele vraag: wanneer is bewijs sterk genoeg om een positieve transactie als structureel inkomen te classificeren?

**Probleem 2: Geen principieel onderscheid tussen zekerheid en onzekerheid.** Een transactie met "SALARIS" in de omschrijving en een transactie van een onbekende tegenpartij die toevallig 4x per jaar €2.500 overmaakt worden allebei met `classificatie_bron='rule'` gemarkeerd. Maar het bewijsniveau is fundamenteel anders. De eerste is deterministisch zeker, de tweede is een statistische inschatting.

**Probleem 3: Volgorde-afhankelijkheid zonder expliciete prioriteit.** Als `_detecteer_vast_inkomen()` iets als salaris markeert, ziet `_classificeer_inflow_type()` het niet meer. Dat werkt nu toevallig correct, maar er is geen expliciete prioriteitsarchitectuur die garandeert dat dit bij 10.000 dossiers altijd goed gaat.

De oplossing is niet "detectors slimmer maken". De oplossing is een decision engine met drie expliciete lagen, elk met eigen bevoegdheden en eigen bewijsdrempels.

---

## 2. Het drielaags model

```
┌─────────────────────────────────────────────────────────┐
│  LAAG A — HARD DETERMINISTIC RULES                      │
│  Beslist: zekere classificaties op basis van feiten      │
│  Bewijsdrempel: deterministisch (keyword, IBAN, eigen    │
│  rekening). Geen statistiek, geen patronen.              │
│  Output: definitieve classificatie OF doorschuiven       │
├─────────────────────────────────────────────────────────┤
│  LAAG B — EVIDENCE-BASED CLASSIFIERS                    │
│  Beslist: semi-zekere classificaties op basis van        │
│  meerdere onafhankelijke evidence checks                 │
│  Bewijsdrempel: voldoende bewijs uit transparante        │
│  checks. Elke check is binair (ja/nee), niet gewogen.   │
│  Output: likely / uncertain / reject per inflow type     │
├─────────────────────────────────────────────────────────┤
│  LAAG C — AI NARRATIVE                                  │
│  Beslist: NIETS over economische waarheid                │
│  Taak: classificeer resterende uitgaven, schrijf         │
│  samenvatting, benoem patronen, geef advies              │
│  Input: alleen transacties die Laag A en B niet konden   │
│  classificeren (en die geen positieve inflows zijn)       │
└─────────────────────────────────────────────────────────┘
```

De kernregel: **elke positieve transactie wordt vóór de AI-laag definitief geclassificeerd**. De AI krijgt geen positieve transacties meer te zien. De AI classificeert alleen uitgaven (negatieve transacties) die niet door Laag A zijn gevangen.

---

## 3. LAAG A — Hard Deterministic Rules

### 3.1 Welke beslissingen zijn volledig deterministisch

| Beslissing | Bewijs | Resultaat |
|------------|--------|-----------|
| Eigen rekening → eigen rekening | Tegenrekening ∈ eigen_rekeningen | `intern` — nooit inkomen |
| Eigen rekening in omschrijving | Eigen IBAN/rekeningnummer in tekst | `intern` — nooit inkomen |
| Cross-account match | Zelfde datum, tegengesteld bedrag, 2 eigen rekeningen | `intern` — nooit inkomen |
| Eigen broker → bank | Tegenrekening ∈ eigen_fi_ibans, bedrag > 0 | `asset_withdrawal` — nooit structural income |
| Bank → eigen broker | Tegenrekening ∈ eigen_fi_ibans, bedrag < 0 | `asset_contribution` — niet relevant voor inkomen |
| Keyword "SALARIS" / "LOON" | Exacte keyword in omschrijving, bedrag > €200 | `structural_income / Netto salaris` |
| Keyword "DIVIDEND" / "COUPON" | Exacte keyword, tegenpartij is financiële instelling | `investment_income` — apart van structural income |
| Overheid-afzender | Naam/IBAN matcht OVERHEID_KEYWORDS | Subcategorie op basis van specifiek keyword |
| Belastingdienst positief | Belastingdienst + bedrag > 0 | `Belastingteruggave` — nooit structural income |
| Belastingdienst negatief | Belastingdienst + bedrag < 0 | `tax_payment` — aparte belastingbucket |
| Bekende merchant | Exacte match in MERCHANT_MAPPING | Sectie + categorie uit mapping |
| Refund-keyword + bedragmatch | RETOUR/REFUND/STORNO + eerder tegengesteld bedrag | `refund` — nooit structural income |
| Transfer-keyword | TIKKIE/SPAARREKENING/OVERBOEKING EIGEN | `internal_transfer` — nooit inkomen |
| Lening-keyword | HYPOTHEEK UITBETALING/LENING UITBETALING | `loan_inflow` — nooit structural income |
| Creditcard-afrekening | CREDITCARD/VISA CARD/MASTERCARD + intern | `intern` — nooit inkomen |

### 3.2 Eigen Financieel Domein Register

Het systeem detecteert automatisch welke financiële instellingen de klant gebruikt door te scannen naar uitgaande (negatieve) transacties naar bekende financiële instellingen. Alle IBANs die zo gevonden worden vormen het "eigen financieel domein". Geld dat terugkomt van deze IBANs is per definitie een vermogensmutatie, geen inkomen.

Dit is een deterministische regel: als je geld naar DeGiro stuurt, dan is alles wat van dat IBAN terugkomt een asset_withdrawal. Geen uitzonderingen.

Aanvulling: een hardcoded lijst van bekende IBANs van grote financiële instellingen (DeGiro, IBKR, Binck, etc.) vangt ook gevallen waar de klant alleen ontvangt maar nooit verstuurt (erfenis, account-overdracht).

### 3.3 Merchant Normalisatie en Type Assignment

Elke merchant in MERCHANT_MAPPING krijgt een vaste `merchant_type`:
- `employer` — werkgevers (rechtsvorm B.V., N.V., Stichting, Gemeente)
- `government` — overheidsinstellingen
- `financial_institution` — brokers, banken, verzekeraars, pensioen
- `insurer` — verzekeraars (multi-role: zie 3.4)
- `utility` — nutsvoorzieningen
- `retailer` — winkels, webshops
- `subscription` — abonnementen

Dit type bepaalt welke classificatieregels van toepassing zijn.

### 3.4 Multi-Role Financial Institutions

Partijen als ASR, Nationale-Nederlanden, Aegon, Achmea, en pensioen-/vermogenspartijen zijn multi-role: ze kunnen zowel premies incasseren (kosten) als uitkeringen doen (inkomen of vermogensmutatie).

Regel: **de naam van de instelling alleen is nooit genoeg voor een definitieve classificatie van positieve transacties**.

Voor negatieve transacties (premies, bijdragen) is de merchant-naam wél voldoende — het is altijd een kost.

Voor positieve transacties geldt:
1. Check eerst op expliciete keywords: UITKERING, SCHADEVERGOEDING, PENSIOEN → specifieke subcategorie
2. Check of het IBAN in het eigen financieel domein zit → asset_withdrawal
3. Zonder keyword én zonder FI-domein match → `uncertain_positive (verzekeraar)` — **nooit automatisch inkomen**

Dit is generiek: het geldt voor elke partij die zowel kosten als inkomsten kan genereren. Geen hardcoded ASR-fix, maar een principiële regel gebaseerd op `merchant_type`.

---

## 4. LAAG B — Evidence-Based Classifiers

### 4.1 Principe

Laag B behandelt transacties die Laag A niet kon classificeren maar die wél patronen vertonen waarmee een classificatie mogelijk is. Elke classifier verzamelt onafhankelijke bewijsstukken (evidence checks) en komt tot een driedelige uitkomst:

- **likely** — voldoende bewijs, classificeer
- **uncertain** — onvoldoende bewijs, naar uncertainty gate
- **reject** — bewijs spreekt classificatie tegen, naar andere classifier of uncertainty gate

Elke evidence check is binair (ja/nee) en transparant uitlegbaar. Geen gewogen scores, geen machine learning, geen black box.

### 4.2 Welke classifiers bestaan

| Classifier | Wat het classificeert | Wanneer aangeroepen |
|------------|----------------------|---------------------|
| **Rent Income Classifier** | Recurring positieve inflows van particulieren | Na Laag A, voor alle onbekende positieve transacties |
| **Salary Pattern Classifier** | Recurring positieve inflows van bedrijven zonder keyword | Na Laag A, voor positieve transacties van bedrijven |
| **Refund Matcher** | Positieve inflows die matchen met eerdere kosten | Na Laag A, voor kleine/middelgrote positieve transacties |
| **BV-Privé Classifier** | Overboekingen van eigen B.V. naar privé | Na Laag A, voor positieve transacties van rechtspersonen |

Elk van deze classifiers wordt in Output B volledig uitgewerkt. Hieronder het ontwerp van de Rent Income Classifier als voorbeeld van het framework.

### 4.3 Rent Income Classifier — Evidence Checks

De rent income classifier beoordeelt of een groep positieve transacties van dezelfde tegenpartij huurinkomsten zijn.

**Groepering**: Transacties worden gegroepeerd op genormaliseerde Tegenrekening (IBAN). Als geen IBAN beschikbaar is, op genormaliseerde tegenpartijnaam.

**Evidence checks** (elk binair ja/nee):

| # | Check | Wat het test | Ja-criterium |
|---|-------|-------------|--------------|
| E1 | Recurring counterparty | Komt deze tegenpartij regelmatig voor? | ≥ 3 positieve transacties in 12 maanden |
| E2 | Externality | Is dit een externe partij? | IBAN ∉ eigen_rekeningen EN IBAN ∉ eigen_fi_ibans |
| E3 | Amount stability | Zijn de bedragen stabiel? | IQR < 30% van mediaan (na outlier-filter: bedragen < 25% mediaan uitgesloten) |
| E4 | Minimum amount | Is het bedrag realistisch voor huur? | Mediaan ≥ €300/transactie |
| E5 | Predominantly unidirectional | Stroomt geld overwegend één kant op? | ≤ 15% van alle transacties met deze tegenpartij is negatief |
| E6 | Not an employer | Is dit geen werkgever? | Geen rechtsvorm-markers (B.V., N.V., Stichting, Holding, Gemeente) |
| E7 | Not a known merchant | Is dit geen bekende merchant? | Geen match in MERCHANT_MAPPING |
| E8 | Not a financial institution | Is dit geen financiële instelling? | Geen match in FINANCIELE_INSTELLINGEN_KEYWORDS of VERZEKERAAR_NAMEN |
| E9 | Description hint | Bevat de omschrijving huur-gerelateerde termen? | HUUR, RENT, KAMER, WONING in omschrijving (optioneel: versterkt bewijs) |

**Beslislogica**:

```
VERPLICHTE checks (alle moeten ja zijn):
  E2 (externality)
  E6 (not employer)
  E7 (not merchant)
  E8 (not financial institution)
  
  Als één van deze nee → REJECT (het is geen huur)

BEWIJSCHECKS (minimum aantal moet ja zijn):
  E1 (recurring) — verplicht
  E3 (amount stability) — verplicht
  E4 (minimum amount) — verplicht
  E5 (predominantly unidirectional) — verplicht
  
  Als alle vier ja → LIKELY (classificeer als huurinkomsten)
  Als E1 ja maar E3, E4, of E5 nee → UNCERTAIN

VERSTERKING (optioneel, bevestigt bij twijfel):
  E9 (description hint)
  
  Als 3 van de 4 bewijschecks ja + E9 ja → LIKELY
  Anders → UNCERTAIN
```

**Resultaat per transactie-groep**:
- `likely` → `inkomsten / Huurinkomsten` — telt mee op pagina 1
- `uncertain` → `inkomsten / Onzeker positief` — telt NIET mee op pagina 1
- `reject` → doorschuiven naar andere classifier of uncertainty gate

### 4.4 Salary Pattern Classifier — Evidence Checks

Classificeert positieve transacties van bedrijven die geen expliciete SALARIS/LOON-keyword bevatten.

| # | Check | Ja-criterium |
|---|-------|-------------|
| E1 | Recurring | ≥ 3 positieve transacties in 12 maanden |
| E2 | Externality | IBAN ∉ eigen_rekeningen EN IBAN ∉ eigen_fi_ibans |
| E3 | Amount stability | IQR < 25% van mediaan |
| E4 | Minimum amount | Mediaan ≥ €500/transactie |
| E5 | Predominantly unidirectional | ≤ 10% negatieve transacties |
| E6 | Legal form present | B.V., N.V., Stichting, Gemeente, etc. in naam |
| E7 | Not a financial institution | Geen match in FI-keywords |
| E8 | Not a known merchant | Geen match in MERCHANT_MAPPING (behalve als sectie = 'inkomsten') |

**Beslislogica**:
- E2 + E6 + E7 + E8 verplicht
- E1 + E3 + E4 + E5 alle vier ja → LIKELY als DGA-loon (bij B.V./Holding) of Netto salaris
- 3 van 4 ja → UNCERTAIN
- < 3 ja → REJECT

### 4.5 Refund Matcher

| # | Check | Ja-criterium |
|---|-------|-------------|
| E1 | Amount match | Eerder een negatieve transactie met bedrag binnen ±30% |
| E2 | Same counterparty | Zelfde IBAN of zelfde tegenpartijnaam |
| E3 | Time window | Negatieve transactie was binnen 90 dagen |
| E4 | Refund keyword | RETOUR, REFUND, STORNO, etc. in omschrijving |
| E5 | Known expense merchant | Tegenpartij staat in MERCHANT_MAPPING als variabele_kosten/vaste_lasten |

**Beslislogica**:
- E4 (keyword) → LIKELY (ook zonder andere checks)
- E1 + E2 + E3 alle ja → LIKELY
- E1 + E5 ja (maar geen E2/E3) → UNCERTAIN
- Geen E1 en geen E4 → REJECT

---

## 5. Uncertainty Gate

### 5.1 Definitie

De uncertainty gate is de principiële buffer tussen "we weten het niet" en "we gokken". Elke positieve transactie die niet door Laag A (deterministisch) of Laag B (likely) geclassificeerd is, komt in de uncertainty gate terecht.

### 5.2 Welke inflows komen in de uncertainty gate

| Bron | Waarom |
|------|--------|
| Laag B classifier output = `uncertain` | Onvoldoende bewijs voor classificatie |
| Laag B classifier output = `reject` door alle classifiers | Geen classifier kon het plaatsen |
| Positieve transactie van onbekende tegenpartij, < 3 keer | Te weinig data voor patroonherkenning |
| Positieve transactie van verzekeraar zonder uitkering-keyword | Multi-role instelling, onduidelijke richting |
| Positieve transactie van IBAN dat bidirectioneel is | Zowel inkomend als uitgaand, onduidelijk type |

### 5.3 Subcategorieën binnen uncertain

| Subcategorie | Wanneer |
|--------------|---------|
| `Onzeker positief` | Default — geen verdere informatie beschikbaar |
| `Onzeker positief (bidirectioneel)` | IBAN heeft zowel in- als uitgaande transacties |
| `Onzeker positief (verzekeraar)` | Afzender is bekende verzekeraar maar geen uitkering-keyword |
| `Onzeker positief (financiële instelling)` | Afzender lijkt op FI maar niet in eigen domein |

### 5.4 Wat er met uncertain inflows gebeurt

1. **Pagina 1**: Uncertain inflows tellen NIET mee als structural income. Ze staan apart vermeld als "Niet-geverifieerd positief: €X/jaar".
2. **Detailpagina's**: Uncertain inflows worden wél getoond in het maandoverzicht, met een markering dat ze niet geverifieerd zijn.
3. **Review flags**: Als het totaal aan uncertain inflows > 20% van het totale positieve volume is, wordt een review flag getriggerd in het rapport.
4. **AI narrative**: De AI mag uncertain inflows benoemen in de samenvatting als "ongeïdentificeerde positieve transacties" en kan suggesties doen voor wat het zou kunnen zijn, maar mag ze niet als inkomen presenteren.

### 5.5 Uncertain mag NOOIT

- Meetellen als external structural income op pagina 1
- Door de AI herclassificeerd worden als inkomen
- Zonder menselijke review als "waarschijnlijk salaris/huur" gepresenteerd worden
- Stilzwijgend verdwijnen uit het rapport

---

## 6. Page-1 Whitelist

### 6.1 Principe

Pagina 1 van het rapport toont "Structureel inkomen". Dit getal wordt UITSLUITEND opgebouwd uit een strikte whitelist van gevalideerde categorieën. Alles wat niet op deze whitelist staat, telt niet mee.

### 6.2 Whitelist: wat telt als External Structural Income

| Categorie | Bron (Laag A of B) | Voorwaarde |
|-----------|--------------------|------------|
| Netto salaris | A (keyword) of B (salary classifier = likely) | Altijd |
| DGA-loon/Managementfee | A (keyword) of B (salary classifier = likely + rechtsvorm) | Altijd |
| Huurinkomsten | B (rent classifier = likely) | Alleen als classifier voldoende bewijs heeft |
| UWV/Uitkeringen | A (overheid-keyword) | Altijd |
| Kinderbijslag/Kindregelingen | A (overheid-keyword) | Altijd |
| Toeslagen | A (overheid-keyword) | Altijd |
| Freelance/Opdrachten | B (salary classifier = likely, geen rechtsvorm maar recurring) | Alleen met voldoende bewijs |

### 6.3 Blacklist: wat NOOIT structural income mag zijn

| Type | Waarom |
|------|--------|
| Asset withdrawals | Terugstorting eigen vermogen, geen inkomen |
| Tax refunds | Belastingteruggave is correctie, geen structureel inkomen |
| Refunds | Terugbetaling eerdere kosten, geen inkomen |
| Loan inflows | Geleend geld is geen inkomen |
| Internal transfers | Verschuiving eigen geld |
| Insurance payouts (incidenteel) | Schade-uitkering is eenmalig, niet structureel |
| Uncertain positive inflows | Onvoldoende bewijs |
| Investment income (dividend) | Wordt apart getoond, niet als structural income |

### 6.4 Aparte vermelding op pagina 1

Naast structural income toont pagina 1:
- **Belastingteruggave**: apart bedrag, niet bij structural income
- **Beleggingsinkomen (dividend/rente)**: apart bedrag
- **Niet-geverifieerd positief**: totaal uncertain, met toelichting
- **Vermogensmutaties**: totaal asset withdrawals/contributions (informationeel)

---

## 7. Blockers voor rapportverzending

### 7.1 Harde blockers (rapport wordt NIET gegenereerd)

| Blocker | Criterium | Rationale |
|---------|-----------|-----------|
| Overig inkomen te hoog | "Overig inkomen" > 40% van geverifieerd inkomen (excl. uncertain) | AI heeft te veel onbekende transacties als "Overig inkomen" geclassificeerd — onbetrouwbaar |
| Interne transfer als inkomen | AI classificeert eigen-rekening-IBAN als inkomen/kosten > €100 | Fundamentele fout — vertrouwen in AI-classificatie is onvoldoende |

### 7.2 Waarschuwingen (rapport wordt WEL gegenereerd, met disclaimer)

| Waarschuwing | Criterium | Actie in rapport |
|-------------|-----------|-----------------|
| Hoog percentage overig inkomen | "Overig inkomen" 15-40% van geverifieerd | Disclaimer bij inkomenssectie |
| Groot uncertain bucket | Uncertain > 20% van totaal positief volume | Expliciete vermelding + review-suggestie |
| Hoge AI-afhankelijkheid | > 60% van transacties alleen door AI geclassificeerd | Vermelding in kwaliteitssectie |
| Grote AI-only bedragen | > €5.000/jaar in één categorie, alleen AI | Waarschuwing bij betreffende categorie |

---

## 8. Hoe AI (Laag C) past in het model

### 8.1 Wat AI wél mag

- Uitgaven classificeren die niet door Laag A zijn gevangen (resterende negatieve transacties)
- Samenvatting schrijven van het financiële beeld
- Patronen benoemen (seizoenseffecten, trends, opvallende posten)
- Strategische observaties maken (bespaaradvies, optimalisatiekansen)
- Rapporttoon verzorgen

### 8.2 Wat AI NIET mag

- Positieve transacties classificeren (die zijn al door Laag A/B afgehandeld)
- Beslissen of iets structureel inkomen is
- Uncertain inflows herclassificeren als inkomen
- Asset withdrawals als inkomen presenteren
- De deterministische classificaties van Laag A overschrijven
- De evidence-based classificaties van Laag B overschrijven

### 8.3 Wat AI ontvangt

De AI ontvangt:
1. Alleen NEGATIEVE transacties die niet door Laag A zijn geclassificeerd
2. Een samenvatting van alle Laag A/B classificaties (als context)
3. De berekende feiten (saldo's, totalen, maandoverzichten)
4. Instructies om GEEN positieve transacties te classificeren

---

## 9. Verschil met huidige implementatie

| Aspect | Huidig | Nieuw |
|--------|--------|-------|
| Positieve transacties | Deels door AI geclassificeerd (uncertain bucket vangt rest) | Volledig door Laag A/B, AI ziet ze niet |
| Huurdetectie | Losse heuristiek met median/IQR threshold | Evidence-based classifier met 9 checks en driedelige uitkomst |
| Salaris zonder keyword | Pattern detection (6+ tx, vast bedrag) als losse functie | Evidence-based classifier met expliciete checks |
| Multi-role instellingen | Verzekeraar-namen lijst met keyword-check | Generiek merchant_type systeem: naam alleen is nooit genoeg voor positieve tx |
| Page-1 inkomen | Excludeert _UNCERTAIN_CATS maar includeert alles anders | Strikte whitelist: alleen expliciet gevalideerde categorieën |
| Quality gates | 40% overig-check + interne-transfer-check | Uitgebreid met uncertain-percentage check en AI-dependency check |
| AI scope | Ontvangt alle niet-rule-classified transacties (incl. positief) | Ontvangt alleen negatieve niet-rule-classified transacties |

---

## 10. Samenvatting beslisarchitectuur

```
Transactie binnenkomst
    │
    ▼
┌─────────────────────────────┐
│ Data parsing & enrichment    │
│ (IBAN-extractie, naam,       │
│  eigen rekeningen detectie)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ LAAG A: Deterministic Rules  │
│                              │
│ ► Intern? → intern           │
│ ► Keyword match? → classify  │
│ ► Merchant match? → classify │
│ ► Eigen FI domein? → asset   │
│ ► Overheid? → government     │
│ ► Refund keyword? → refund   │
│ ► Transfer keyword? → intern │
│ ► Lening keyword? → loan     │
└──────────┬──────────────────┘
           │
           │ onbekende positieve tx
           ▼
┌─────────────────────────────┐
│ LAAG B: Evidence Classifiers │
│                              │
│ ► Rent Classifier            │
│   → likely / uncertain /     │
│     reject                   │
│ ► Salary Classifier          │
│   → likely / uncertain /     │
│     reject                   │
│ ► Refund Matcher             │
│   → likely / uncertain /     │
│     reject                   │
│ ► BV-Privé Classifier        │
│   → likely / uncertain /     │
│     reject                   │
└──────────┬──────────────────┘
           │
           │ uncertain + reject
           ▼
┌─────────────────────────────┐
│ UNCERTAINTY GATE             │
│                              │
│ Subcategoriseer:             │
│ ► bidirectioneel             │
│ ► verzekeraar                │
│ ► financiële instelling      │
│ ► onbekend                   │
│                              │
│ → telt NOOIT als structural  │
│   income op pagina 1         │
└──────────┬──────────────────┘
           │
           │ alle positieve tx afgehandeld
           │
           │ onbekende negatieve tx
           ▼
┌─────────────────────────────┐
│ LAAG C: AI Narrative         │
│                              │
│ Classificeert: alleen        │
│ resterende negatieve tx      │
│ Schrijft: samenvatting,      │
│ analyse, advies              │
│                              │
│ Mag NIET: inkomen bepalen,   │
│ uncertain herclassificeren   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ MERGE + QUALITY GATES        │
│                              │
│ Rule totalen (ground truth)  │
│ + AI totalen (uitgaven)      │
│ = merged result              │
│                              │
│ Quality checks:              │
│ ► Overig inkomen < 40%       │
│ ► Geen interne tx als inkomen│
│ ► Uncertain < 20% van pos.   │
│ ► AI dependency < 60%        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ PAGE-1 METRICS               │
│                              │
│ Structureel inkomen =        │
│ ALLEEN whitelist-categorieën │
│                              │
│ Apart vermeld:               │
│ ► Belastingteruggave         │
│ ► Beleggingsinkomen          │
│ ► Niet-geverifieerd positief │
│ ► Vermogensmutaties          │
└─────────────────────────────┘
```

---

## Bijlage A: Volledige lijst harde regels (Laag A)

### A.1 Interne transfers

| Regel | Conditie | Resultaat |
|-------|----------|-----------|
| A1.1 | Tegenrekening ∈ eigen_rekeningen | `intern` |
| A1.2 | Eigen rekeningnummer in omschrijving | `intern` |
| A1.3 | Zelfde datum, tegengesteld bedrag, 2 eigen rekeningen | `intern` |
| A1.4 | CREDITCARD/VISA CARD/MASTERCARD + intern patroon | `intern` |
| A1.5 | Keyword TIKKIE/BETAALVERZOEK/SPAARREKENING | `intern` |
| A1.6 | Keyword OVERBOEKING EIGEN/EIGEN REKENING | `intern` |

### A.2 Overheid

| Regel | Conditie | Resultaat |
|-------|----------|-----------|
| A2.1 | BELASTINGDIENST + bedrag > 0 | `inkomsten / Belastingteruggave` |
| A2.2 | BELASTINGDIENST + bedrag < 0 + IB-keyword | `vaste_lasten / Inkomstenbelasting` |
| A2.3 | BELASTINGDIENST + bedrag < 0 + OB-keyword | `vaste_lasten / BTW/Omzetbelasting` |
| A2.4 | UWV + bedrag > 0 | `inkomsten / UWV/Uitkeringen` |
| A2.5 | SVB/KINDERBIJSLAG + bedrag > 0 | `inkomsten / Kinderbijslag` |
| A2.6 | ZORGTOESLAG/HUURTOESLAG + bedrag > 0 | `inkomsten / Toeslagen` |
| A2.7 | DUO + bedrag > 0 | `inkomsten / Studiefinanciering` |
| A2.8 | GEMEENTE + bedrag < 0 | `vaste_lasten / Gemeentebelasting` |
| A2.9 | CJIB + bedrag < 0 | `vaste_lasten / Boetes/CJIB` |

### A.3 Financieel domein

| Regel | Conditie | Resultaat |
|-------|----------|-----------|
| A3.1 | Tegenrekening ∈ eigen_fi_ibans + bedrag > 0 + DIVIDEND-keyword | `inkomsten / Beleggingsinkomen` |
| A3.2 | Tegenrekening ∈ eigen_fi_ibans + bedrag > 0 (geen dividend) | `sparen_beleggen / [subcategorie]` |
| A3.3 | Tegenrekening ∈ eigen_fi_ibans + bedrag < 0 | `sparen_beleggen / [subcategorie]` |
| A3.4 | FI-keyword in naam + bedrag > 0 + DIVIDEND-keyword | `inkomsten / Beleggingsinkomen` |
| A3.5 | FI-keyword in naam + bedrag > 0 (geen dividend) | `sparen_beleggen / [subcategorie]` |

### A.4 Merchants

| Regel | Conditie | Resultaat |
|-------|----------|-----------|
| A4.1 | Match in MERCHANT_MAPPING | Sectie + categorie uit mapping |
| A4.2 | Match + bedrag > 0 + sectie = sparen_beleggen | `sparen_beleggen / [terugstorting]` |

### A.5 Inkomen (keyword)

| Regel | Conditie | Resultaat |
|-------|----------|-----------|
| A5.1 | SALARIS/LOON in omschrijving + bedrag > €200 | `inkomsten / Netto salaris` |
| A5.2 | PENSIOEN/AOW in omschrijving + bedrag > 0 | `inkomsten / Pensioen/AOW` |

### A.6 Leningen

| Regel | Conditie | Resultaat |
|-------|----------|-----------|
| A6.1 | HYPOTHEEK UITBETALING/LENING keyword + bedrag > 0 | `sparen_beleggen / Lening (uitbetaling)` — nooit inkomen |

### A.7 Refunds (keyword-based)

| Regel | Conditie | Resultaat |
|-------|----------|-----------|
| A7.1 | RETOUR/REFUND/STORNO keyword + bedrag > 0 | `variabele_kosten / Terugbetaling/Refund` |

---

## Bijlage B: Definitie "External Structural Income"

External structural income is inkomen dat:
1. Van buiten het huishouden komt (niet intern)
2. Structureel is (recurring, niet eenmalig)
3. Geen vermogensmutatie is (niet asset withdrawal, niet refund, niet lening)
4. Geen belastingcorrectie is (niet belastingteruggave)
5. Met voldoende zekerheid geclassificeerd is (niet uncertain)

Dit is het getal dat op pagina 1 staat en dat de klant als "mijn inkomen" herkent.
