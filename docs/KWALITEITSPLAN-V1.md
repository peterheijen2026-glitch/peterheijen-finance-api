# Kwaliteitsplan: Van Gokken naar Reconciliatie

## Status: ONTWERP — ter review door Peter + ChatGPT CEO
## Datum: 17 april 2026

---

## 0. Kernprobleem

Het huidige rapport produceert getallen die **niet herleidbaar** zijn naar de bankdata. De Shortcut.ai Cashflow Overzicht sheet bewijst dat elke euro klopt (Controle = 0 voor alle 12 maanden). Ons rapport wijkt op meerdere plekken af — soms met tienduizenden euro's.

**De enige maatstaf:** Begin saldo + alle transacties = Eind saldo, per maand, per rekening. Als dat niet klopt, mag het rapport niet verzonden worden.

---

## 1. Alle geïdentificeerde fouten (systematisch)

### FOUT A: AI halluceert totaalbedragen (€103.965 verschil)

**Symptoom:** Rapport meldt €267.548 inkomen. Werkelijk (Shortcut.ai): €163.583.

**Root cause:** De AI krijgt ruwe transacties en berekent zelf totalen in de vrije tekst (samenvatting). Die berekening klopt niet omdat:
- BICK-terugstortingen (€65.193) worden als inkomen gezien
- Esther's overboekingen (€19.250) als salaris geteld
- Interne transfers (€12.000) niet uitgefilterd

**Generiek fouttype:** AI berekent getallen in vrije tekst die niet overeenkomen met deterministische berekeningen.

### FOUT B: Household-related-party als salaris (€19.250)

**Symptoom:** "Mw E Kop" overboekingen (€1.750/mnd × 11) staan bij "Netto salaris".

**Root cause:** De salary detector kijkt alleen naar recurring + stabiel bedrag. Er is geen party_type gating: een `household_related_party` (Esther) kan nooit een werkgever zijn.

**Generiek fouttype:** Classifier mist context over tegenpartij-type. Werkt niet alleen voor dit gezin — elk huishouden met regelmatige partneroverboekingen heeft dit probleem.

**Bij 3 andere gezinnen:** (1) Partner stuurt maandelijks huishoudgeld → salary (2) Ouder doet vaste bijdrage aan student → salary (3) Ex-partner betaalt alimentatie → salary i.p.v. correcte categorie.

### FOUT C: Interne verschuivingen vervuilen totalen

**Symptoom:** Transfers tussen eigen rekeningen (€12.000 ING↔ABN, €65.193 BICK terugstortingen) worden niet volledig uitgefilterd.

**Root cause:** De backend filtert "eigen rekeningen" op basis van IBAN-matching, maar:
- Spaarrekening-terugstortingen (BICK) worden soms als inkomsten gezien
- Het is onduidelijk of transfers naar/van de spaarrekening als "sparen" of "neutraal" tellen

**Shortcut.ai oplossing:** Eigen BICK rekeningen staan in SPAREN & BELEGGEN (netto €65.193), interne transfers staan in ONDERLING/NEUTRAAL (€-12.000). Alles is zichtbaar maar telt niet mee in inkomsten/uitgaven.

**Generiek fouttype:** Ontbrekende scheiding tussen economische cashflow en administratieve geldstroom.

### FOUT D: Periodeberekening incorrect

**Symptoom:** April 2026 (12 transacties) telt mee als volledige maand → 13-14 maanden i.p.v. 12.

**Root cause:** `n_mnd` wordt berekend op basis van alle maanden met transacties, zonder minimum-threshold.

**Shortcut.ai oplossing:** Apr-26 staat als aparte kolom maar telt NIET mee in "Totaal 12m" en "Gem p/m (12m)".

**Generiek fouttype:** Onvolledige maanden vervuilen gemiddelden.

### FOUT E: Ontbrekende categorieën (€34.612 onzichtbaar)

**Wat Shortcut.ai WEL heeft en onze backend NIET:**

| Categorie | 12m bedrag | Impact |
|-----------|-----------|--------|
| Cash opname | €5.290 | Verdwijnt in "overig" of wordt niet geteld |
| Privé overboekingen / familie | €8.964 | Wordt niet herkend als aparte stroom |
| Tikkies / betaalverzoeken betaald | €4.111 | Zit in variabele kosten maar niet apart |
| Creditcard afrekening | €4.910 | Dubbeltellingrisico (individuele transacties + afrekening) |
| Bijzondere eenmalige kosten | €16.247 | Cruciaal: dit vertekent maandgemiddelden enorm |
| VvE | €1.645 | Zit bij hypotheek of overig |
| Lening / hypotheekrente | €2.984 | Niet apart van hypotheek |
| Levensverzekering / ORV | €652 | Zit bij overige verzekeringen |
| Mobiliteit vast | €199 | Niet herkend |

**Generiek fouttype:** De categorieboom is te smal. Economisch relevante stromen worden in restcategorieën gedumpt.

### FOUT F: Geen saldo-reconciliatie

**Symptoom:** Er is geen mechanisme dat bewijst dat alle transacties zijn meegeteld en correct gecategoriseerd.

**Root cause:** `bereken_feiten()` berekent wel een saldo-check per rekening (begin + mutaties ≈ eind), maar:
- Dit wordt niet doorgetrokken naar de categorisatie-laag
- Na AI-classificatie wordt niet gecontroleerd of SUM(alle categorieën) = netto mutaties
- Er is geen maandelijkse reconciliatie

**Generiek fouttype:** Ontbrekende end-to-end integriteitscheck.

### FOUT G: AI-jargon in klanttekst

**Symptoom:** Backend-termen als "rule_based_totaal", "interne_verschuivingen" verschijnen in het rapport.

**Root cause:** De AI krijgt technische veldnamen en kopieert die naar de samenvatting.

**Generiek fouttype:** Geen scheiding tussen technische data-laag en presentatie-laag.

---

## 2. Architectuurvoorstel: "Reconciliation-First" (ter bespreking met ChatGPT CEO)

### Principe: GEEN getal in het rapport dat niet herleidbaar is naar een optelsom van individuele transacties.

### 2.1 Nieuwe data-pipeline

```
STAP 1: Parse & Validate
  Bank CSV/Excel → DataFrame
  Per rekening: bereken begin_saldo, eind_saldo
  Validatie: begin + SUM(bedrag) = eind (cent-nauwkeurig)
  → FAIL = stop, rapport niet genereren

STAP 2: Identificeer eigen rekeningen
  Detecteer transfers tussen eigen rekeningen
  Markeer als is_intern = True
  → Deze tellen NIET mee in economische analyse

STAP 3: Classificeer (deterministisch eerst)
  Rule-based: MERCHANT_MAPPING (220+ regels)
  Party resolution: household, employer, etc.
  → Elke transactie krijgt: sectie, categorie, confidence, bron

STAP 4: AI classificeert REST
  Alleen transacties zonder rule-based match
  AI mag NIET zelf totalen berekenen
  AI retourneert: per transactie → sectie + categorie
  
STAP 5: POST-CLASSIFICATIE RECONCILIATIE (NIEUW)
  Per maand, per rekening:
    SUM(alle gecategoriseerde transacties) = netto mutaties
    Als niet: FOUT → identificeer welke transacties missen
  Per sectie:
    SUM(sectie-bedragen) = som van individuele transacties in die sectie
  Totaal-check:
    SUM(inkomsten) + SUM(vaste_lasten) + SUM(variabele) + SUM(sparen) + SUM(onderling) = netto mutaties

STAP 6: Genereer outputs
  6a: Reconciliatie Excel (altijd)
  6b: PDF rapport (met getallen uit stap 5, NIET uit AI-tekst)
  6c: AI-analyse (krijgt BEREKENDE totalen als input, mag niet zelf rekenen)
```

### 2.2 Reconciliatie Excel (output bij elke run)

Exact het format van de Shortcut.ai Cashflow Overzicht:

```
Kolommen: B (labels) | C-N (apr-25 t/m mrt-26) | O (apr-26 apart) | P (Totaal 12m) | Q (Gem p/m)

Rijen:
  Begin saldo totaal
  
  INKOMSTEN
    [alle inkomsten-categorieën]
    Totaal inkomsten
  
  VASTE LASTEN
    [alle vaste-lasten-categorieën]
    Totaal vaste lasten
  
  VARIABELE UITGAVEN
    [alle variabele-categorieën]
    Totaal variabele uitgaven
  
  SPAREN & BELEGGEN
    [alle sparen-categorieën]
    Totaal sparen & beleggen
  
  ONDERLING / NEUTRAAL
    Interne transfers
    Huishouden/partner overboekingen
    Totaal onderling
  
  Netto mutaties
  Berekend eindsaldo
  Werkelijk eindsaldo
  Controle (=0)     ← MUST be zero for every month
  
  SALDO PER REKENING
    [per rekening: begin + eind per maand]
```

### 2.3 Categorieboom uitbreiden

De huidige categorieboom mist economisch relevante categorieën. Voorstel:

**Nieuw in VASTE LASTEN:**
- VvE (apart van Hypotheek)
- Lening/Hypotheekrente (apart)
- Levensverzekering/ORV (apart van overige verzekeringen)
- Mobiliteit vast (wegenbelasting, lease)

**Nieuw in VARIABELE UITGAVEN:**
- Cash opname (pinautomaat)
- Tikkies / betaalverzoeken betaald
- Privéoverboekingen / familie
- Bijzondere eenmalige kosten (>€2.000 individueel OF niet-recurring)

**Nieuw in ONDERLING/NEUTRAAL:**
- Interne transfer tussen zichtrekeningen
- Overboekingen huishouden / partner
- Creditcard afrekening (als onderliggende transacties al apart staan)

### 2.4 AI-analyse krijgt "grondwaarheid"

De AI-prompt voor de samenvatting/analyse krijgt:

```
## BEREKENDE TOTALEN (gebruik DEZE cijfers, reken NIETS zelf)
Totaal inkomsten: €163.583 (12 maanden)
  - Salaris: €44.530
  - DGA-loon: €46.824
  - Huurinkomsten: €30.400
  - UWV: €22.129
  - [etc.]

Totaal vaste lasten: €58.208
  [breakdown]

[etc.]

## INSTRUCTIE
Schrijf een samenvatting. Gebruik ALLEEN de bovenstaande getallen.
Je mag NIET zelf optellen, aftrekken, of percentages berekenen.
Elk getal in je tekst moet letterlijk voorkomen in de bovenstaande lijst.
```

### 2.5 No-send regels (gate)

Het rapport mag NIET verzonden worden als:
1. Controle-rij ≠ 0 voor enige maand (reconciliatie faalt)
2. >5% van het totale bedrag in "Overig" categorieën zit
3. AI-samenvatting bevat getallen die niet in de grondwaarheid staan
4. Onvolledige maand (<15 transacties) telt mee in 12m-totalen

---

## 3. Wat is Claude-domein vs Cursor-domein?

| Onderdeel | Owner | Waarom |
|-----------|-------|--------|
| Reconciliatie Excel template + generation | **Claude** | Frontend/output, openpyxl |
| Post-classificatie reconciliatie check | **Cursor** | Backend logica in pipeline |
| Party_type gating in salary detector | **Cursor** | Backend classifier |
| Categorieboom uitbreiden | **Cursor** | Backend schema |
| AI-prompt aanpassen (grondwaarheid) | **Cursor** | bouw_prompt() in app.py |
| Periode-filtering (onvolledige maanden) | **Cursor** | bereken_feiten() |
| No-send gate | **Cursor** | Pipeline controle |
| PDF page-1 met reconciled getallen | **Claude** | Presentatie-laag |
| Interne transfer detectie verbeteren | **Cursor** | Backend data-laag |
| Onderling/neutraal sectie toevoegen | **Cursor** | Backend schema |

---

## 4. Implementatievolgorde (voorstel)

### Fase 1: Fundament (week 1)
1. **Cursor:** Categorieboom uitbreiden + ONDERLING/NEUTRAAL sectie
2. **Cursor:** Party_type gating (household_related_party ≠ salary)
3. **Cursor:** Periode-filtering (onvolledige maanden excluderen uit totalen)

### Fase 2: Reconciliatie (week 1-2)
4. **Cursor:** Post-classificatie reconciliatie check in pipeline
5. **Claude:** Reconciliatie Excel generator (openpyxl, exact Shortcut.ai format)
6. **Cursor:** Excel output integreren in rapport-pipeline

### Fase 3: AI-kwaliteit (week 2)
7. **Cursor:** AI-prompt aanpassen → grondwaarheid meegeven, niet zelf laten rekenen
8. **Cursor:** No-send gate implementeren
9. **Claude:** PDF page-1 updaten met reconciled getallen

### Fase 4: Validatie (week 2-3)
10. **Claude + Cursor:** End-to-end test met Peter's bankdata
11. **Claude:** Reconciliatie Excel vergelijken met Shortcut.ai output → elke cel moet matchen

---

## 5. Meetbare succescriteria

1. **Controle (=0)** voor alle 12 maanden → saldo-reconciliatie klopt
2. **Totaal inkomsten** verschilt <€100 van Shortcut.ai (€163.583)
3. **Geen AI-hallucinated getallen** in samenvatting
4. **Esther's overboekingen** staan in Onderling/Neutraal, NIET bij Salaris
5. **Cash opnames, Tikkies, etc.** zijn apart zichtbaar
6. **Onvolledige maanden** tellen niet mee in gemiddelden

---

## 6. Wat dit NIET oplost (bewust)

- Individuele transactie-classificatie: sommige transacties zullen anders gecategoriseerd worden dan Shortcut.ai (bv. "Eten, horeca & uitjes" vs onze "Restaurant/Uit eten" + "Café/Drinken"). Dat is OK zolang het TOTAAL per sectie klopt en elke euro ergens terechtkomt.
- 100% match met Shortcut.ai categorieën: onze categorieboom hoeft niet identiek te zijn, maar moet economisch gelijkwaardig zijn.
- AI-interpretatie: de AI mag eigen inzichten geven, maar alleen op basis van correcte getallen.

---

*Dit document is ter bespreking. Peter neemt het mee naar ChatGPT CEO voor architectuurbeslissing.*
