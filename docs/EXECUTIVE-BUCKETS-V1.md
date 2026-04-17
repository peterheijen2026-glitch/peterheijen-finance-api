# Executive Buckets V1 — Page-1 Herontwerp

## Versie 1.0 — April 2026

---

## 1. Probleem met huidige pagina 1

Huidige metrics zijn huishoudboekje-logica:
- Structureel inkomen
- Vaste lasten
- Variabele kosten
- Vermogensopbouw
- **Vrij besteedbaar** (= inkomen - alles)

Dit past niet bij vermogende particulieren / DGA's:
- "Vrij besteedbaar" is misleidend (negatief door vermogensopbouw ≠ probleem)
- Geen onderscheid belasting vs. woonlasten vs. lifestyle
- Geen zicht op vermogensallocatie als bewuste keuze
- Onzekere posten vervuilen de headline

---

## 2. Nieuwe pagina-1 structuur

### Executive metrics (6 blokken)

| # | Bucket | Bron | Wat het bevat |
|---|--------|------|---------------|
| 1 | **Structureel inkomen** | Whitelist inkomsten | Salaris, DGA-loon, Huur, UWV, Kinderbijslag, Toeslagen, Pensioen, Studiefinanciering |
| 2 | **Belastingdruk** | vaste_lasten (belasting-cats) + Inkomstenbelasting | IB/voorlopige aanslag, BTW, Gemeentebelasting/OZB/Waterschap |
| 3 | **Woonlasten** | vaste_lasten (woon-cats) | Hypotheek/Huur, Energie, Water |
| 4 | **Levensstijl** | variabele_kosten + overige vaste_lasten | Alles wat niet belasting, woonlast of vermogen is |
| 5 | **Vermogensopbouw** | sparen_beleggen | Effecten, crowdlending, pensioensparen, crypto, spaar |
| 6 | **Onzeker / buiten beeld** | uncertain cats | Posten die niet hard geclassificeerd zijn |

### Wat ERUIT gaat
- "Vrij besteedbaar" als headline metric → weg
- "Variabele kosten" als apart blok → opgegaan in Levensstijl
- "Vaste lasten" als ongesplitst blok → gesplitst in Belasting + Woon + Levensstijl

### Strategische observaties
Onder de metrics: 3 korte, feitelijke observaties over de financiële structuur.
Gegenereerd door de AI, gebaseerd op de bucket-verhoudingen.

---

## 3. Categorie → Executive Bucket mapping

### Bucket: BELASTINGDRUK
```
'Inkomstenbelasting/Voorlopige aanslag' → belasting
'BTW/Omzetbelasting'                    → belasting
'Overige belastingen'                   → belasting
'Gemeentebelasting/OZB/Waterschapsbelasting' → belasting
```

### Bucket: WOONLASTEN
```
'Hypotheek/Huur'        → woonlasten
'Energie'               → woonlasten
'Water'                 → woonlasten
```

### Bucket: LEVENSSTIJL
Alles in vaste_lasten dat NIET belasting en NIET woonlasten is:
```
'Zorgverzekering'              → levensstijl
'Overige verzekeringen'        → levensstijl
'Internet/TV'                  → levensstijl
'Telefoon'                     → levensstijl
'Kinderopvang/BSO/School'      → levensstijl
'Contributie/Lidmaatschap'     → levensstijl
```

Plus ALLE variabele_kosten:
```
'Boodschappen'                 → levensstijl
'Horeca'                       → levensstijl
'Kleding'                      → levensstijl
'Transport/Brandstof'          → levensstijl
'Terugbetaling/Refund'         → levensstijl (negatief, verlaagt totaal)
... etc.
```

### Bucket: STRUCTUREEL INKOMEN
Ongewijzigd — dezelfde _PAGE1_WHITELIST:
```
'Netto salaris'                → structureel_inkomen
'DGA-loon/Managementfee'       → structureel_inkomen
'Huurinkomsten'                → structureel_inkomen
'UWV/Uitkeringen'              → structureel_inkomen
'Kinderbijslag/Kindregelingen' → structureel_inkomen
'Toeslagen'                    → structureel_inkomen
'Freelance/Opdrachten'         → structureel_inkomen
'Pensioen/AOW'                 → structureel_inkomen
'Studiefinanciering'           → structureel_inkomen
'Overheid overig'              → structureel_inkomen
```

### Bucket: VERMOGENSOPBOUW
Alles in sparen_beleggen:
```
'Effectenrekening (terugstorting)' → vermogen
'Crowdlending (terugbetaling)'    → vermogen
'Crypto (terugstorting)'          → vermogen
'Pensioen (terugstorting)'        → vermogen
... etc.
```

### Bucket: ONZEKER / BUITEN BEELD
Alles met "Onzeker" of niet in een whitelist:
```
'Onzeker positief'                      → onzeker
'Onzeker positief (verzekeraar)'        → onzeker
'Onzeker positief (bidirectioneel)'     → onzeker
'Onzeker positief (financiële instelling)' → onzeker
'Belastingteruggave'                    → onzeker (incidenteel, niet structureel)
'Verzekeringsuitkering'                 → onzeker (incidenteel)
'Beleggingsinkomen'                     → onzeker (variabel, niet structureel)
```

---

## 4. Visuele opzet

```
┌─────────────────────────────────────────────────────┐
│  Financieel Overzicht                               │
│  Uw financiële structuur in een oogopslag           │
│  Datum | Rekeningen | Periode                       │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─ Structureel inkomen ──┐  ┌─ Belastingdruk ────┐│
│  │  € 12.998 /mnd         │  │  € 2.163 /mnd      ││
│  └────────────────────────┘  └─────────────────────┘│
│                                                     │
│  ┌─ Woonlasten ───────────┐  ┌─ Levensstijl ──────┐│
│  │  € 1.735 /mnd          │  │  € 7.294 /mnd      ││
│  └────────────────────────┘  └─────────────────────┘│
│                                                     │
│  ┌─ Vermogensopbouw ──────┐  ┌─ Onzeker (buiten ──┐│
│  │  € 6.698 /mnd          │  │  beeld) € 981 /mnd ││
│  └────────────────────────┘  └─────────────────────┘│
│                                                     │
│  ─── gouden lijn ────────────────────────────────── │
│                                                     │
│  Uitleg: "Dit overzicht toont uw gemiddelde..."     │
│                                                     │
│  Disclaimer                                         │
└─────────────────────────────────────────────────────┘
```

---

## 5. Wat NIET verandert
- Pagina 2+ (AI-analyse, bijlagen, maandoverzicht)
- Data-berekeningen in bereken_feiten() / _bereken_rule_based_totalen()
- Whitelist logica
- Email template
- API endpoints

---

## 6. Implementatie
Alleen de `cover_page()` methode in RapportPDF wordt aangepast:
- Nieuwe bucket-mapping in de functie
- 6 blokken ipv 5
- "Vrij besteedbaar" eruit
- Subtitel aanpassen
- Kleuren per bucket
