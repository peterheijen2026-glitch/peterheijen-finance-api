# Executive Buckets V2 — Page-1 Herontwerp (signed-off)

## Versie 2.0 — 17 april 2026

**Status:** Geïmplementeerd en gedeployed
**Sign-off:** Peter Heijen (inhoudelijk goedgekeurd na 5-punts review)

---

## 1. Wijzigingen t.o.v. V1

| # | V1 fout | V2 correctie |
|---|---------|-------------|
| 1 | Vermogensopbouw = abs(alles) | Netto vermogensallocatie (algebraïsch) |
| 2 | "Onzeker" mixt bekende + onbekende posten | Gesplitst: niet-kerninstroom vs review/onzeker |
| 3 | Structureel inkomen te breed (incl. kinderbijslag etc.) | Kerninkomen smaller + aanvullende instroom apart |
| 4 | Levensstijl = grote restbak | Gesplitst: vaste leefkosten vs discretionair |
| 5 | Overige verzekeringen blind gemapt | Review-markering bij >€2.000/jaar (Optie B) |

---

## 2. Nieuwe pagina-1 structuur (Layout A)

### Blok 1 — Kerninkomen (full-width, bovenaan)
| Categorie | Bron |
|-----------|------|
| Netto salaris | inkomsten |
| DGA-loon/Managementfee | inkomsten |
| Huurinkomsten | inkomsten |
| UWV/Uitkeringen | inkomsten |
| Pensioen/AOW | inkomsten |
| Freelance/Opdrachten | inkomsten — **alleen als recurring (≥3 maanden)** |

### + Aanvullende structurele instroom (zichtbaar, niet headline)
| Categorie | Bron |
|-----------|------|
| Kinderbijslag/Kindregelingen | inkomsten |
| Toeslagen | inkomsten |
| Studiefinanciering | inkomsten |
| Overheid overig | inkomsten |
| Freelance/Opdrachten (als niet recurring) | inkomsten |

### Blok 2 — Belastingdruk
| Categorie | Bron |
|-----------|------|
| Inkomstenbelasting/Voorlopige aanslag | vaste_lasten |
| BTW/Omzetbelasting | vaste_lasten |
| Overige belastingen | vaste_lasten |
| Gemeentebelasting/OZB/Waterschapsbelasting | vaste_lasten |

### Blok 3 — Woonlasten
| Categorie | Bron |
|-----------|------|
| Hypotheek/Huur | vaste_lasten |
| Energie | vaste_lasten |
| Water | vaste_lasten |

### Blok 4 — Leefkosten (twee lagen)
**4a — Vaste leefkosten:** alle vaste_lasten NIET in belasting/woonlasten
**4b — Discretionaire uitgaven:** alle variabele_kosten

**Overige verzekeringen (Optie B):** telt mee in leefkosten, krijgt review-markering als >€2.000/jaar.

### Blok 5 — Netto vermogensallocatie
**Formule:** `SUM(sparen_beleggen[*])` — algebraïsch, NIET abs()
- Negatief = netto stortingen naar vermogen → label: "Netto naar vermogen"
- Positief = netto onttrekkingen → label: "Netto uit vermogen"
- UI toont altijd positief bedrag met passend label + "per saldo gestort/onttrokken"

### Blok 6 — Buiten kernbeeld (full-width, onderaan, kleiner)
**6a — Niet-kerninstroom** (bekend maar niet structureel):
- Belastingteruggave
- Verzekeringsuitkering
- Beleggingsinkomen

**6b — Review/onzeker** (handmatige validatie nodig):
- Onzeker positief (alle varianten)

---

## 3. Layout

```
┌─────────────────────────────────────────────────────────┐
│  FINANCIEEL OVERZICHT                                   │
│  Uw financiële structuur in een oogopslag               │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌── Kerninkomen (full width) ─────────────────────────┐│
│  │  € 12.568/mnd                + aanvullend: € 430    ││
│  └─────────────────────────────────────────────────────┘│
│                                                         │
│  ┌── Belastingdruk ────────┐  ┌── Woonlasten ─────────┐│
│  │  € 2.093/mnd            │  │  € 1.830/mnd          ││
│  └─────────────────────────┘  └────────────────────────┘│
│                                                         │
│  ┌── Leefkosten ───────────┐  ┌── Netto naar vermogen ┐│
│  │  € 4.148/mnd            │  │  € 5.759/mnd          ││
│  │  vast / discretionair   │  │  per saldo gestort     ││
│  │  * overige verz. review │  │                        ││
│  └─────────────────────────┘  └────────────────────────┘│
│                                                         │
│  ┌── Buiten kernbeeld (kleiner) ───────────────────────┐│
│  │  Niet-kern: € 547    Review/onzeker: € 956          ││
│  └─────────────────────────────────────────────────────┘│
│                                                         │
│  [uitleg + disclaimer]                                  │
└─────────────────────────────────────────────────────────┘
```

---

## 4. Formules

| Blok | Formule |
|------|---------|
| Kerninkomen | `SUM(abs(inkomsten[cat])) / n_mnd` voor cat IN kerninkomen-set |
| Aanvullend | `SUM(abs(inkomsten[cat])) / n_mnd` voor cat IN aanvullend-set |
| Belastingdruk | `SUM(abs(vaste_lasten[cat])) / n_mnd` voor cat IN belasting-set |
| Woonlasten | `SUM(abs(vaste_lasten[cat])) / n_mnd` voor cat IN woon-set |
| Leefkosten vast | `SUM(abs(vaste_lasten[cat])) / n_mnd` voor overige vaste_lasten |
| Leefkosten disc | `SUM(abs(variabele_kosten[*])) / n_mnd` |
| Netto vermogen | `SUM(sparen_beleggen[*]) / n_mnd` — algebraïsch |
| Niet-kerninstroom | `SUM(abs(inkomsten[cat])) / n_mnd` voor belastingteruggave, verz.uitk., beleggingsinkomen |
| Review/onzeker | `SUM(abs(inkomsten[cat])) / n_mnd` voor alle "Onzeker" categorieën |

---

## 5. Wat NIET verandert
- Pagina 2+ (AI-analyse, bijlagen, maandoverzicht)
- Data-berekeningen in bereken_feiten() / _bereken_rule_based_totalen()
- Whitelist logica in _classify_positive_inflows()
- Email template
- API endpoints

---

*Vervangt: EXECUTIVE-BUCKETS-V1.md*
