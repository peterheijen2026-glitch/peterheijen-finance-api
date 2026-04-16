"""
peterheijen.com — Finance API v1
=================================
Simpel, werkend, robuust.

Upload CSV/XLSX → Python rekent → Claude categoriseert & analyseert → JSON rapport
"""

import os
import io
import json
import uuid
import logging
import base64
import httpx
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fpdf import FPDF
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="PeterHeijen Finance API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In productie: alleen je eigen domein
    allow_methods=["POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# STAP 1: DATA INLEZEN
# ---------------------------------------------------------------------------

def lees_transacties(inhoud: bytes, bestandsnaam: str) -> pd.DataFrame:
    if bestandsnaam.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(io.BytesIO(inhoud))
    elif bestandsnaam.endswith('.csv'):
        for sep in ['\t', ';', ',']:
            try:
                df = pd.read_csv(io.BytesIO(inhoud), sep=sep, encoding='utf-8')
                if len(df.columns) > 3:
                    break
            except Exception:
                continue
        else:
            raise ValueError("Kan CSV niet parsen")
    else:
        raise ValueError(f"Onbekend bestandstype: {bestandsnaam}")

    verwacht = ['Rekeningnummer', 'Transactiedatum', 'Transactiebedrag', 'Omschrijving']
    ontbreekt = [k for k in verwacht if k not in df.columns]
    if ontbreekt:
        raise ValueError(f"Kolommen ontbreken: {ontbreekt}. Gevonden: {list(df.columns)}")

    df['datum'] = pd.to_datetime(df['Transactiedatum'], format='%Y%m%d')
    df['maand'] = df['datum'].dt.to_period('M')
    df['bedrag'] = df['Transactiebedrag'].astype(float)

    return df


# ---------------------------------------------------------------------------
# STAP 2: DETERMINISTISCH REKENEN
# ---------------------------------------------------------------------------

def bereken_feiten(df: pd.DataFrame) -> dict:
    resultaat = {}
    for rekening in sorted(df['Rekeningnummer'].unique()):
        rdf = df[df['Rekeningnummer'] == rekening].sort_values('datum')

        eerste_begin = float(rdf.iloc[0]['Beginsaldo'])
        laatste_eind = float(rdf.iloc[-1]['Eindsaldo'])
        totaal_mutaties = round(float(rdf['bedrag'].sum()), 2)
        berekend_eind = round(eerste_begin + totaal_mutaties, 2)

        maanden = {}
        for maand, mdf in rdf.groupby('maand'):
            maanden[str(maand)] = {
                'inkomsten': round(float(mdf[mdf['bedrag'] > 0]['bedrag'].sum()), 2),
                'uitgaven': round(float(mdf[mdf['bedrag'] < 0]['bedrag'].sum()), 2),
                'netto': round(float(mdf['bedrag'].sum()), 2),
                'transacties': len(mdf),
            }

        resultaat[str(rekening)] = {
            'periode': {
                'van': rdf['datum'].min().strftime('%Y-%m-%d'),
                'tot': rdf['datum'].max().strftime('%Y-%m-%d'),
            },
            'saldo': {
                'beginsaldo': eerste_begin,
                'eindsaldo': laatste_eind,
                'mutaties': totaal_mutaties,
                'berekend_eind': berekend_eind,
                'klopt': abs(berekend_eind - laatste_eind) < 0.02,
            },
            'totalen': {
                'inkomsten': round(float(rdf[rdf['bedrag'] > 0]['bedrag'].sum()), 2),
                'uitgaven': round(float(rdf[rdf['bedrag'] < 0]['bedrag'].sum()), 2),
                'netto': totaal_mutaties,
            },
            'transacties': len(rdf),
            'maanden': maanden,
        }

    return resultaat


def extract_naam(omschr: str) -> str:
    if pd.isna(omschr):
        return 'Onbekend'
    omschr = str(omschr)
    for marker in ['Naam: ', 'Naam:']:
        if marker in omschr:
            naam = omschr.split(marker)[1].split('Omschrijving:')[0].split('IBAN:')[0].strip()
            return naam[:60]
    if 'BEA' in omschr:
        delen = omschr.split(',')
        if len(delen) >= 2:
            return delen[1].strip().split('PAS')[0].strip()[:60]
    return omschr[:60]


def bereken_top(df: pd.DataFrame, n: int = 15) -> dict:
    resultaat = {}
    for rekening in sorted(df['Rekeningnummer'].unique()):
        rdf = df[df['Rekeningnummer'] == rekening].copy()
        rdf['tegenpartij'] = rdf['Omschrijving'].apply(extract_naam)

        top_uit = (rdf[rdf['bedrag'] < 0]
                   .groupby('tegenpartij')['bedrag']
                   .agg(['sum', 'count'])
                   .sort_values('sum')
                   .head(n))

        top_in = (rdf[rdf['bedrag'] > 0]
                  .groupby('tegenpartij')['bedrag']
                  .agg(['sum', 'count'])
                  .sort_values('sum', ascending=False)
                  .head(n))

        resultaat[str(rekening)] = {
            'top_uitgaven': [
                {'naam': naam, 'bedrag': round(float(row['sum']), 2), 'aantal': int(row['count'])}
                for naam, row in top_uit.iterrows()
            ],
            'top_inkomsten': [
                {'naam': naam, 'bedrag': round(float(row['sum']), 2), 'aantal': int(row['count'])}
                for naam, row in top_in.iterrows()
            ],
        }

    return resultaat


# ---------------------------------------------------------------------------
# STAP 3: CLAUDE CATEGORISEERT EN ANALYSEERT
# ---------------------------------------------------------------------------

def bouw_prompt(df: pd.DataFrame, feiten: dict, top: dict) -> str:
    regels = []
    for _, row in df.iterrows():
        regels.append(
            f"{row['datum'].strftime('%Y-%m-%d')}|{row['Rekeningnummer']}|"
            f"{row['bedrag']:>10.2f}|{str(row['Omschrijving'])[:100]}"
        )

    return f"""Je bent een financieel analist voor vermogende particulieren en DGA's in Nederland.
Hieronder staan {len(df)} banktransacties.

## REGELS
1. Categoriseer ELKE transactie in precies één categorie uit onderstaande lijst.
   Gebruik EXACT deze categorienamen (niet afwijken, niet samenvoegen, niet verzinnen).
   Als een transactie nergens past, gebruik dan de "Overig" variant van de juiste sectie.
   BELANGRIJK: "Overig" categorieën mogen MAXIMAAL 5% van het totaalbedrag per sectie bevatten.
   Als er veel in "Overig" dreigt te belanden, kies dan de best passende bestaande categorie.

2. INKOMSTEN (10 categorieën):
   - Netto salaris (loon van werkgever of eigen BV)
   - UWV/Uitkeringen (WW, WIA, Ziektewet, bijstand)
   - DGA-loon/Managementfee (vanuit eigen BV)
   - Huurinkomsten (ontvangen huur van huurders)
   - Toeslagen (zorgtoeslag, huurtoeslag, kindgebonden budget)
   - Belastingteruggave (teruggave IB, BTW, voorlopige aanslag)
   - Kinderbijslag/Kindregelingen
   - Freelance/Opdrachten (losse inkomsten, facturen)
   - Beleggingsinkomen (dividend, rente, uitkeringen)
   - Overig inkomen

3. VASTE LASTEN (20 categorieën):
   - Hypotheek/Huur
   - Energie (gas, elektra, warmte)
   - Water
   - Gemeentebelasting/OZB/Waterschapsbelasting
   - Zorgverzekering (basis + aanvullend)
   - Inkomstenbelasting/Voorlopige aanslag
   - BTW/Omzetbelasting
   - Overige belastingen (erfbelasting, schenkbelasting)
   - Autoverzekering
   - Woonverzekering/Inboedel
   - Overige verzekeringen (reis, aansprakelijkheid, uitvaart)
   - Internet/TV (Ziggo, KPN, glasvezel)
   - Mobiele telefonie
   - Streaming/Digitaal (Netflix, Spotify, Disney+, iCloud)
   - Overige abonnementen (krant, tijdschrift, software)
   - Kinderopvang/BSO/School
   - Contributie/Lidmaatschap (sport, vereniging)
   - Donaties/Goede doelen
   - Bankkosten
   - Overige vaste lasten

4. VARIABELE KOSTEN (30 categorieën):
   - Boodschappen/Supermarkt (Albert Heijn, Jumbo, Lidl etc.)
   - Drogist (Etos, Kruidvat)
   - Restaurant/Uit eten
   - Café/Drinken
   - Afhaal/Bezorging (Thuisbezorgd, Uber Eats)
   - Benzine/Diesel/Laden
   - OV/Trein (NS, OV-chipkaart)
   - Parkeren
   - Taxi/Uber
   - Auto-onderhoud/APK
   - Kleding
   - Schoenen
   - Huisarts/Tandarts/Specialist
   - Apotheek/Medicijnen
   - Ziekenhuiskosten/Eigen risico
   - Fysiotherapie/Alternatief
   - Brillen/Lenzen
   - Huishoudelijke artikelen
   - Meubels/Inrichting
   - Tuin/Buiten
   - Onderhoud woning/Klussen
   - Elektronica/Gadgets (bol.com, Coolblue, Amazon)
   - Boeken/Media
   - Sport/Fitness
   - Uitjes/Attracties/Bioscoop
   - Vakantie/Reizen (accommodatie, vluchten, activiteiten)
   - Cadeaus
   - School/Studie/Cursussen
   - Huisdieren
   - Overig variabel

5. SPAREN & BELEGGEN (10 categorieën):
   - Effectenrekening (Saxo, DeGiro, IBKR)
   - Crowdlending (Mintos, Lendahand, PeerBerry)
   - Pensioenopbouw (Brand New Day, lijfrente)
   - Kindersparen
   - Spaarrekening
   - Crypto
   - Vastgoedinvestering
   - Beleggingsfonds/ETF
   - Levensverzekering/Kapitaalverzekering
   - Overig sparen/beleggen

6. INTERNE VERSCHUIVINGEN:
   - Overboekingen eigen rekeningen (tussen eigen privé-, ondernemers-, spaar- en beleggingsrekeningen)

## BELASTINGDIENST — BETALINGSKENMERKEN HERKENNEN
Nederlandse belastingbetalingen bevatten een betalingskenmerk in de omschrijving.
Gebruik deze kenmerken om het TYPE belasting te bepalen:
- "IB" of "Inkomstenbelasting" of "Inkomstenbel" of "voorlopige aanslag IB" → Inkomstenbelasting/Voorlopige aanslag
- "OB" of "Omzetbelasting" of "BTW" → BTW/Omzetbelasting
- "MRB" of "Motorrijtuigenbelasting" of "wegenbelasting" → Overige belastingen
- "ZVW" of "Zorgverzekeringswet" of "bijdrage Zvw" → Zorgverzekering
- "Toeslagen" of "zorgtoeslag" of "huurtoeslag" of "kindgebonden" → Toeslagen (als INKOMSTEN)
- "WOZ" of "OZB" of "gemeentelijke belasting" of "waterschapsbelasting" → Gemeentebelasting/OZB/Waterschapsbelasting
- "Erfbelasting" of "schenkbelasting" → Overige belastingen
- Belastingdienst TERUGGAVE (positief bedrag) → Belastingteruggave (als INKOMSTEN)
- Belastingdienst BETALING (negatief bedrag) → juiste belastingcategorie hierboven

## CATEGORISATIE-HINTS VOOR DEZE DATA
- Sevi B.V. / ENGELCKE B.V. → DGA-loon/Managementfee
- UWV → UWV/Uitkeringen
- DHR M J C DE MONNINK → Huurinkomsten
- Saxo Bank → Effectenrekening
- Mintos Marketplace → Crowdlending
- Brand New Day → Pensioenopbouw
- bol.com / Coolblue / Amazon → Elektronica/Gadgets (tenzij duidelijk anders)
- Albert Heijn / Jumbo / Lidl / Plus / Dirk → Boodschappen/Supermarkt
- Etos / Kruidvat → Drogist
- Ziggo / KPN / T-Mobile → Internet/TV of Mobiele telefonie (op basis van bedrag/context)
- CZ Groep / Zilveren Kruis / Menzis → Zorgverzekering
- Frank Energie / Vattenfall / Eneco / Essent / Budget Energie → Energie
- Vitens / Brabant Water / PWN / Dunea → Water
- Netflix / Spotify / Disney / Apple / iCloud / YouTube Premium → Streaming/Digitaal
- NS / OV-chipkaart / Connexxion / Arriva → OV/Trein
- Shell / BP / TotalEnergies / Tango / Tinq → Benzine/Diesel/Laden
- BEA/GEA transacties bij restaurants/eetgelegenheden → Restaurant/Uit eten
- BEA/GEA transacties bij tankstations → Benzine/Diesel/Laden
- BEA/GEA transacties bij supermarkten → Boodschappen/Supermarkt
- BEA/GEA transacties bij kledingwinkels (H&M, Zara, C&A, Primark) → Kleding
- Overboekingen tussen eigen NL-rekeningen (zelfde naam) → Interne verschuivingen
- GIVT / KWF / Partij voor de Dieren / Rode Kruis / Oxfam → Donaties/Goede doelen
- Thuisbezorgd / Uber Eats / Deliveroo → Afhaal/Bezorging
- Uber / Bolt (taxi) → Taxi/Uber
- Booking.com / Airbnb / Transavia / KLM / Ryanair → Vakantie/Reizen
- Action / HEMA / IKEA huishoudelijk → Huishoudelijke artikelen
- IKEA meubels/inrichting → Meubels/Inrichting
- Apotheek / BENU → Apotheek/Medicijnen

## BELANGRIJKE PRINCIPES
- De TOTALEN hieronder zijn wiskundig berekend en 100% correct. Gebruik deze cijfers, reken NIETS zelf.
- "Overig" categorieën mogen MAXIMAAL 3% van het totaalbedrag per sectie bevatten. Als er veel in "Overig" dreigt te belanden, zoek dan HARDER naar een passende categorie.
- Wees SPECIFIEK: gebruik je kennis van Nederlandse bedrijfsnamen, winkelketens en dienstverleners.
- Bij twijfel tussen twee categorieën: kies de meest specifieke.
- Online aankopen (bol.com etc.) zijn NIET automatisch "Overig" — categoriseer op basis van wat er waarschijnlijk gekocht is.
- Elke transactie met een herkenbare tegenpartij MOET in een specifieke categorie, NOOIT in "Overig".
- Bekijk het bedrag: kleine bedragen bij een onbekende tegenpartij passen vaak bij Boodschappen, Huishoudelijke artikelen, of Café. Grotere bedragen bij onbekenden passen vaak bij Onderhoud woning, Meubels, of Vakantie.

## CORRECTE TOTALEN
{json.dumps(feiten, indent=2, ensure_ascii=False)}

## TOP TEGENPARTIJEN
{json.dumps(top, indent=2, ensure_ascii=False)}

## TRANSACTIES (datum|rekening|bedrag|omschrijving)
{chr(10).join(regels)}

## OUTPUT FORMAT
Antwoord ALLEEN in valid JSON:
{{
  "maandoverzicht": {{
    "<rekening>": {{
      "<YYYY-MM>": {{
        "inkomsten": {{"<categorie>": {{"bedrag": 0.00, "aantal": 0}}}},
        "vaste_lasten": {{"<categorie>": {{"bedrag": 0.00, "aantal": 0}}}},
        "variabele_kosten": {{"<categorie>": {{"bedrag": 0.00, "aantal": 0}}}},
        "sparen_beleggen": {{"<categorie>": {{"bedrag": 0.00, "aantal": 0}}}},
        "interne_verschuivingen": {{"bedrag": 0.00, "aantal": 0}}
      }}
    }}
  }},
  "jaartotalen": {{
    "<rekening>": {{
      "inkomsten": {{"<categorie>": 0.00}},
      "vaste_lasten": {{"<categorie>": 0.00}},
      "variabele_kosten": {{"<categorie>": 0.00}},
      "sparen_beleggen": {{"<categorie>": 0.00}},
      "interne_verschuivingen": 0.00
    }}
  }},
  "analyse": {{
    "samenvatting": "3-4 alinea's: schets het complete financiële beeld. Noem concrete bedragen, vergelijk maanden, signaleer trends. Schrijf als een ervaren financieel adviseur die een vermogende particulier adviseert.",
    "sterke_punten": ["Noem 3-5 sterke punten met concrete bedragen, bv: 'Stabiel DGA-inkomen van €X per maand'"],
    "aandachtspunten": ["Noem 3-5 aandachtspunten met concrete bedragen en vergelijkingen, bv: 'Variabele kosten stegen van €X in mei naar €Y in juni'"],
    "aanbevelingen": ["Geef 3-5 concrete, uitvoerbare aanbevelingen specifiek voor deze persoon, niet generiek"]
  }}
}}"""


def vraag_claude(prompt: str) -> dict:
    from anthropic import Anthropic

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY niet geconfigureerd")

    client = Anthropic(api_key=api_key)
    model = os.environ.get('CLAUDE_MODEL', 'claude-sonnet-4-6')

    logger.info(f"Claude aanroepen ({model}), prompt: {len(prompt)} tekens")

    response = client.messages.create(
        model=model,
        max_tokens=32000,
        messages=[{"role": "user", "content": prompt}],
    )

    tekst = response.content[0].text
    tokens_in = response.usage.input_tokens
    tokens_out = response.usage.output_tokens
    logger.info(f"Claude klaar: {tokens_in} in, {tokens_out} out")

    # Parse JSON
    if '```json' in tekst:
        tekst = tekst.split('```json')[1].split('```')[0]
    elif '```' in tekst:
        tekst = tekst.split('```')[1].split('```')[0]

    try:
        return {
            'data': json.loads(tekst),
            'tokens': {'input': tokens_in, 'output': tokens_out},
            'model': model,
        }
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        return {
            'data': None,
            'raw': tekst[:2000],
            'error': str(e),
            'tokens': {'input': tokens_in, 'output': tokens_out},
            'model': model,
        }


# ---------------------------------------------------------------------------
# STAP 4: PDF RAPPORT GENEREREN
# ---------------------------------------------------------------------------

# V2 Kleuren
INK = (26, 26, 46)
INK_SOFT = (61, 61, 92)
ACCENT = (31, 92, 139)
GOLD = (201, 168, 76)
WHITE = (255, 255, 255)
SURFACE = (247, 246, 242)
GREEN = (39, 174, 96)
RED = (192, 57, 43)
BORDER = (221, 217, 208)

SEC_COLORS = {
    'inkomsten': (26, 107, 60),
    'vaste_lasten': (139, 69, 19),
    'variabele_kosten': (74, 85, 104),
    'sparen_beleggen': (31, 92, 139),
    'interne_verschuivingen': (107, 91, 115),
}

SEC_LABELS = {
    'inkomsten': 'INKOMSTEN',
    'vaste_lasten': 'VASTE LASTEN',
    'variabele_kosten': 'VARIABELE KOSTEN',
    'sparen_beleggen': 'SPAREN & BELEGGEN',
    'interne_verschuivingen': 'INTERNE VERSCHUIVINGEN',
}

MAAND_NAMEN = {
    '01': 'jan', '02': 'feb', '03': 'mrt', '04': 'apr',
    '05': 'mei', '06': 'jun', '07': 'jul', '08': 'aug',
    '09': 'sep', '10': 'okt', '11': 'nov', '12': 'dec',
}


def eur(n: float) -> str:
    """Format getal als Euro bedrag."""
    if abs(n) < 0.01:
        return ''
    return f"\u20ac {n:,.0f}".replace(',', '.')


class RapportPDF(FPDF):
    """Premium financieel rapport PDF met website-huisstijl.

    Fonts (zelfde als peterheijen.com):
      - Playfair Display: koppen en titels (serif, premium uitstraling)
      - Source Serif 4: lopende tekst en analyse (leesbaar, warm)
      - Inter: tabellen, data, kleine labels (helder, professioneel)

    Alle fonts worden meegeleverd als TTF — geen afhankelijkheid van systeemfonts.
    """

    # Font aliassen voor makkelijk gebruik door de class heen
    HEADING = 'Playfair'
    BODY = 'SourceSerif'
    DATA = 'Inter'

    def __init__(self):
        super().__init__('P', 'mm', 'A4')
        self.set_auto_page_break(auto=True, margin=20)

        # Premium fonts laden — meegeleverd in fonts/ naast app.py
        fonts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts')

        # Playfair Display (headings)
        self.add_font('Playfair', '', os.path.join(fonts_dir, 'PlayfairDisplay-Regular.ttf'))
        self.add_font('Playfair', 'B', os.path.join(fonts_dir, 'PlayfairDisplay-Bold.ttf'))
        self.add_font('Playfair', 'I', os.path.join(fonts_dir, 'PlayfairDisplay-Italic.ttf'))

        # Source Serif 4 (body text)
        self.add_font('SourceSerif', '', os.path.join(fonts_dir, 'SourceSerif4-Regular.ttf'))
        self.add_font('SourceSerif', 'B', os.path.join(fonts_dir, 'SourceSerif4-SemiBold.ttf'))
        self.add_font('SourceSerif', 'I', os.path.join(fonts_dir, 'SourceSerif4-Italic.ttf'))

        # Inter (data / tabellen / labels)
        self.add_font('Inter', '', os.path.join(fonts_dir, 'Inter-Regular.ttf'))
        self.add_font('Inter', 'B', os.path.join(fonts_dir, 'Inter-Bold.ttf'))

    def header(self):
        if self.page_no() > 1:
            self.set_fill_color(*INK)
            self.rect(0, 0, 210, 14, 'F')
            self.set_font(self.DATA, 'B', 8)
            self.set_text_color(*WHITE)
            self.set_xy(10, 4)
            self.cell(0, 6, 'PeterHeijen.com  |  Financieel Rapport', 0, 0, 'L')
            self.set_font(self.DATA, '', 7)
            self.set_xy(10, 4)
            self.cell(190, 6, f'Pagina {self.page_no()}', 0, 0, 'R')
            self.set_y(18)
        else:
            self.set_y(10)

    def footer(self):
        self.set_y(-15)
        self.set_font(self.DATA, '', 7)
        self.set_text_color(*INK_SOFT)
        self.cell(0, 10, 'Dit rapport is gegenereerd door PeterHeijen.com  |  Vertrouwelijk', 0, 0, 'C')

    def cover_page(self, feiten: dict, rapport_datum: str):
        """Pagina 1: Cover met samenvatting — premium uitstraling."""
        # Donkere header
        self.set_fill_color(*INK)
        self.rect(0, 0, 210, 80, 'F')

        # Gouden lijn
        self.set_draw_color(*GOLD)
        self.set_line_width(0.8)
        self.line(15, 72, 55, 72)

        # Titel — Playfair Display voor premium serif look
        self.set_font(self.HEADING, '', 28)
        self.set_text_color(*WHITE)
        self.set_xy(15, 22)
        self.cell(0, 12, 'Financieel Rapport', 0, 1, 'L')

        # Subtitel — Source Serif voor warme leesbaarheid
        self.set_font(self.BODY, '', 12)
        self.set_text_color(200, 200, 210)
        self.set_xy(15, 38)
        self.cell(0, 7, 'Persoonlijk overzicht van uw inkomsten, uitgaven en vermogen', 0, 1, 'L')

        # Datum
        self.set_font(self.DATA, '', 9)
        self.set_text_color(150, 150, 170)
        self.set_xy(15, 52)
        self.cell(0, 6, f'Gegenereerd op {rapport_datum}', 0, 1, 'L')

        # Rekening info
        self.set_xy(15, 58)
        rek_list = list(feiten.keys())
        self.cell(0, 6, f'{len(rek_list)} rekening(en) geanalyseerd', 0, 1, 'L')

        # Quick stats onder de header
        self.set_y(88)
        self.set_text_color(*INK)

        for rek, info in feiten.items():
            self.set_fill_color(*SURFACE)
            self.rect(15, self.get_y(), 180, 26, 'F')
            self.set_draw_color(*BORDER)
            self.rect(15, self.get_y(), 180, 26, 'D')

            y = self.get_y() + 3
            self.set_font(self.DATA, 'B', 9)
            self.set_text_color(*INK)
            self.set_xy(20, y)
            self.cell(50, 5, f'Rekening {rek}', 0, 0, 'L')

            self.set_font(self.DATA, '', 8)
            self.set_text_color(*INK_SOFT)
            self.set_xy(20, y + 7)
            self.cell(40, 5, f'{info["periode"]["van"]} t/m {info["periode"]["tot"]}', 0, 0, 'L')

            col_x = [85, 115, 150]
            labels = ['Inkomsten', 'Uitgaven', 'Netto']
            values = [info['totalen']['inkomsten'], info['totalen']['uitgaven'], info['totalen']['netto']]
            colors = [GREEN, RED, GREEN if values[2] >= 0 else RED]

            for i in range(3):
                self.set_text_color(*INK_SOFT)
                self.set_font(self.DATA, '', 7)
                self.set_xy(col_x[i], y)
                self.cell(30, 5, labels[i], 0, 0, 'C')
                self.set_text_color(*colors[i])
                self.set_font(self.DATA, 'B', 9)
                self.set_xy(col_x[i], y + 7)
                self.cell(30, 5, eur(values[i]), 0, 0, 'C')

            self.set_y(self.get_y() + 30)

    def analyse_page(self, analyse: dict):
        """Pagina met AI-analyse: samenvatting, sterke punten, etc."""
        self.add_page()

        # Sectie header — Playfair voor premium titels
        self._section_title('Analyse & Inzichten')

        # Samenvatting
        if analyse.get('samenvatting'):
            self.set_font(self.HEADING, '', 12)
            self.set_text_color(*ACCENT)
            self.cell(0, 7, 'Samenvatting', 0, 1, 'L')
            self.set_font(self.BODY, '', 10)
            self.set_text_color(*INK)
            self.multi_cell(180, 5.5, analyse['samenvatting'])
            self.ln(6)

        # Sterke punten
        if analyse.get('sterke_punten'):
            self._bullet_section('Sterke punten', analyse['sterke_punten'], GREEN)

        # Aandachtspunten
        if analyse.get('aandachtspunten'):
            self._bullet_section('Aandachtspunten', analyse['aandachtspunten'], RED)

        # Aanbevelingen
        if analyse.get('aanbevelingen'):
            self._bullet_section('Aanbevelingen', analyse['aanbevelingen'], ACCENT)

    def _section_title(self, title: str):
        self.set_font(self.HEADING, '', 16)
        self.set_text_color(*INK)
        self.cell(0, 10, title, 0, 1, 'L')
        self.set_draw_color(*GOLD)
        self.set_line_width(0.6)
        self.line(15, self.get_y(), 50, self.get_y())
        self.ln(5)

    def _bullet_section(self, title: str, items: list, color: tuple):
        self.set_font(self.HEADING, '', 11)
        self.set_text_color(*color)
        self.cell(0, 7, title, 0, 1, 'L')
        self.set_font(self.BODY, '', 9.5)
        self.set_text_color(*INK)
        for item in items:
            self.set_x(20)
            self.cell(4, 5, '\u2022', 0, 0, 'L')
            self.multi_cell(170, 5, f'  {item}')
            self.ln(1)
        self.ln(4)

    def maandoverzicht_page(self, maandoverzicht: dict, feiten: dict):
        """Pagina's met maandelijks overzicht per rekening in spreadsheet-stijl."""
        sections_config = [
            ('inkomsten', 'INKOMSTEN'),
            ('vaste_lasten', 'VASTE LASTEN'),
            ('variabele_kosten', 'VARIABELE KOSTEN'),
            ('sparen_beleggen', 'SPAREN & BELEGGEN'),
        ]

        for rek, maanden in maandoverzicht.items():
            self.add_page('L')  # Landscape voor brede tabel
            self._section_title(f'Maandoverzicht — Rekening {rek}')

            months = sorted(maanden.keys())
            if not months:
                continue

            # Verzamel categorieën per sectie
            for sec_key, sec_label in sections_config:
                cats = set()
                for m in months:
                    md = maanden[m].get(sec_key, {})
                    if isinstance(md, dict):
                        for cat, val in md.items():
                            b = val.get('bedrag', 0) if isinstance(val, dict) else (val or 0)
                            if abs(b) > 0.01:
                                cats.add(cat)
                cats = sorted(cats)
                if not cats:
                    continue

                # Check of er ruimte is op de pagina
                needed = (len(cats) + 2) * 5 + 10
                if self.get_y() + needed > 185:
                    self.add_page('L')

                # Sectie header
                color = SEC_COLORS.get(sec_key, INK)
                self.set_fill_color(*color)
                self.set_text_color(*WHITE)
                self.set_font(self.DATA, 'B', 7)

                # Kolom breedte berekenen
                cat_w = 55
                m_w = min((277 - cat_w - 22) / len(months), 22)  # max 22mm per maand
                total_w = cat_w + m_w * len(months)

                # Sectie label
                self.cell(total_w, 5, f'  {sec_label}', 1, 1, 'L', True)

                # Maand headers
                self.set_fill_color(*SURFACE)
                self.set_text_color(*INK_SOFT)
                self.set_font(self.DATA, 'B', 6)
                self.cell(cat_w, 5, '  Categorie', 1, 0, 'L', True)
                for m in months:
                    parts = m.split('-')
                    label = MAAND_NAMEN.get(parts[1], parts[1]) + ' ' + parts[0][2:]
                    self.cell(m_w, 5, label, 1, 0, 'C', True)
                self.ln()

                # Data rijen
                section_totals = [0.0] * len(months)
                self.set_font(self.DATA, '', 7)

                for cat in cats:
                    self.set_text_color(*INK)
                    self.cell(cat_w, 4.5, f'  {cat[:35]}', 0, 0, 'L')

                    for mi, m in enumerate(months):
                        sd = maanden[m].get(sec_key, {})
                        b = 0
                        if cat in sd:
                            b = sd[cat].get('bedrag', 0) if isinstance(sd[cat], dict) else (sd[cat] or 0)
                        section_totals[mi] += b

                        if abs(b) > 0.01:
                            self.set_text_color(*(GREEN if b > 0 else RED))
                            self.cell(m_w, 4.5, eur(b), 0, 0, 'R')
                        else:
                            self.set_text_color(*INK_SOFT)
                            self.cell(m_w, 4.5, '', 0, 0, 'R')
                    self.ln()

                # Subtotaal rij
                self.set_fill_color(*SURFACE)
                self.set_font(self.DATA, 'B', 7)
                self.set_text_color(*INK)
                self.cell(cat_w, 5, f'  Totaal', 'T', 0, 'L', True)
                for mi in range(len(months)):
                    t = section_totals[mi]
                    self.set_text_color(*(GREEN if t > 0 else RED if t < 0 else INK))
                    self.cell(m_w, 5, eur(t), 'T', 0, 'R', True)
                self.ln(7)

    def jaartotalen_page(self, jaartotalen: dict, maandoverzicht: dict):
        """Pagina met jaartotalen per categorie."""
        sections_config = [
            ('inkomsten', 'Inkomsten'),
            ('vaste_lasten', 'Vaste Lasten'),
            ('variabele_kosten', 'Variabele Kosten'),
            ('sparen_beleggen', 'Sparen & Beleggen'),
        ]

        for rek, totalen in jaartotalen.items():
            self.add_page()
            n_maanden = len(maandoverzicht.get(rek, {})) or 12
            self._section_title(f'Jaartotalen — Rekening {rek}')

            for sec_key, sec_label in sections_config:
                data = totalen.get(sec_key)
                if not data or not isinstance(data, dict):
                    continue

                entries = [(cat, b) for cat, b in data.items() if abs(b or 0) > 0.01]
                entries.sort(key=lambda x: abs(x[1]), reverse=True)
                if not entries:
                    continue

                needed = (len(entries) + 2) * 5.5 + 10
                if self.get_y() + needed > 270:
                    self.add_page()

                # Sectie header
                color = SEC_COLORS.get(sec_key, INK)
                self.set_fill_color(*color)
                self.set_text_color(*WHITE)
                self.set_font(self.DATA, 'B', 8)
                self.cell(180, 6, f'  {sec_label}', 0, 1, 'L', True)

                # Kolom headers
                self.set_fill_color(*SURFACE)
                self.set_text_color(*INK_SOFT)
                self.set_font(self.DATA, 'B', 7)
                self.cell(90, 5, '  Categorie', 'B', 0, 'L', True)
                self.cell(45, 5, 'Jaarbedrag', 'B', 0, 'R', True)
                self.cell(45, 5, 'Per maand', 'B', 1, 'R', True)

                # Data
                self.set_font(self.DATA, '', 8)
                section_total = 0
                for cat, bedrag in entries:
                    section_total += bedrag
                    pm = bedrag / n_maanden
                    self.set_text_color(*INK)
                    self.cell(90, 5, f'  {cat}', 0, 0, 'L')
                    self.set_text_color(*(GREEN if bedrag > 0 else RED))
                    self.cell(45, 5, eur(bedrag), 0, 0, 'R')
                    self.set_text_color(*(GREEN if pm > 0 else RED))
                    self.cell(45, 5, eur(pm), 0, 1, 'R')

                # Totaal
                self.set_font(self.DATA, 'B', 8)
                self.set_fill_color(*SURFACE)
                self.set_text_color(*INK)
                self.cell(90, 5.5, '  Totaal', 'T', 0, 'L', True)
                self.set_text_color(*(GREEN if section_total > 0 else RED))
                self.cell(45, 5.5, eur(section_total), 'T', 0, 'R', True)
                pm_total = section_total / n_maanden
                self.set_text_color(*(GREEN if pm_total > 0 else RED))
                self.cell(45, 5.5, eur(pm_total), 'T', 1, 'R', True)
                self.ln(6)


def genereer_pdf(rapport: dict) -> bytes:
    """Genereer een premium PDF rapport."""
    pdf = RapportPDF()

    feiten = rapport.get('feiten', {})
    analyse = rapport.get('analyse', {})
    maandoverzicht = rapport.get('maandoverzicht', {})
    jaartotalen = rapport.get('jaartotalen', {})
    datum = datetime.now().strftime('%d-%m-%Y')

    # Pagina 1: Cover + quick stats
    pdf.add_page()
    pdf.cover_page(feiten, datum)

    # Pagina 2: Analyse & Inzichten
    if analyse and analyse.get('samenvatting'):
        pdf.analyse_page(analyse)

    # Pagina 3+: Maandoverzicht (spreadsheet)
    if maandoverzicht:
        pdf.maandoverzicht_page(maandoverzicht, feiten)

    # Pagina 4+: Jaartotalen
    if jaartotalen:
        pdf.jaartotalen_page(jaartotalen, maandoverzicht)

    return pdf.output()


# ---------------------------------------------------------------------------
# STAP 5: EMAIL VERSTUREN VIA RESEND
# ---------------------------------------------------------------------------

def verstuur_rapport_email(email: str, pdf_bytes: bytes, report_id: str):
    """Verstuur het PDF rapport per email via Resend.

    Retourneert True bij succes, False bij falen.
    Logt altijd de volledige Resend-response voor diagnose.
    """
    resend_key = os.environ.get('RESEND_API_KEY')
    if not resend_key:
        logger.error("RESEND_API_KEY ontbreekt in environment variables — email KAN NIET worden verstuurd")
        return False

    # Valideer dat de key er uitziet als een geldige Resend key
    if not resend_key.startswith('re_'):
        logger.error(f"RESEND_API_KEY lijkt ongeldig (begint niet met 're_') — controleer Railway variables")
        return False

    pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
    logger.info(f"PDF bijlage: {len(pdf_bytes)} bytes, base64: {len(pdf_base64)} chars")

    payload = {
        "from": "PeterHeijen.com <rapport@peterheijen.com>",
        "to": [email],
        "subject": f"Uw Financieel Rapport \u2014 PeterHeijen.com",
        "html": f"""
        <div style="font-family:Georgia,serif;max-width:600px;margin:0 auto;color:#1a1a2e">
            <div style="background:#1a1a2e;padding:30px;text-align:center">
                <h1 style="color:#fff;font-size:22px;margin:0">Peter<span style="color:#c9a84c">Heijen</span>.com</h1>
            </div>
            <div style="padding:30px;background:#f7f6f2">
                <h2 style="color:#1a1a2e;font-size:20px">Uw financieel rapport is klaar</h2>
                <p style="color:#3d3d5c;line-height:1.7">
                    Bijgevoegd vindt u uw persoonlijke financi\u00eble analyse.
                    Het rapport bevat een overzicht van uw inkomsten en uitgaven,
                    gecategoriseerd per maand, met concrete inzichten en aanbevelingen.
                </p>
                <p style="color:#3d3d5c;line-height:1.7">
                    Rapport ID: <strong>{report_id}</strong>
                </p>
                <hr style="border:none;border-top:1px solid #ddd9d0;margin:20px 0">
                <p style="color:#3d3d5c;font-size:13px">
                    Heeft u vragen over uw rapport? Neem contact op via
                    <a href="mailto:info@peterheijen.com" style="color:#1f5c8b">info@peterheijen.com</a>
                </p>
            </div>
            <div style="background:#1a1a2e;padding:15px;text-align:center">
                <p style="color:rgba(255,255,255,0.5);font-size:12px;margin:0">
                    &copy; 2025-2026 PeterHeijen.com | Vertrouwelijk
                </p>
            </div>
        </div>
        """,
        "attachments": [
            {
                "filename": f"financieel-rapport-{report_id}.pdf",
                "content": pdf_base64,
                "type": "application/pdf",
            }
        ],
    }

    try:
        resp = httpx.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {resend_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            logger.info(f"Email SUCCESVOL verstuurd naar {email} (Resend status {resp.status_code})")
            return True
        else:
            # Log de volledige error voor diagnose
            logger.error(
                f"Resend FOUT: status={resp.status_code}, "
                f"response={resp.text}, "
                f"from=rapport@peterheijen.com, to={email}"
            )
            # Veelvoorkomende fouten loggen met uitleg
            if resp.status_code == 403:
                logger.error("DIAGNOSE: Domein peterheijen.com is waarschijnlijk niet geverifieerd in Resend. "
                             "Ga naar resend.com/domains en voeg peterheijen.com toe met de juiste DNS records.")
            elif resp.status_code == 422:
                logger.error("DIAGNOSE: Ongeldige email parameters. Check of het 'from' adres correct is "
                             "en het domein is geverifieerd.")
            elif resp.status_code == 429:
                logger.error("DIAGNOSE: Rate limit bereikt. Wacht even en probeer opnieuw.")
            return False
    except httpx.TimeoutException:
        logger.error(f"Resend TIMEOUT na 30 seconden voor email naar {email}")
        return False
    except Exception as e:
        logger.error(f"Email FOUT (onverwacht): {type(e).__name__}: {e}")
        return False


# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------

@app.get("/")
def health():
    return {"status": "ok", "versie": "0.1.0", "service": "peterheijen-finance"}


@app.post("/analyseer")
async def analyseer(bestand: UploadFile):
    """Upload een ABN AMRO CSV/XLSX en ontvang een financieel rapport."""
    report_id = str(uuid.uuid4())[:8]
    logger.info(f"[{report_id}] Start analyse: {bestand.filename}")

    # 1. Inlezen
    try:
        inhoud = await bestand.read()
        df = lees_transacties(inhoud, bestand.filename)
        logger.info(f"[{report_id}] {len(df)} transacties ingelezen")
    except Exception as e:
        logger.error(f"[{report_id}] Inleesfout: {e}")
        raise HTTPException(status_code=400, detail=f"Kan bestand niet lezen: {e}")

    # 2. Deterministisch rekenen
    feiten = bereken_feiten(df)
    top = bereken_top(df)

    for rek, data in feiten.items():
        if not data['saldo']['klopt']:
            logger.warning(f"[{report_id}] Saldo klopt NIET voor rekening {rek}")

    logger.info(f"[{report_id}] Feiten berekend, saldo's gecheckt")

    # 3. Claude categoriseren + analyseren
    claude_result = None
    try:
        prompt = bouw_prompt(df, feiten, top)
        claude_result = vraag_claude(prompt)
        logger.info(f"[{report_id}] Claude analyse compleet")
    except Exception as e:
        logger.error(f"[{report_id}] Claude fout: {e}")
        claude_result = {'data': None, 'error': str(e)}

    # 4. Rapport samenstellen
    rapport = {
        'report_id': report_id,
        'gegenereerd': datetime.now().isoformat(),
        'bestand': bestand.filename,
        'feiten': feiten,
        'top_tegenpartijen': top,
    }

    if claude_result and claude_result.get('data'):
        rapport['maandoverzicht'] = claude_result['data'].get('maandoverzicht', {})
        rapport['jaartotalen'] = claude_result['data'].get('jaartotalen', {})
        rapport['analyse'] = claude_result['data'].get('analyse', {})
        rapport['ai'] = {
            'model': claude_result.get('model'),
            'tokens': claude_result.get('tokens'),
        }
    else:
        rapport['analyse'] = {
            'samenvatting': 'AI-analyse niet beschikbaar. Hieronder de deterministisch berekende cijfers.',
            'ai_fout': claude_result.get('error') if claude_result else 'Geen API key',
        }

    logger.info(f"[{report_id}] Rapport klaar")
    return rapport


@app.post("/rapport")
async def rapport(bestand: UploadFile, email: str = Form(...)):
    """Volledige pipeline: upload → analyse → PDF → email."""
    report_id = str(uuid.uuid4())[:8]
    logger.info(f"[{report_id}] Start rapport pipeline voor {email}")

    # 1. Inlezen
    try:
        inhoud = await bestand.read()
        df = lees_transacties(inhoud, bestand.filename)
        logger.info(f"[{report_id}] {len(df)} transacties ingelezen")
    except Exception as e:
        logger.error(f"[{report_id}] Inleesfout: {e}")
        raise HTTPException(status_code=400, detail=f"Kan bestand niet lezen: {e}")

    # 2. Deterministisch rekenen
    feiten = bereken_feiten(df)
    top = bereken_top(df)
    logger.info(f"[{report_id}] Feiten berekend")

    # 3. Claude categoriseren + analyseren
    try:
        prompt = bouw_prompt(df, feiten, top)
        claude_result = vraag_claude(prompt)
        logger.info(f"[{report_id}] Claude analyse compleet")
    except Exception as e:
        logger.error(f"[{report_id}] Claude fout: {e}")
        raise HTTPException(status_code=500, detail=f"AI-analyse mislukt: {e}")

    if not claude_result.get('data'):
        raise HTTPException(status_code=500, detail=f"AI-analyse ongeldig: {claude_result.get('error', 'onbekend')}")

    # 4. Rapport data samenstellen
    rapport_data = {
        'report_id': report_id,
        'gegenereerd': datetime.now().isoformat(),
        'bestand': bestand.filename,
        'feiten': feiten,
        'maandoverzicht': claude_result['data'].get('maandoverzicht', {}),
        'jaartotalen': claude_result['data'].get('jaartotalen', {}),
        'analyse': claude_result['data'].get('analyse', {}),
    }

    # 5. PDF genereren
    try:
        pdf_bytes = genereer_pdf(rapport_data)
        logger.info(f"[{report_id}] PDF gegenereerd ({len(pdf_bytes)} bytes)")
    except Exception as e:
        logger.error(f"[{report_id}] PDF fout: {e}")
        raise HTTPException(status_code=500, detail=f"PDF generatie mislukt: {e}")

    # 6. Email versturen — MOET slagen, anders is de service incompleet
    email_verstuurd = verstuur_rapport_email(email, pdf_bytes, report_id)

    if not email_verstuurd:
        logger.error(f"[{report_id}] Email naar {email} MISLUKT — klant krijgt geen rapport")
        raise HTTPException(
            status_code=502,
            detail="Uw analyse is gelukt, maar het rapport kon niet per email worden verstuurd. "
                   "Probeer het opnieuw of neem contact op via info@peterheijen.com."
        )

    logger.info(f"[{report_id}] Pipeline compleet — rapport verstuurd naar {email}")

    # Retourneer ALLEEN bevestiging — geen financiële data in de response.
    # Het rapport gaat uitsluitend per email/PDF naar de klant.
    return {
        'report_id': report_id,
        'status': 'compleet',
        'email_verstuurd': True,
        'email': email,
    }


@app.post("/feiten")
async def alleen_feiten(bestand: UploadFile):
    """Alleen deterministisch rekenen, zonder AI. Snel en gratis."""
    report_id = str(uuid.uuid4())[:8]

    try:
        inhoud = await bestand.read()
        df = lees_transacties(inhoud, bestand.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Kan bestand niet lezen: {e}")

    feiten = bereken_feiten(df)
    top = bereken_top(df)

    return {
        'report_id': report_id,
        'gegenereerd': datetime.now().isoformat(),
        'feiten': feiten,
        'top_tegenpartijen': top,
    }
