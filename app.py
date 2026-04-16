"""
peterheijen.com — Finance API v1
=================================
Simpel, werkend, robuust.

Upload CSV/XLSX → Python rekent → Claude categoriseert & analyseert → PDF rapport per email.

ARCHITECTUUR: Async job-model
  POST /rapport  → start achtergrond-job, retourneert direct job_id (< 1 sec)
  GET  /rapport/{job_id}/status → poll voor voortgang
  Geen lange HTTP-requests meer → geen Railway/proxy timeout issues.
"""

import os
import io
import json
import uuid
import logging
import base64
import httpx
import threading
from datetime import datetime

import pandas as pd
from typing import List, Optional
from fastapi import FastAPI, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fpdf import FPDF
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="PeterHeijen Finance API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In productie: alleen je eigen domein
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# JOB STORE — in-memory tracking van achtergrond-analyses
# ---------------------------------------------------------------------------
# Thread-safe dict: {job_id: {status, fase, email, error, ...}}
jobs: dict = {}
jobs_lock = threading.Lock()


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
    # Limiteer tot 2000 transacties om prompt-grootte beheersbaar te houden
    # Bij meer dan 2000: neem een representatieve sample
    if len(df) > 2000:
        logger.warning(f"Bestand bevat {len(df)} transacties — gelimiteerd tot 2000 voor Claude")
        df = df.head(2000)

    regels = []
    for _, row in df.iterrows():
        regels.append(
            f"{row['datum'].strftime('%Y-%m-%d')}|{row['Rekeningnummer']}|"
            f"{row['bedrag']:>10.2f}|{str(row['Omschrijving'])[:80]}"  # 80 ipv 100 chars — scheelt tokens
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
- ENGELCKE B.V. → eigen BV van de gebruiker → DGA-loon/Managementfee
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
- BV-eigendom: zeg ALLEEN "uw BV" als de BV voorkomt in de CATEGORISATIE-HINTS als eigen BV van de gebruiker. Als een BV NIET in de hints staat, beschrijf dan alleen de feitelijke geldstroom zonder eigendom aan te nemen. Een betaling AAN een BV betekent NIET dat de gebruiker eigenaar is.

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
    "samenvatting": "3-4 alinea's. Schrijf als een senior financieel adviseur die een vermogende particulier of DGA informeert — rustig, zakelijk, respectvol. Begin met het totaalbeeld: hoeveel komt er structureel binnen, hoeveel gaat er structureel uit, hoeveel gaat naar vermogensopbouw. Benoem dan de cashflowdynamiek: zijn er grote interne verschuivingen, beleggingstransacties, of seizoenseffecten die het beeld vertekenen? Eindig met de kern: waar zit de financiele kracht en waar de kwetsbaarheid. Noem altijd concrete bedragen. Gebruik NOOIT een oordelende of budgetcoach-achtige toon ('u geeft te veel uit', 'onnodige aankopen'). Gebruik in plaats daarvan neutrale financiele taal ('deze categorie vertoont spreiding', 'discretionair verbruik concentreert zich in...', 'uw liquiditeitsmarge is...').",
    "sterke_punten": ["Noem 3-5 financiele sterktes met concrete bedragen. Schrijf bevestigend en zakelijk, bv: 'Stabiel structureel inkomen van gemiddeld €X/mnd via DGA-loon en huurinkomsten', 'Actieve vermogensopbouw: gemiddeld €X/mnd naar beleggingen en pensioen'"],
    "aandachtspunten": ["Noem 3-5 signalen die aandacht verdienen. Gebruik GEEN oordelende taal. Schrijf als observaties, bv: 'Discretionaire uitgaven vertonen maandelijkse spreiding van €X tot €Y — mogelijke grip-verbetering', 'Liquiditeitsmarge na vaste lasten en vermogensopbouw is beperkt tot ca. €X/mnd'"],
    "aanbevelingen": ["Geef 3-5 concrete, strategische aanbevelingen. Denk op het niveau van financieel advies, niet budgetcoaching. Bv: 'Overweeg een liquiditeitsbuffer van 3-6 maanden vaste lasten (ca. €X) aan te houden alvorens extra beleggingen', 'Consolidatie van beleggingsrekeningen kan beheerkosten en overzicht verbeteren'"],
    "verrassende_inzichten": ["Geef 2-3 patronen of inzichten die een drukke vermogende particulier NIET zelf zou zien maar die een AI wel opvalt. Denk aan: seizoenspatronen in cashflow, verborgen belastingoptimalisatie-mogelijkheden, structurele mismatch tussen inkomen en vermogensopbouw, ongewone correlaties, DGA-loon vs dividendoptimalisatie, effectief belastingtarief dat te hoog lijkt, categorieën die relatief hoog zijn vergeleken met vergelijkbare huishoudens. Dit is de WOW-factor van het rapport — maak het concreet met bedragen en vertel iets dat de klant verrast."]
  }}
}}"""


def vraag_claude(prompt: str) -> dict:
    from anthropic import Anthropic

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY niet geconfigureerd")

    client = Anthropic(api_key=api_key)
    # Gebruik het slimste model voor de beste kwaliteit.
    # Kwaliteit > snelheid. Timeouts zijn ruim genoeg (10 min frontend, 5 min Claude).
    model = os.environ.get('CLAUDE_MODEL', 'claude-opus-4-6')

    logger.info(f"Claude aanroepen ({model}), prompt: {len(prompt)} tekens")

    response = client.messages.create(
        model=model,
        max_tokens=32000,
        timeout=300,  # 5 minuten — Opus heeft meer tijd nodig voor grote bestanden
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
        # Subtiele scheidslijn boven footer
        self.set_draw_color(*BORDER)
        self.set_line_width(0.2)
        self.line(15, self.get_y(), 195 if self.cur_orientation == 'P' else 282, self.get_y())
        self.set_font(self.DATA, '', 7)
        self.set_text_color(*INK_SOFT)
        self.cell(0, 10, 'Dit rapport is gegenereerd door PeterHeijen.com  |  Vertrouwelijk', 0, 0, 'C')

    def cover_page(self, feiten: dict, rapport_datum: str, jaartotalen: dict = None, maandoverzicht: dict = None):
        """Pagina 1: Executive summary op huishoudniveau — premium uitstraling.

        Toont niet per rekening, maar het totaalbeeld:
        structureel inkomen, vaste lasten, vrij besteedbaar, vermogensopbouw.
        """
        # Donkere header
        self.set_fill_color(*INK)
        self.rect(0, 0, 210, 80, 'F')

        # Gouden lijn
        self.set_draw_color(*GOLD)
        self.set_line_width(0.8)
        self.line(15, 72, 55, 72)

        # Titel
        self.set_font(self.HEADING, '', 28)
        self.set_text_color(*WHITE)
        self.set_xy(15, 22)
        self.cell(0, 12, 'Financieel Overzicht', 0, 1, 'L')

        # Subtitel
        self.set_font(self.BODY, '', 12)
        self.set_text_color(200, 200, 210)
        self.set_xy(15, 38)
        self.cell(0, 7, 'Uw persoonlijke financiele situatie in een oogopslag', 0, 1, 'L')

        # Datum + scope
        self.set_font(self.DATA, '', 9)
        self.set_text_color(150, 150, 170)
        self.set_xy(15, 52)
        self.cell(0, 6, f'Gegenereerd op {rapport_datum}', 0, 1, 'L')
        self.set_xy(15, 58)
        periodes = []
        for f in feiten.values():
            periodes.extend([f['periode']['van'], f['periode']['tot']])
        van = min(periodes) if periodes else ''
        tot = max(periodes) if periodes else ''
        self.cell(0, 6, f'{len(feiten)} rekening(en) geanalyseerd  |  {van} t/m {tot}', 0, 1, 'L')

        # --- Executive metrics: gecombineerde cijfers ---
        # Bereken totalen over alle rekeningen (excl. interne verschuivingen)
        n_maanden = 1
        if maandoverzicht:
            all_months = set()
            for rek_m in maandoverzicht.values():
                all_months.update(rek_m.keys())
            n_maanden = max(len(all_months), 1)

        totaal_inkomen = 0
        totaal_vaste = 0
        totaal_variabel = 0
        totaal_sparen = 0

        if jaartotalen:
            for rek, totalen in jaartotalen.items():
                for cat, bedrag in totalen.get('inkomsten', {}).items():
                    totaal_inkomen += abs(bedrag or 0)
                for cat, bedrag in totalen.get('vaste_lasten', {}).items():
                    totaal_vaste += abs(bedrag or 0)
                for cat, bedrag in totalen.get('variabele_kosten', {}).items():
                    totaal_variabel += abs(bedrag or 0)
                for cat, bedrag in totalen.get('sparen_beleggen', {}).items():
                    totaal_sparen += abs(bedrag or 0)

        pm_inkomen = totaal_inkomen / n_maanden
        pm_vaste = totaal_vaste / n_maanden
        pm_variabel = totaal_variabel / n_maanden
        pm_sparen = totaal_sparen / n_maanden
        pm_vrij = pm_inkomen - pm_vaste - pm_variabel - pm_sparen

        # Metrics grid — 5 blokken
        self.set_y(90)
        metrics = [
            ('Structureel inkomen', pm_inkomen, '/mnd', ACCENT),
            ('Vaste lasten', pm_vaste, '/mnd', (139, 69, 19)),
            ('Variabele kosten', pm_variabel, '/mnd', (74, 85, 104)),
            ('Vermogensopbouw', pm_sparen, '/mnd', (26, 107, 60)),
            ('Vrij besteedbaar', pm_vrij, '/mnd', GREEN if pm_vrij >= 0 else RED),
        ]

        box_w = 85
        box_h = 28
        gap = 10
        start_x = 15

        for i, (label, value, suffix, color) in enumerate(metrics):
            col = i % 2
            row = i // 2
            x = start_x + col * (box_w + gap)
            y = 90 + row * (box_h + 6)

            # Achtergrond
            self.set_fill_color(*SURFACE)
            self.set_draw_color(*BORDER)
            self.rect(x, y, box_w, box_h, 'FD')

            # Label
            self.set_font(self.DATA, '', 7.5)
            self.set_text_color(*INK_SOFT)
            self.set_xy(x + 8, y + 5)
            self.cell(box_w - 16, 4, label, 0, 0, 'L')

            # Bedrag
            self.set_font(self.DATA, 'B', 13)
            self.set_text_color(*color)
            self.set_xy(x + 8, y + 13)
            self.cell(box_w - 16, 7, eur(value) + suffix, 0, 0, 'L')

        # Laatste blok (Vrij besteedbaar) centraal als het oneven is
        if len(metrics) % 2 == 1:
            pass  # Al correct geplaatst door grid logica

        # --- Cashflow-reconciliatie uitleg ---
        self.set_y(90 + 3 * (box_h + 6) + 4)
        self.set_draw_color(*GOLD)
        self.set_line_width(0.4)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(4)

        self.set_font(self.BODY, 'I', 8.5)
        self.set_text_color(*INK_SOFT)
        uitleg = (
            f'Dit overzicht toont uw gemiddelde maandelijkse geldstromen over {n_maanden} maanden, '
            f'berekend op basis van {len(feiten)} rekening(en). '
            f'Interne overboekingen tussen uw eigen rekeningen zijn hierin niet meegerekend — '
            f'zij verschuiven geld maar veranderen uw financiele positie niet.'
        )
        self.multi_cell(180, 4.5, uitleg, 0, 'L')

        # Disclaimer onderaan cover
        self.set_y(258)
        self.set_font(self.DATA, '', 6.5)
        self.set_text_color(*INK_SOFT)
        self.cell(180, 4, 'Dit rapport is uitsluitend bedoeld als financieel inzicht en vormt geen financieel advies.', 0, 1, 'C')
        self.cell(180, 4, 'Raadpleeg altijd een erkend financieel adviseur voor persoonlijke beslissingen.', 0, 1, 'C')

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

        # Verrassende inzichten — de WOW-factor
        if analyse.get('verrassende_inzichten'):
            self._insight_section('Wat valt op?', analyse['verrassende_inzichten'])

    def _insight_section(self, title: str, items: list):
        """Premium sectie voor verrassende inzichten — visueel onderscheidend."""
        if self.get_y() + 25 > 270:
            self.add_page()

        # Gouden accent balk
        self.set_fill_color(*GOLD)
        self.rect(15, self.get_y(), 3, 8, 'F')
        self.set_font(self.HEADING, '', 12)
        self.set_text_color(*INK)
        self.set_x(22)
        self.cell(0, 8, title, 0, 1, 'L')
        self.ln(2)

        self.set_font(self.BODY, '', 9.5)
        self.set_text_color(*INK)
        for i, item in enumerate(items):
            est_lines = max(1, len(item) // 80 + 1)
            est_h = est_lines * 5 + 8
            if self.get_y() + est_h > 270:
                self.add_page()

            # Genummerd met gouden cirkel
            y = self.get_y()
            self.set_fill_color(*GOLD)
            self.set_text_color(*WHITE)
            self.set_font(self.DATA, 'B', 8)
            # Kleine gouden cirkel met nummer
            cx = 20
            self.set_xy(cx - 3, y)
            self.cell(8, 5, str(i + 1), 0, 0, 'C')
            # Tekst
            self.set_text_color(*INK)
            self.set_font(self.BODY, '', 9.5)
            self.set_xy(30, y)
            self.multi_cell(165, 5, item)
            self.ln(2)
        self.ln(3)

    def _section_title(self, title: str):
        self.set_font(self.HEADING, '', 16)
        self.set_text_color(*INK)
        self.cell(0, 10, title, 0, 1, 'L')
        self.set_draw_color(*GOLD)
        self.set_line_width(0.5)
        self.line(15, self.get_y(), 60, self.get_y())
        self.ln(6)

    def _bullet_section(self, title: str, items: list, color: tuple):
        # Check of titel + eerste item past, anders nieuwe pagina
        if self.get_y() + 20 > 270:
            self.add_page()
        self.set_font(self.HEADING, '', 11)
        self.set_text_color(*color)
        self.cell(0, 7, title, 0, 1, 'L')
        self.set_font(self.BODY, '', 9.5)
        self.set_text_color(*INK)
        for item in items:
            # Schat hoogte: ~5mm per 80 tekens
            est_lines = max(1, len(item) // 80 + 1)
            est_h = est_lines * 5 + 2
            if self.get_y() + est_h > 270:
                self.add_page()
            self.set_x(20)
            self.cell(4, 5, '\u2022', 0, 0, 'L')
            self.multi_cell(170, 5, f'  {item}')
            self.ln(1)
        self.ln(4)

    def _maand_table_header(self, sec_label, sec_key, months, cat_w, m_w, total_w, continued=False):
        """Render sectie-header + maand-kolomheaders voor maandoverzicht tabel."""
        color = SEC_COLORS.get(sec_key, INK)
        self.set_fill_color(*color)
        self.set_text_color(*WHITE)
        self.set_font(self.DATA, 'B', 7)
        label = f'  {sec_label}' + (' (vervolg)' if continued else '')
        self.cell(total_w, 5, label, 1, 1, 'L', True)

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

    def maandoverzicht_page(self, maandoverzicht: dict, feiten: dict):
        """Pagina's met maandelijks overzicht per rekening in spreadsheet-stijl.

        Slimme page-breaks: als een sectie niet op de huidige pagina past,
        wordt de tabel gesplitst met herhaalde headers op de nieuwe pagina.
        """
        sections_config = [
            ('inkomsten', 'INKOMSTEN'),
            ('vaste_lasten', 'VASTE LASTEN'),
            ('variabele_kosten', 'VARIABELE KOSTEN'),
            ('sparen_beleggen', 'SPAREN & BELEGGEN'),
        ]

        PAGE_BOTTOM = 185  # landscape max Y voor content

        for rek, maanden in maandoverzicht.items():
            self.add_page('L')  # Landscape voor brede tabel
            self._section_title(f'Maandoverzicht — Rekening {rek}')

            months = sorted(maanden.keys())
            if not months:
                continue

            # Kolom breedte — vast voor alle secties op deze rekening
            cat_w = 55
            m_w = min((277 - cat_w - 22) / len(months), 22)
            total_w = cat_w + m_w * len(months)

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

                # Benodigde hoogte: header (10mm) + rijen (4.5mm elk) + totaal (5mm) + spacing (7mm)
                row_h = 4.5
                header_h = 10
                footer_h = 12  # totaal rij + spacing
                needed = header_h + len(cats) * row_h + footer_h

                # Past de hele sectie op de huidige pagina?
                if self.get_y() + needed > PAGE_BOTTOM:
                    # Past het op een NIEUWE pagina?
                    fresh_start = 22  # na page header
                    if fresh_start + needed <= PAGE_BOTTOM:
                        # Hele sectie past op nieuwe pagina
                        self.add_page('L')
                    else:
                        # Te groot — we moeten splitsen. Start op nieuwe pagina
                        # als er minder dan 5 rijen ruimte over is
                        remaining = PAGE_BOTTOM - self.get_y()
                        if remaining < header_h + 5 * row_h + footer_h:
                            self.add_page('L')

                # Render header
                self._maand_table_header(sec_label, sec_key, months, cat_w, m_w, total_w)

                # Data rijen — met slimme page-break
                section_totals = [0.0] * len(months)
                self.set_font(self.DATA, '', 7)
                rows_on_page = 0

                for ci, cat in enumerate(cats):
                    # Check of deze rij + eventuele totaalrij past
                    is_last = (ci == len(cats) - 1)
                    space_needed = row_h + (footer_h if is_last else 0)

                    if self.get_y() + space_needed > PAGE_BOTTOM:
                        # Page break nodig — eerst subtotaal-tussenstand tonen
                        self.set_fill_color(*SURFACE)
                        self.set_font(self.DATA, 'B', 6)
                        self.set_text_color(*INK_SOFT)
                        self.cell(cat_w, 4, '  (vervolg op volgende pagina)', 'T', 0, 'L', True)
                        for mi in range(len(months)):
                            self.cell(m_w, 4, '', 'T', 0, 'R', True)
                        self.ln()

                        # Nieuwe pagina met herhaalde header
                        self.add_page('L')
                        self._maand_table_header(sec_label, sec_key, months, cat_w, m_w, total_w, continued=True)
                        self.set_font(self.DATA, '', 7)
                        rows_on_page = 0

                    # Render data rij — zebra-strepen voor leesbaarheid
                    is_even = (rows_on_page % 2 == 0)
                    if is_even:
                        self.set_fill_color(252, 251, 249)  # Heel subtiel warm grijs
                    else:
                        self.set_fill_color(*WHITE)

                    self.set_text_color(*INK)
                    self.cell(cat_w, row_h, f'  {cat[:35]}', 0, 0, 'L', True)

                    for mi, m in enumerate(months):
                        sd = maanden[m].get(sec_key, {})
                        b = 0
                        if cat in sd:
                            b = sd[cat].get('bedrag', 0) if isinstance(sd[cat], dict) else (sd[cat] or 0)
                        section_totals[mi] += b

                        if abs(b) > 0.01:
                            self.set_text_color(*(GREEN if b > 0 else RED))
                            self.cell(m_w, row_h, eur(b), 0, 0, 'R', True)
                        else:
                            self.set_text_color(*INK_SOFT)
                            self.cell(m_w, row_h, '', 0, 0, 'R', True)
                    self.ln()
                    rows_on_page += 1

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

    def _jaar_table_header(self, sec_label, sec_key, continued=False):
        """Render sectie-header + kolomheaders voor jaartotalen tabel."""
        color = SEC_COLORS.get(sec_key, INK)
        self.set_fill_color(*color)
        self.set_text_color(*WHITE)
        self.set_font(self.DATA, 'B', 8)
        label = f'  {sec_label}' + (' (vervolg)' if continued else '')
        self.cell(180, 6, label, 0, 1, 'L', True)

        self.set_fill_color(*SURFACE)
        self.set_text_color(*INK_SOFT)
        self.set_font(self.DATA, 'B', 7)
        self.cell(90, 5, '  Categorie', 'B', 0, 'L', True)
        self.cell(45, 5, 'Jaarbedrag', 'B', 0, 'R', True)
        self.cell(45, 5, 'Per maand', 'B', 1, 'R', True)

    def jaartotalen_page(self, jaartotalen: dict, maandoverzicht: dict):
        """Pagina met jaartotalen per categorie.

        Slimme page-breaks: secties die te lang zijn worden gesplitst
        met herhaalde headers op de nieuwe pagina.
        """
        sections_config = [
            ('inkomsten', 'Inkomsten'),
            ('vaste_lasten', 'Vaste Lasten'),
            ('variabele_kosten', 'Variabele Kosten'),
            ('sparen_beleggen', 'Sparen & Beleggen'),
        ]

        PAGE_BOTTOM = 270  # portrait max Y

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

                row_h = 5
                header_h = 11  # sectie header + kolom headers
                footer_h = 12  # totaal rij + spacing
                needed = header_h + len(entries) * row_h + footer_h

                # Past de hele sectie?
                if self.get_y() + needed > PAGE_BOTTOM:
                    fresh_start = 22
                    if fresh_start + needed <= PAGE_BOTTOM:
                        self.add_page()
                    else:
                        remaining = PAGE_BOTTOM - self.get_y()
                        if remaining < header_h + 3 * row_h + footer_h:
                            self.add_page()

                # Render header
                self._jaar_table_header(sec_label, sec_key)

                # Data rijen met slimme page-break
                self.set_font(self.DATA, '', 8)
                section_total = 0

                row_count = 0
                for ei, (cat, bedrag) in enumerate(entries):
                    section_total += bedrag
                    pm = bedrag / n_maanden
                    is_last = (ei == len(entries) - 1)
                    space_needed = row_h + (footer_h if is_last else 0)

                    if self.get_y() + space_needed > PAGE_BOTTOM:
                        # Page break — nieuwe pagina met herhaalde header
                        self.add_page()
                        self._jaar_table_header(sec_label, sec_key, continued=True)
                        self.set_font(self.DATA, '', 8)
                        row_count = 0

                    # Zebra-strepen
                    if row_count % 2 == 0:
                        self.set_fill_color(252, 251, 249)
                    else:
                        self.set_fill_color(*WHITE)

                    self.set_text_color(*INK)
                    self.cell(90, row_h, f'  {cat}', 0, 0, 'L', True)
                    self.set_text_color(*(GREEN if bedrag > 0 else RED))
                    self.cell(45, row_h, eur(bedrag), 0, 0, 'R', True)
                    self.set_text_color(*(GREEN if pm > 0 else RED))
                    self.cell(45, row_h, eur(pm), 0, 1, 'R', True)
                    row_count += 1

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


def _combineer_maandoverzichten(maandoverzicht: dict) -> dict:
    """Combineer maandoverzichten van alle rekeningen tot één geheel.

    Telt bedragen per categorie per maand op over alle rekeningen.
    Slaat interne_verschuivingen over — die vallen weg in het totaalbeeld.
    Retourneert: {'TOTAAL': {maand: {sectie: {cat: {bedrag: x}}}}}
    """
    gecombineerd = {}

    for rek, maanden in maandoverzicht.items():
        for maand, secties in maanden.items():
            if maand not in gecombineerd:
                gecombineerd[maand] = {}
            for sec_key, cats in secties.items():
                if not isinstance(cats, dict):
                    continue
                # Interne verschuivingen overslaan in gecombineerd overzicht
                if sec_key == 'interne_verschuivingen':
                    continue
                if sec_key not in gecombineerd[maand]:
                    gecombineerd[maand][sec_key] = {}
                for cat, val in cats.items():
                    b = val.get('bedrag', 0) if isinstance(val, dict) else (val or 0)
                    if cat not in gecombineerd[maand][sec_key]:
                        gecombineerd[maand][sec_key][cat] = {'bedrag': 0}
                    gecombineerd[maand][sec_key][cat]['bedrag'] += b

    return {'TOTAAL': gecombineerd}


def _combineer_jaartotalen(jaartotalen: dict) -> dict:
    """Combineer jaartotalen van alle rekeningen tot één geheel.

    Slaat interne_verschuivingen over.
    """
    gecombineerd = {}

    for rek, totalen in jaartotalen.items():
        for sec_key, cats in totalen.items():
            if not isinstance(cats, dict):
                continue
            # Interne verschuivingen overslaan
            if sec_key == 'interne_verschuivingen':
                continue
            if sec_key not in gecombineerd:
                gecombineerd[sec_key] = {}
            for cat, bedrag in cats.items():
                if cat not in gecombineerd[sec_key]:
                    gecombineerd[sec_key][cat] = 0
                gecombineerd[sec_key][cat] += (bedrag or 0)

    return {'TOTAAL': gecombineerd}


def _combineer_feiten(feiten: dict) -> dict:
    """Combineer feiten van alle rekeningen voor cover-stats."""
    totaal_ink = sum(f['totalen']['inkomsten'] for f in feiten.values())
    totaal_uit = sum(f['totalen']['uitgaven'] for f in feiten.values())
    periodes = []
    for f in feiten.values():
        periodes.append(f['periode']['van'])
        periodes.append(f['periode']['tot'])

    return {
        'totalen': {'inkomsten': totaal_ink, 'uitgaven': totaal_uit, 'netto': totaal_ink + totaal_uit},
        'periode': {'van': min(periodes), 'tot': max(periodes)},
    }


def genereer_pdf(rapport: dict) -> bytes:
    """Genereer een premium PDF rapport."""
    pdf = RapportPDF()

    feiten = rapport.get('feiten', {})
    analyse = rapport.get('analyse', {})
    maandoverzicht = rapport.get('maandoverzicht', {})
    jaartotalen = rapport.get('jaartotalen', {})
    datum = datetime.now().strftime('%d-%m-%Y')

    # Pagina 1: Executive summary op huishoudniveau
    pdf.add_page()
    pdf.cover_page(feiten, datum, jaartotalen=jaartotalen, maandoverzicht=maandoverzicht)

    # Pagina 2: Analyse & Inzichten
    if analyse and analyse.get('samenvatting'):
        pdf.analyse_page(analyse)

    # --- BIJLAGE: Gedetailleerd overzicht ---
    # Scheidingspagina
    pdf.add_page()
    pdf.set_fill_color(*INK)
    pdf.rect(0, 0, 210, 297, 'F')
    pdf.set_font(pdf.HEADING, '', 22)
    pdf.set_text_color(*WHITE)
    pdf.set_xy(15, 120)
    pdf.cell(180, 12, 'Bijlage', 0, 1, 'C')
    pdf.set_font(pdf.BODY, '', 11)
    pdf.set_text_color(180, 180, 195)
    pdf.set_xy(15, 138)
    pdf.cell(180, 7, 'Gedetailleerd maand- en jaaroverzicht', 0, 1, 'C')
    pdf.set_draw_color(*GOLD)
    pdf.set_line_width(0.6)
    pdf.line(85, 150, 125, 150)

    # Gecombineerd overzicht (alle rekeningen samen)
    if maandoverzicht and len(feiten) >= 1:
        combi_maand = _combineer_maandoverzichten(maandoverzicht)
        combi_feiten = {'TOTAAL': _combineer_feiten(feiten)}
        pdf.maandoverzicht_page(combi_maand, combi_feiten)

    if jaartotalen and len(feiten) >= 1:
        combi_jaar = _combineer_jaartotalen(jaartotalen)
        combi_maand_count = _combineer_maandoverzichten(maandoverzicht) if maandoverzicht else {}
        pdf.jaartotalen_page(combi_jaar, combi_maand_count)

    # Detail per rekening (alleen als er meerdere rekeningen zijn)
    if len(feiten) > 1:
        if maandoverzicht:
            pdf.maandoverzicht_page(maandoverzicht, feiten)
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

    # Anti-spam: platte tekst versie (HTML-only is een spam signaal)
    plain_text = (
        f"Uw financieel rapport is klaar\n\n"
        f"Beste klant,\n\n"
        f"Bijgevoegd vindt u uw persoonlijke financiele analyse van PeterHeijen.com.\n"
        f"Het rapport bevat een overzicht van uw inkomsten en uitgaven, "
        f"gecategoriseerd per maand, met concrete inzichten en aanbevelingen.\n\n"
        f"Rapport ID: {report_id}\n\n"
        f"Heeft u vragen? Neem contact op via info@peterheijen.com\n\n"
        f"Met vriendelijke groet,\n"
        f"Peter Heijen\n"
        f"PeterHeijen.com\n\n"
        f"Engelcke B.V. | Tienhoven | KvK 30277920\n"
        f"U ontvangt deze email omdat u een financiele analyse heeft aangevraagd op peterheijen.com."
    )

    payload = {
        "from": "Peter Heijen <rapport@peterheijen.com>",
        "reply_to": "info@peterheijen.com",
        "to": [email],
        "subject": f"Uw persoonlijke financiele analyse is klaar",
        "text": plain_text,
        "html": f"""<!DOCTYPE html>
<html lang="nl">
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f7f6f2;font-family:Georgia,serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f6f2">
<tr><td align="center" style="padding:20px 0">
<table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden">
  <tr><td style="background:#1a1a2e;padding:24px 30px">
    <p style="color:#ffffff;font-size:18px;margin:0;font-family:Georgia,serif">Peter<span style="color:#c9a84c">Heijen</span>.com</p>
  </td></tr>
  <tr><td style="padding:30px">
    <h1 style="color:#1a1a2e;font-size:20px;margin:0 0 16px;font-family:Georgia,serif">Uw financieel rapport is klaar</h1>
    <p style="color:#3d3d5c;font-size:15px;line-height:1.7;margin:0 0 16px">
      Beste klant,
    </p>
    <p style="color:#3d3d5c;font-size:15px;line-height:1.7;margin:0 0 16px">
      Bijgevoegd vindt u uw persoonlijke financiele analyse.
      Het rapport bevat een overzicht van uw inkomsten en uitgaven,
      gecategoriseerd per maand, met concrete inzichten en aanbevelingen.
    </p>
    <p style="color:#3d3d5c;font-size:14px;line-height:1.7;margin:0 0 16px">
      Rapport ID: <strong>{report_id}</strong>
    </p>
    <hr style="border:none;border-top:1px solid #ddd9d0;margin:20px 0">
    <p style="color:#3d3d5c;font-size:13px;line-height:1.6;margin:0 0 8px">
      Heeft u vragen over uw rapport? Neem contact op via
      <a href="mailto:info@peterheijen.com" style="color:#1f5c8b">info@peterheijen.com</a>
    </p>
    <p style="color:#3d3d5c;font-size:13px;line-height:1.6;margin:0">
      Met vriendelijke groet,<br>Peter Heijen
    </p>
  </td></tr>
  <tr><td style="background:#f7f6f2;padding:16px 30px;border-top:1px solid #ddd9d0">
    <p style="color:#999;font-size:11px;line-height:1.5;margin:0">
      Engelcke B.V. | Tienhoven | KvK 30277920<br>
      U ontvangt deze email omdat u een financiele analyse heeft aangevraagd op
      <a href="https://peterheijen.com" style="color:#999">peterheijen.com</a>.
    </p>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>""",
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
    """Basis health check."""
    return {"status": "ok", "versie": "0.1.0", "service": "peterheijen-finance"}


@app.get("/health")
def deep_health():
    """Uitgebreide health check — test of alle afhankelijkheden geconfigureerd zijn."""
    checks = {}

    # Claude API key
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    checks['claude_api_key'] = bool(api_key and api_key.startswith('sk-'))

    # Claude model
    model = os.environ.get('CLAUDE_MODEL', 'claude-opus-4-6')
    checks['claude_model'] = model

    # Resend API key
    resend_key = os.environ.get('RESEND_API_KEY')
    checks['resend_api_key'] = bool(resend_key and resend_key.startswith('re_'))

    # Fonts directory
    fonts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts')
    fonts_present = os.path.exists(fonts_dir) and len([f for f in os.listdir(fonts_dir) if f.endswith('.ttf')]) >= 8
    checks['premium_fonts'] = fonts_present

    alles_ok = all([
        checks['claude_api_key'],
        checks['resend_api_key'],
        checks['premium_fonts'],
    ])

    return {
        "status": "ok" if alles_ok else "NIET KLAAR",
        "checks": checks,
        "bericht": "Alle systemen operationeel" if alles_ok else "Een of meer afhankelijkheden ontbreken — check Railway Variables",
    }


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


def _run_rapport_pipeline(job_id: str, bestanden: list, email: str):
    """Achtergrond-thread: volledige pipeline upload → analyse → PDF → email.

    bestanden: list van (inhoud_bytes, bestandsnaam) tuples.
    Schrijft voortgang naar jobs[job_id] zodat de status-endpoint het kan serveren.
    Draait NIET in een HTTP-request — dus geen proxy timeout meer.
    """
    def update(fase: str, pct: int):
        with jobs_lock:
            jobs[job_id]['fase'] = fase
            jobs[job_id]['voortgang'] = pct
        logger.info(f"[{job_id}] {fase}")

    try:
        # 1. Inlezen — meerdere bestanden samenvoegen
        update('Transacties inlezen...', 10)
        dfs = []
        for inhoud, bestandsnaam in bestanden:
            df_deel = lees_transacties(inhoud, bestandsnaam)
            dfs.append(df_deel)
            logger.info(f"[{job_id}] {bestandsnaam}: {len(df_deel)} transacties")
        df = pd.concat(dfs, ignore_index=True)
        update(f'{len(df)} transacties ingelezen uit {len(bestanden)} bestand(en)', 15)

        # 2. Deterministisch rekenen
        update('Bedragen berekenen en controleren...', 20)
        feiten = bereken_feiten(df)
        top = bereken_top(df)
        update(f'Feiten berekend voor {len(feiten)} rekening(en)', 30)

        # 3. Claude categoriseren + analyseren (langste stap: 30s-300s)
        update('AI analyseert uw transacties...', 35)
        prompt = bouw_prompt(df, feiten, top)
        logger.info(f"[{job_id}] Prompt: {len(prompt)} tekens, {len(df)} transacties")
        claude_result = vraag_claude(prompt)

        if not claude_result.get('data'):
            raise ValueError(f"AI-analyse ongeldig: {claude_result.get('error', 'onbekend')}")
        update('AI-analyse compleet', 70)

        # 4. Rapport data samenstellen
        rapport_data = {
            'report_id': job_id,
            'gegenereerd': datetime.now().isoformat(),
            'bestand': bestandsnaam,
            'feiten': feiten,
            'maandoverzicht': claude_result['data'].get('maandoverzicht', {}),
            'jaartotalen': claude_result['data'].get('jaartotalen', {}),
            'analyse': claude_result['data'].get('analyse', {}),
        }

        # 5. PDF genereren
        update('Premium PDF-rapport genereren...', 75)
        pdf_bytes = genereer_pdf(rapport_data)
        logger.info(f"[{job_id}] PDF gegenereerd ({len(pdf_bytes)} bytes)")
        update('PDF klaar', 85)

        # 6. Email versturen
        update('Rapport per email versturen...', 90)
        email_verstuurd = verstuur_rapport_email(email, pdf_bytes, job_id)

        if not email_verstuurd:
            raise ValueError(
                "Uw analyse is gelukt, maar het rapport kon niet per email worden verstuurd. "
                "Probeer het opnieuw of neem contact op via info@peterheijen.com."
            )

        # Klaar!
        with jobs_lock:
            jobs[job_id]['status'] = 'compleet'
            jobs[job_id]['fase'] = 'Rapport verstuurd!'
            jobs[job_id]['voortgang'] = 100
        logger.info(f"[{job_id}] Pipeline compleet — rapport verstuurd naar {email}")

    except Exception as e:
        logger.error(f"[{job_id}] Pipeline FOUT: {type(e).__name__}: {e}")
        with jobs_lock:
            jobs[job_id]['status'] = 'fout'
            jobs[job_id]['error'] = str(e)
            jobs[job_id]['voortgang'] = 0


@app.post("/rapport")
async def rapport(bestanden: Optional[List[UploadFile]] = None, bestand: Optional[UploadFile] = None, email: str = Form(...)):
    """Start de rapport-pipeline als achtergrond-job.

    Accepteert één of meerdere bestanden:
      - 'bestanden' (meerdere files) of 'bestand' (enkele file, backward compatible)

    Retourneert DIRECT (< 1 sec) met een job_id.
    Client pollt /rapport/{job_id}/status voor voortgang.
    """
    job_id = str(uuid.uuid4())[:8]
    logger.info(f"[{job_id}] Rapport aangevraagd voor {email}")

    # Verzamel alle bestanden (support zowel 'bestanden' als 'bestand' veld)
    uploads = []
    if bestanden:
        uploads.extend(bestanden)
    if bestand:
        uploads.append(bestand)
    if not uploads:
        raise HTTPException(status_code=400, detail="Geen bestanden geüpload")

    # Bestanden direct inlezen en valideren (snel, < 1 sec per bestand)
    bestanden_data = []
    totaal_transacties = 0
    try:
        for f in uploads:
            inhoud = await f.read()
            if len(inhoud) > 10 * 1024 * 1024:
                raise HTTPException(status_code=400, detail=f"Bestand '{f.filename}' is te groot (max 10 MB)")
            # Quick validatie
            df_test = lees_transacties(inhoud, f.filename)
            totaal_transacties += len(df_test)
            del df_test
            bestanden_data.append((inhoud, f.filename))
            logger.info(f"[{job_id}] {f.filename}: {len(inhoud)} bytes — OK")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{job_id}] Inleesfout: {e}")
        raise HTTPException(status_code=400, detail=f"Kan bestand niet lezen: {e}")

    logger.info(f"[{job_id}] {totaal_transacties} transacties totaal uit {len(bestanden_data)} bestand(en)")

    # Job registreren
    namen = ', '.join(f[1] for f in bestanden_data)
    with jobs_lock:
        jobs[job_id] = {
            'status': 'bezig',
            'fase': 'Gestart...',
            'voortgang': 5,
            'email': email,
            'bestand': namen,
            'gestart': datetime.now().isoformat(),
        }

    # Start achtergrond-thread
    thread = threading.Thread(
        target=_run_rapport_pipeline,
        args=(job_id, bestanden_data, email),
        daemon=True,
    )
    thread.start()

    return {
        'job_id': job_id,
        'status': 'gestart',
    }


@app.get("/rapport/{job_id}/status")
def rapport_status(job_id: str):
    """Poll-endpoint voor voortgang van een rapport-job.

    Retourneert altijd snel (< 100ms).
    Client pollt elke 3 seconden tot status == 'compleet' of 'fout'.
    """
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job niet gevonden")

    return {
        'job_id': job_id,
        'status': job['status'],
        'fase': job.get('fase', ''),
        'voortgang': job.get('voortgang', 0),
        'email': job.get('email', ''),
        'error': job.get('error'),
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
