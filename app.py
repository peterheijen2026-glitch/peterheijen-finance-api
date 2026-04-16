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

# ---------------------------------------------------------------------------
# BANK-FORMAAT HERKENNING & NORMALISATIE
# ---------------------------------------------------------------------------
# Ondersteunde banken: ABN AMRO, ING, Rabobank, SNS, ASN, Triodos, Knab,
# Bunq, N26, RegioBank — zowel CSV als XLS/XLSX.
#
# Elke bank levert eigen kolomnamen en formaat. Alles wordt genormaliseerd
# naar: Rekeningnummer, Transactiedatum (YYYYMMDD), Transactiebedrag (float),
# Omschrijving, Beginsaldo, Eindsaldo.
# ---------------------------------------------------------------------------

def _parse_dutch_amount(val) -> float:
    """Parse Nederlands bedrag: '1.234,56' → 1234.56. Werkt ook met '-1234.56'."""
    if pd.isna(val):
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    # Als het al een float-achtig getal is (punt als decimaal, geen komma)
    if ',' not in s and s.replace('.', '', 1).replace('-', '', 1).isdigit():
        return float(s)
    # Nederlands formaat: punt = duizendtallen, komma = decimaal
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_datum(val) -> str:
    """Normaliseer datum naar YYYYMMDD string. Herkent: YYYYMMDD, DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD."""
    s = str(val).strip()
    # Al YYYYMMDD (8 cijfers)
    if len(s) == 8 and s.isdigit():
        return s
    # DD-MM-YYYY of DD/MM/YYYY
    for sep in ['-', '/']:
        if sep in s:
            parts = s.split(sep)
            if len(parts) == 3:
                # Bepaal volgorde: als eerste deel 4 cijfers → YYYY-MM-DD
                if len(parts[0]) == 4:
                    return f"{parts[0]}{parts[1].zfill(2)}{parts[2].zfill(2)}"
                else:
                    return f"{parts[2]}{parts[1].zfill(2)}{parts[0].zfill(2)}"
    return s


def _bereken_saldos(df: pd.DataFrame) -> pd.DataFrame:
    """Bereken Beginsaldo/Eindsaldo als die ontbreken. Cumulatief per rekening."""
    if 'Beginsaldo' in df.columns and 'Eindsaldo' in df.columns:
        return df
    df = df.sort_values('Transactiedatum')
    for rek in df['Rekeningnummer'].unique():
        mask = df['Rekeningnummer'] == rek
        bedragen = df.loc[mask, 'Transactiebedrag'].astype(float)
        cumsum = bedragen.cumsum()
        df.loc[mask, 'Eindsaldo'] = cumsum
        df.loc[mask, 'Beginsaldo'] = cumsum - bedragen
    return df


def _gebruik_saldo_kolom(df: pd.DataFrame, saldo_col: str) -> pd.DataFrame:
    """Als er een 'Saldo na mutatie' kolom is, gebruik die voor nauwkeurigere saldo's."""
    if saldo_col not in df.columns:
        return _bereken_saldos(df)
    df['Eindsaldo'] = df[saldo_col].apply(_parse_dutch_amount)
    df['Beginsaldo'] = df['Eindsaldo'] - df['Transactiebedrag'].astype(float)
    return df


def _detecteer_formaat(df: pd.DataFrame) -> str:
    """Detecteer bankformaat op basis van kolomnamen."""
    cols = set(c.strip() for c in df.columns)

    # --- ABN AMRO XLSX (standaardformaat — ons referentieformaat) ---
    if 'Rekeningnummer' in cols and 'Transactiebedrag' in cols and 'Transactiedatum' in cols:
        return 'abnamro_xlsx'

    # --- ABN AMRO / ING CSV ---
    # ING: Datum;Naam / Omschrijving;Rekening;Tegenrekening;Code;Af Bij;Bedrag (EUR);Mutatiesoort;Mededelingen;Saldo na mutatie;Tag
    # ABN: Datum;Naam / Omschrijving;Rekening;Tegenrekening;Code;Af Bij;Bedrag (EUR);Mededelingen
    if 'Af Bij' in cols and 'Bedrag (EUR)' in cols:
        if 'Saldo na mutatie' in cols:
            return 'ing_csv'
        return 'abnamro_csv'

    # --- Rabobank CSV ---
    # IBAN/BBAN,Munt,BIC,Volgnr,Datum,Rentedatum,Bedrag,Saldo na trn,...,Naam tegenpartij,...,Omschrijving-1,...
    if 'IBAN/BBAN' in cols and ('Saldo na trn' in cols or 'Bedrag' in cols):
        return 'rabobank_csv'

    # --- Triodos CSV (uit custom parser) ---
    # Kolommen: Datum, Rekening, Bedrag, CreditDebet, Naam, Tegenrekening, Code, Omschrijving, Saldo
    # Verschil met Knab: Triodos heeft 'Tegenrekening' + 'Naam', Knab heeft 'Tegenrekeningnummer' + 'Tegenrekeninghouder'
    if 'CreditDebet' in cols and 'Bedrag' in cols and 'Tegenrekening' in cols and 'Naam' in cols:
        return 'triodos_csv'

    # --- Knab CSV ---
    # Rekeningnummer;Transactiedatum;Valutacode;CreditDebet;Bedrag;Tegenrekeningnummer;Tegenrekeninghouder;...;Omschrijving
    if 'CreditDebet' in cols and 'Bedrag' in cols:
        return 'knab_csv'

    # --- N26 CSV ---
    # "Date","Payee","Account number","Transaction type","Payment reference","Category","Amount (EUR)",...
    if 'Payee' in cols or 'Amount (EUR)' in cols:
        return 'n26_csv'

    # --- Bunq CSV ---
    # Datum;Bedrag;Rekening;Tegenpartij;Naam;Omschrijving
    if 'Tegenpartij' in cols and 'Naam' in cols and 'Bedrag' in cols:
        return 'bunq_csv'

    # --- SNS / ASN / RegioBank / Triodos (Volksbank-formaten) ---
    # Vaak: Datum, Omschrijving, Bedrag, Saldo (soms met Rekening/IBAN)
    if 'Bedrag' in cols and 'Datum' in cols:
        # Generiek NL-formaat met Datum + Bedrag
        return 'generiek_nl'

    return 'onbekend'


def _normaliseer(df: pd.DataFrame, formaat: str) -> pd.DataFrame:
    """Normaliseer elk bankformaat naar standaardkolommen."""
    logger.info(f"Bankformaat gedetecteerd: {formaat}")

    if formaat == 'abnamro_xlsx':
        # Al in standaardformaat — alleen datum normaliseren
        df['Transactiedatum'] = df['Transactiedatum'].astype(str).apply(_parse_datum)
        df['Transactiebedrag'] = df['Transactiebedrag'].apply(
            lambda v: _parse_dutch_amount(v) if not isinstance(v, (int, float)) else float(v))
        # Tegenrekening bewaren voor transfer-detectie
        if 'Tegenrekening' not in df.columns:
            df['Tegenrekening'] = ''
        return df

    elif formaat in ('abnamro_csv', 'ing_csv'):
        # Bedrag: komma-decimaal + "Af Bij" kolom
        df['_bedrag'] = df['Bedrag (EUR)'].apply(_parse_dutch_amount)
        df['_bedrag'] = df.apply(
            lambda r: -abs(r['_bedrag']) if str(r.get('Af Bij', '')).strip().lower() == 'af' else abs(r['_bedrag']),
            axis=1)
        # Mapping
        df['Rekeningnummer'] = df['Rekening']
        df['Transactiedatum'] = df['Datum'].astype(str).apply(_parse_datum)
        df['Transactiebedrag'] = df['_bedrag']
        # Omschrijving
        naam = df.get('Naam / Omschrijving', pd.Series([''] * len(df))).fillna('')
        meded = df.get('Mededelingen', pd.Series([''] * len(df))).fillna('')
        df['Omschrijving'] = (naam + ' ' + meded).str.strip()
        # Tegenrekening bewaren voor transfer-detectie
        if 'Tegenrekening' not in df.columns:
            df['Tegenrekening'] = ''
        # Saldo
        if formaat == 'ing_csv' and 'Saldo na mutatie' in df.columns:
            df = _gebruik_saldo_kolom(df, 'Saldo na mutatie')
        else:
            df = _bereken_saldos(df)
        return df

    elif formaat == 'rabobank_csv':
        df['Rekeningnummer'] = df['IBAN/BBAN']
        df['Transactiedatum'] = df['Datum'].astype(str).apply(_parse_datum)
        df['Transactiebedrag'] = df['Bedrag'].apply(_parse_dutch_amount)
        # Omschrijving: Naam tegenpartij + Omschrijving-1/2/3
        omschr_parts = []
        for col in ['Naam tegenpartij', 'Omschrijving-1', 'Omschrijving-2', 'Omschrijving-3']:
            if col in df.columns:
                omschr_parts.append(df[col].fillna(''))
        df['Omschrijving'] = pd.concat(omschr_parts, axis=1).apply(lambda r: ' '.join(r).strip(), axis=1) if omschr_parts else 'Onbekend'
        # Tegenrekening bewaren (Rabobank: 'IBAN/BBAN tegenpartij' of 'Tegenrekening IBAN/BBAN')
        for col in ['IBAN/BBAN tegenpartij', 'Tegenrekening IBAN/BBAN', 'Tegenrekening']:
            if col in df.columns:
                df['Tegenrekening'] = df[col].fillna('')
                break
        else:
            df['Tegenrekening'] = ''
        # Saldo
        if 'Saldo na trn' in df.columns:
            df = _gebruik_saldo_kolom(df, 'Saldo na trn')
        else:
            df = _bereken_saldos(df)
        return df

    elif formaat == 'triodos_csv':
        df['Rekeningnummer'] = df['Rekening']
        df['Transactiedatum'] = df['Datum'].astype(str).apply(_parse_datum)
        bedrag = df['Bedrag'].apply(_parse_dutch_amount)
        # CreditDebet: 'Credit' = positief, 'Debet' = negatief
        df['Transactiebedrag'] = df.apply(
            lambda r: -abs(_parse_dutch_amount(r['Bedrag'])) if str(r.get('CreditDebet', '')).strip().lower().startswith('d')
            else abs(_parse_dutch_amount(r['Bedrag'])), axis=1)
        # Omschrijving: Naam + Omschrijving
        naam = df.get('Naam', pd.Series([''] * len(df))).fillna('')
        omschr = df.get('Omschrijving', pd.Series([''] * len(df))).fillna('')
        df['Omschrijving'] = (naam + ' ' + omschr).str.strip()
        # Tegenrekening bewaren voor transfer-detectie (Triodos heeft deze kolom)
        if 'Tegenrekening' not in df.columns:
            df['Tegenrekening'] = ''
        # Saldo
        if 'Saldo' in df.columns:
            df = _gebruik_saldo_kolom(df, 'Saldo')
        else:
            df = _bereken_saldos(df)
        return df

    elif formaat == 'knab_csv':
        df['Rekeningnummer'] = df['Rekeningnummer']  # al goed
        df['Transactiedatum'] = df['Transactiedatum'].astype(str).apply(_parse_datum)
        bedrag = df['Bedrag'].apply(_parse_dutch_amount)
        # CreditDebet: 'C' = positief, 'D' = negatief
        df['Transactiebedrag'] = df.apply(
            lambda r: -abs(_parse_dutch_amount(r['Bedrag'])) if str(r.get('CreditDebet', '')).strip().upper() == 'D'
            else abs(_parse_dutch_amount(r['Bedrag'])), axis=1)
        # Omschrijving
        parts = []
        for col in ['Tegenrekeninghouder', 'Omschrijving']:
            if col in df.columns:
                parts.append(df[col].fillna(''))
        df['Omschrijving'] = pd.concat(parts, axis=1).apply(lambda r: ' '.join(r).strip(), axis=1) if parts else 'Onbekend'
        # Tegenrekening bewaren (Knab noemt het 'Tegenrekeningnummer')
        if 'Tegenrekeningnummer' in df.columns:
            df['Tegenrekening'] = df['Tegenrekeningnummer'].fillna('')
        elif 'Tegenrekening' not in df.columns:
            df['Tegenrekening'] = ''
        df = _bereken_saldos(df)
        return df

    elif formaat == 'n26_csv':
        # Engelse kolomnamen
        if 'Date' in df.columns:
            df['Transactiedatum'] = df['Date'].astype(str).apply(_parse_datum)
        elif 'Datum' in df.columns:
            df['Transactiedatum'] = df['Datum'].astype(str).apply(_parse_datum)
        # Amount
        amt_col = 'Amount (EUR)' if 'Amount (EUR)' in df.columns else 'Bedrag (EUR)' if 'Bedrag (EUR)' in df.columns else 'Bedrag'
        df['Transactiebedrag'] = df[amt_col].apply(
            lambda v: float(v) if isinstance(v, (int, float)) else _parse_dutch_amount(v))
        # Omschrijving
        payee = df.get('Payee', df.get('Naam', pd.Series([''] * len(df)))).fillna('')
        ref = df.get('Payment reference', df.get('Omschrijving', pd.Series([''] * len(df)))).fillna('')
        df['Omschrijving'] = (payee + ' ' + ref).str.strip()
        # N26 heeft geen rekeningnummer in export — gebruik Account number als die er is, anders placeholder
        if 'Account number' in df.columns:
            df['Rekeningnummer'] = df['Account number'].fillna('N26')
        else:
            df['Rekeningnummer'] = 'N26'
        df['Tegenrekening'] = ''  # N26 geeft geen tegenrekening
        df = _bereken_saldos(df)
        return df

    elif formaat == 'bunq_csv':
        df['Rekeningnummer'] = df.get('Rekening', pd.Series(['Bunq'] * len(df)))
        df['Transactiedatum'] = df['Datum'].astype(str).apply(_parse_datum)
        df['Transactiebedrag'] = df['Bedrag'].apply(_parse_dutch_amount)
        naam = df.get('Naam', pd.Series([''] * len(df))).fillna('')
        omschr = df.get('Omschrijving', pd.Series([''] * len(df))).fillna('')
        df['Omschrijving'] = (naam + ' ' + omschr).str.strip()
        # Tegenrekening: Bunq noemt het soms 'Tegenpartij'
        if 'Tegenpartij' in df.columns:
            df['Tegenrekening'] = df['Tegenpartij'].fillna('')
        else:
            df['Tegenrekening'] = ''
        df = _bereken_saldos(df)
        return df

    elif formaat == 'generiek_nl':
        # Generiek formaat voor SNS, ASN, Triodos, RegioBank en onbekende banken
        # Probeer slim de juiste kolommen te vinden
        df['Transactiedatum'] = df['Datum'].astype(str).apply(_parse_datum)
        df['Transactiebedrag'] = df['Bedrag'].apply(_parse_dutch_amount)

        # Zoek rekeningnummer
        for col in ['Rekening', 'IBAN', 'Rekeningnummer', 'IBAN/BBAN']:
            if col in df.columns:
                df['Rekeningnummer'] = df[col]
                break
        else:
            df['Rekeningnummer'] = 'Onbekend'

        # Zoek omschrijving
        for col in ['Omschrijving', 'Naam / Omschrijving', 'Naam', 'Mededelingen', 'Naam tegenpartij']:
            if col in df.columns:
                df['Omschrijving'] = df[col].fillna('')
                break
        else:
            df['Omschrijving'] = 'Geen omschrijving'

        # Af/Bij correctie als die kolom er is
        for col in ['Af Bij', 'Af/Bij', 'Credit/Debet', 'CreditDebet']:
            if col in df.columns:
                df['Transactiebedrag'] = df.apply(
                    lambda r: -abs(r['Transactiebedrag']) if str(r[col]).strip().lower() in ('af', 'd', 'debet')
                    else abs(r['Transactiebedrag']), axis=1)
                break

        # Tegenrekening bewaren als die er is
        for col in ['Tegenrekening', 'IBAN tegenpartij', 'Tegenrekeningnummer']:
            if col in df.columns:
                df['Tegenrekening'] = df[col].fillna('')
                break
        else:
            df['Tegenrekening'] = ''

        # Saldo
        for col in ['Saldo', 'Saldo na mutatie', 'Saldo na trn']:
            if col in df.columns:
                df = _gebruik_saldo_kolom(df, col)
                break
        else:
            df = _bereken_saldos(df)

        return df

    return df


def _parse_triodos_csv(inhoud: bytes) -> pd.DataFrame:
    """Parse Triodos Bank CSV - speciaal formaat zonder header, met dubbele quotes.

    Triodos levert een CSV waar elke regel gewrapped is in outer quotes,
    met escaped inner quotes en ;; aan het eind van elke regel.
    9 velden: Datum, Rekening, Bedrag, Credit/Debet, Naam, Tegenrekening, Code, Omschrijving, Saldo
    """
    text = inhoud.decode('latin-1', errors='replace')
    rows = []

    for line in text.strip().split('\n'):
        line = line.strip().rstrip(';')
        if not line:
            continue
        # Strip buitenste quotes
        if line.startswith('"') and line.endswith('"'):
            line = line[1:-1]
        # Unescape dubbele quotes
        line = line.replace('""', '"')
        # Parse met quote-aware comma splitting
        parts = []
        current = ''
        in_quotes = False
        for ch in line:
            if ch == '"':
                in_quotes = not in_quotes
            elif ch == ',' and not in_quotes:
                parts.append(current.strip())
                current = ''
            else:
                current += ch
        parts.append(current.strip())

        if len(parts) >= 7:
            rows.append(parts)

    if not rows:
        raise ValueError("Triodos CSV bevat geen transacties")

    # Maak DataFrame met standaard kolomnamen
    col_names = ['Datum', 'Rekening', 'Bedrag', 'CreditDebet', 'Naam', 'Tegenrekening', 'Code', 'Omschrijving', 'Saldo']
    # Pas kolommen aan als er meer of minder zijn
    df = pd.DataFrame(rows)
    actual_cols = min(len(col_names), len(df.columns))
    df.columns = col_names[:actual_cols] + [f'Extra_{i}' for i in range(actual_cols, len(df.columns))]

    logger.info(f"Triodos CSV: {len(df)} transacties geparsed")
    return df


def _is_triodos_format(inhoud: bytes) -> bool:
    """Detecteer of dit een Triodos CSV is: begint met quote, geen header, ;; aan einde."""
    try:
        first_line = inhoud.decode('latin-1', errors='replace').split('\n')[0].strip()
        # Triodos: begint met ", bevat ""-escaped velden, eindigt op ;;
        return first_line.startswith('"') and '""' in first_line and first_line.endswith(';;')
    except Exception:
        return False


def lees_transacties(inhoud: bytes, bestandsnaam: str) -> pd.DataFrame:
    """Lees transacties uit CSV/XLS/XLSX - herkent automatisch het bankformaat.

    Ondersteunde banken: ABN AMRO, ING, Rabobank, SNS, ASN, Triodos,
    Knab, Bunq, N26, RegioBank - zowel CSV als XLS/XLSX.
    """
    naam_lower = bestandsnaam.lower()

    if naam_lower.endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(io.BytesIO(inhoud))
        except Exception as e:
            raise ValueError(f"Kan Excel-bestand niet lezen: {e}")
    elif naam_lower.endswith('.csv'):
        # Strip BOM (Byte Order Mark)
        if inhoud.startswith(b'\xef\xbb\xbf'):
            inhoud = inhoud[3:]
        elif inhoud.startswith(b'\xff\xfe') or inhoud.startswith(b'\xfe\xff'):
            inhoud = inhoud[2:]

        # Fix problematische line endings (ING gebruikt \r\r\n)
        inhoud = inhoud.replace(b'\r\r\n', b'\n').replace(b'\r\n', b'\n').replace(b'\r', b'\n')

        # Log preview voor debugging
        try:
            preview = inhoud[:300].decode('utf-8', errors='replace')
        except Exception:
            preview = str(inhoud[:300])
        logger.info(f"CSV preview: {preview[:150]}")

        # Check speciaal Triodos-formaat (geen header, dubbele quotes)
        if _is_triodos_format(inhoud):
            logger.info("Triodos-formaat gedetecteerd — custom parser")
            df = _parse_triodos_csv(inhoud)
        else:
            # Standaard CSV parsing — probeer combinaties
            parsed = False
            best_df = None
            best_cols = 0

            for enc in ['utf-8', 'latin-1', 'cp1252']:
                for sep in [';', ',', '\t', '|']:
                    try:
                        test_df = pd.read_csv(io.BytesIO(inhoud), sep=sep, encoding=enc,
                                              dtype=str, on_bad_lines='skip')
                        n_cols = len(test_df.columns)
                        if n_cols > best_cols:
                            best_df = test_df
                            best_cols = n_cols
                        if n_cols > 3:
                            parsed = True
                            df = test_df
                            logger.info(f"CSV geparsed: sep={repr(sep)}, enc={enc}, "
                                        f"kolommen={n_cols}: {list(test_df.columns)}")
                            break
                    except Exception:
                        continue
                if parsed:
                    break

            if not parsed:
                # Fallback: python engine met auto-detectie
                try:
                    df = pd.read_csv(io.BytesIO(inhoud), sep=None, engine='python',
                                     encoding='utf-8', dtype=str, on_bad_lines='skip')
                    if len(df.columns) > 3:
                        parsed = True
                except Exception:
                    pass

            if not parsed:
                cols_info = ""
                if best_df is not None:
                    cols_info = f" Beste poging: {best_cols} kolommen."
                raise ValueError(
                    f"Kan CSV niet parsen — controleer of het een geldig bankafschrift is.{cols_info}")
    else:
        raise ValueError(f"Onbekend bestandstype: {bestandsnaam}. Ondersteund: .csv, .xls, .xlsx")

    # Strip spaties uit kolomnamen
    df.columns = [c.strip() for c in df.columns]

    # Detecteer en normaliseer bankformaat
    formaat = _detecteer_formaat(df)
    if formaat == 'onbekend':
        raise ValueError(
            f"Bankformaat niet herkend. Gevonden kolommen: {list(df.columns)}. "
            f"Ondersteund: ABN AMRO, ING, Rabobank, SNS, ASN, Triodos, Knab, Bunq, N26, RegioBank."
        )

    df = _normaliseer(df, formaat)

    # Valideer dat normalisatie gelukt is
    verwacht = ['Rekeningnummer', 'Transactiedatum', 'Transactiebedrag', 'Omschrijving']
    ontbreekt = [k for k in verwacht if k not in df.columns]
    if ontbreekt:
        raise ValueError(f"Kolommen ontbreken na normalisatie: {ontbreekt}. Gevonden: {list(df.columns)}")

    # Rekeningnummer altijd als string (ABN AMRO levert int, andere banken string)
    df['Rekeningnummer'] = df['Rekeningnummer'].astype(str).str.strip()

    # Saldo's toevoegen als die nog ontbreken
    df = _bereken_saldos(df)

    # Datum parsing - flexibel (YYYYMMDD)
    df['Transactiedatum'] = df['Transactiedatum'].astype(str)
    try:
        df['datum'] = pd.to_datetime(df['Transactiedatum'], format='%Y%m%d')
    except Exception:
        # Fallback: laat pandas het zelf uitzoeken
        df['datum'] = pd.to_datetime(df['Transactiedatum'], dayfirst=True)

    df['maand'] = df['datum'].dt.to_period('M')
    df['bedrag'] = df['Transactiebedrag'].astype(float)

    logger.info(f"Bestand gelezen: {len(df)} transacties, formaat={formaat}, "
                f"rekeningen={df['Rekeningnummer'].nunique()}")

    return df


# ---------------------------------------------------------------------------
# STAP 1B: HUISHOUDREGISTER & INTERNE-OVERBOEKINGEN DETECTIE
# ---------------------------------------------------------------------------

def _bouw_huishoudregister(df: pd.DataFrame) -> set:
    """Verzamel alle eigen rekeningnummers uit de upload.

    Alle rekeningen in de upload zijn per definitie 'eigen'.
    Returns: set van rekeningnummers (strings, stripped, lowercase-safe).
    """
    eigen = set()
    for rek in df['Rekeningnummer'].unique():
        rek_str = str(rek).strip()
        if rek_str and rek_str != 'Onbekend':
            eigen.add(rek_str)
    logger.info(f"Huishoudregister: {len(eigen)} eigen rekeningen gevonden: {eigen}")
    return eigen


def _normaliseer_iban(val) -> str:
    """Normaliseer een IBAN/rekeningnummer voor vergelijking.

    Verwijdert spaties, streepjes, en maakt uppercase.
    Zo matcht 'NL12 ABNA 0123 4567 89' met 'NL12ABNA0123456789'.
    """
    if pd.isna(val):
        return ''
    return str(val).strip().replace(' ', '').replace('-', '').upper()


import re
# NL IBAN: 2 letters + 2 cijfers + 4 letters bankcode + 10 cijfers account
# Buitenlands IBAN (bv EE/DE): ook 4 letters bankcode maar account kan langer zijn
_IBAN_RE = re.compile(r'[A-Z]{2}\d{2}[A-Z]{4}\d{10,14}')

def _extract_iban_uit_omschrijving(omschrijving: str) -> str:
    """Haal IBAN uit ABN-omschrijving.

    ABN XLSX-transacties hebben GEEN aparte Tegenrekening kolom.
    Het IBAN zit verborgen in de omschrijving in twee formaten:
      1. /IBAN/NL50BUNQ2208185153/BIC/...
      2. IBAN: NL50BUNQ2208185153        BIC: ...
    """
    if pd.isna(omschrijving):
        return ''
    tekst = str(omschrijving).upper().replace(' ', '')
    match = _IBAN_RE.search(tekst)
    return match.group(0) if match else ''


def _vul_lege_tegenrekening(df: pd.DataFrame) -> pd.DataFrame:
    """Vul lege Tegenrekening-velden door IBAN uit omschrijving te extraheren.

    Dit is essentieel voor ABN AMRO XLSX, waar geen Tegenrekening-kolom is.
    Zonder deze stap kunnen _detecteer_vast_inkomen en _detecteer_huurinkomsten
    niet groeperen op tegenpartij → inkomen wordt niet herkend.
    """
    if 'Tegenrekening' not in df.columns:
        df['Tegenrekening'] = ''

    lege_mask = df['Tegenrekening'].apply(lambda x: _normaliseer_iban(x) == '')
    n_leeg = lege_mask.sum()

    if n_leeg > 0:
        df.loc[lege_mask, 'Tegenrekening'] = (
            df.loc[lege_mask, 'Omschrijving'].apply(_extract_iban_uit_omschrijving)
        )
        n_gevuld = (df.loc[lege_mask, 'Tegenrekening'] != '').sum()
        if n_gevuld > 0:
            logger.info(f"IBAN-EXTRACTIE: {n_gevuld} van {n_leeg} lege tegenrekeningen gevuld uit omschrijving")

    return df


def _markeer_interne_transfers(df: pd.DataFrame, eigen_rekeningen: set) -> pd.DataFrame:
    """Markeer transacties tussen eigen rekeningen als interne transfer.

    Detectiemethoden (in volgorde van betrouwbaarheid):
    1. Tegenrekening kolom matcht een eigen rekeningnummer
    2. Eigen rekeningnummer komt voor in de omschrijving
    3. Matching bijschrijving/afschrijving op dezelfde datum (cross-account)

    Voegt kolom 'is_intern' toe (True/False).
    """
    # Normaliseer eigen rekeningen voor vergelijking
    eigen_genormaliseerd = set(_normaliseer_iban(r) for r in eigen_rekeningen if r)

    # Start: alles is niet-intern
    df['is_intern'] = False

    # Methode 1: Tegenrekening matcht eigen rekening
    if 'Tegenrekening' in df.columns:
        df['_tegen_norm'] = df['Tegenrekening'].apply(_normaliseer_iban)
        methode1_mask = df['_tegen_norm'].isin(eigen_genormaliseerd) & (df['_tegen_norm'] != '')
        df.loc[methode1_mask, 'is_intern'] = True
        n_methode1 = methode1_mask.sum()
        if n_methode1 > 0:
            logger.info(f"Interne transfers methode 1 (tegenrekening): {n_methode1} transacties")
        df.drop(columns=['_tegen_norm'], inplace=True)

    # Methode 2: Eigen rekeningnummer in omschrijving (fallback voor banken zonder tegenrekening)
    for rek in eigen_genormaliseerd:
        if len(rek) >= 8:  # Alleen zinvol bij IBANs/lange rekeningnummers
            # Check of het rekeningnummer (of deel ervan) in de omschrijving staat
            mask = (
                ~df['is_intern'] &  # Nog niet gemarkeerd
                df['Omschrijving'].str.upper().str.replace(' ', '').str.contains(rek, na=False, regex=False)
            )
            if mask.sum() > 0:
                df.loc[mask, 'is_intern'] = True
                logger.info(f"Interne transfers methode 2 (omschrijving bevat {rek}): {mask.sum()} transacties")

    # Statistieken loggen
    n_intern = df['is_intern'].sum()
    bedrag_intern = df.loc[df['is_intern'], 'Transactiebedrag'].apply(lambda x: abs(float(x))).sum()
    logger.info(f"Totaal interne transfers: {n_intern} transacties, "
                f"totaal bedrag: EUR {bedrag_intern:,.2f}")

    return df


def _detecteer_huishoudleden(df: pd.DataFrame) -> pd.DataFrame:
    """Detecteer automatisch overboekingen naar/van huishoudleden (partner, kinderen).

    Werkt voor ALLE gezinnen — geen hardcoded namen nodig.

    Heuristiek:
    1. Groepeer transacties per tegenpartij (uit Tegenrekening of naam in omschrijving)
    2. Een tegenpartij is waarschijnlijk een huishoudlid als:
       a) Er ZOWEL positieve als negatieve transacties zijn (geld gaat heen-en-weer)
       b) Er minstens 4 transacties per jaar zijn (regelmatig contact)
       c) Het netto-effect < 40% van het bruto volume is (ongeveer in balans)
       d) De tegenpartij NIET al door de merchant-mapping is herkend als bedrijf
    3. Markeer deze transacties als 'is_intern' = True

    Extra check: als een tegenpartij op dezelfde IBAN zit als een van de eigen rekeningen
    van de klant, is het sowieso al een interne transfer (al afgevangen in methode 1/2).
    """
    if 'Tegenrekening' not in df.columns:
        logger.info("HUISHOUDLEDEN: geen Tegenrekening kolom — skip detectie")
        return df

    # Alleen niet-interne, niet-merchant-mapped transacties bekijken
    mask_kandidaat = ~df['is_intern']
    df_kandidaat = df[mask_kandidaat].copy()

    if len(df_kandidaat) == 0:
        return df

    # Groepeer per tegenrekening (genormaliseerd)
    df_kandidaat['_tegen_norm'] = df_kandidaat['Tegenrekening'].apply(_normaliseer_iban)
    # Filter lege tegenrekeningen
    df_kandidaat = df_kandidaat[df_kandidaat['_tegen_norm'] != '']

    # Bekende merchant-zoektermen om bedrijven eruit te filteren
    bekende_merchants = set()
    for zoekterm, _, _, _ in MERCHANT_MAPPING:
        bekende_merchants.add(zoekterm)

    huishoudleden_ibans = set()
    huishoudleden_namen = []

    for tegen_rek, groep in df_kandidaat.groupby('_tegen_norm'):
        if len(groep) < 4:
            continue  # Te weinig transacties

        # Check of dit een bekende merchant is
        omschr_sample = ' '.join(groep['Omschrijving'].astype(str).str.upper().head(3))
        is_merchant = False
        for merchant_zoek in bekende_merchants:
            if merchant_zoek in omschr_sample:
                is_merchant = True
                break
        if is_merchant:
            continue

        positief = groep[groep['bedrag'] > 0]['bedrag'].sum()
        negatief = abs(groep[groep['bedrag'] < 0]['bedrag'].sum())
        bruto = positief + negatief
        netto = abs(positief - negatief)

        if bruto < 500:
            continue  # Te klein volume, niet significant

        # Bidirectioneel: er moet geld BEIDE kanten op gaan
        if positief == 0 or negatief == 0:
            continue

        # Netto-effect moet relatief klein zijn (< 40% van bruto)
        if bruto > 0 and (netto / bruto) < 0.40:
            huishoudleden_ibans.add(tegen_rek)
            # Pak een leesbare naam uit de omschrijving
            naam_sample = groep.iloc[0]['Omschrijving']
            huishoudleden_namen.append(f"{tegen_rek} ({naam_sample[:40]}...)")
            logger.info(
                f"HUISHOUDLID GEDETECTEERD: {tegen_rek} — "
                f"{len(groep)} transacties, positief EUR {positief:,.0f}, "
                f"negatief EUR {negatief:,.0f}, netto EUR {netto:,.0f} "
                f"({netto/bruto*100:.0f}% van bruto)"
            )

    # Markeer alle transacties met deze tegenrekeningen als intern
    if huishoudleden_ibans:
        df['_tegen_check'] = df['Tegenrekening'].apply(_normaliseer_iban)
        mask_huishoud = df['_tegen_check'].isin(huishoudleden_ibans) & ~df['is_intern']
        n_gemarkeerd = mask_huishoud.sum()
        df.loc[mask_huishoud, 'is_intern'] = True
        df.drop(columns=['_tegen_check'], inplace=True)

        bedrag_huishoud = df.loc[mask_huishoud, 'bedrag'].apply(lambda x: abs(float(x))).sum() if n_gemarkeerd > 0 else 0
        logger.info(
            f"HUISHOUDLEDEN: {len(huishoudleden_ibans)} huishoudlid(en) gedetecteerd, "
            f"{n_gemarkeerd} transacties als intern gemarkeerd "
            f"(EUR {bedrag_huishoud:,.0f} bruto)"
        )
    else:
        if '_tegen_check' in df.columns:
            df.drop(columns=['_tegen_check'], inplace=True)
        logger.info("HUISHOUDLEDEN: geen huishoudleden gedetecteerd via bidirectionele analyse")

    # Cleanup
    if '_tegen_norm' in df.columns:
        df.drop(columns=['_tegen_norm'], errors='ignore', inplace=True)

    return df


# ---------------------------------------------------------------------------
# STAP 1c: RULE-BASED CLASSIFICATIE (vóór AI)
# ---------------------------------------------------------------------------
# ChatGPT CEO-plan: "AI mag pas aan zet nadat het systeem al heeft vastgesteld
# wat inkomen is, wat transfer is, wat belasting is, wat hypotheek is."
#
# Deze laag classificeert transacties op basis van harde regels.
# AI mag daarna samenvatten en restcategorieën invullen, maar NIET overrulen.

# Merchant mapping: bekende tegenpartijen → vaste categorie
# Format: (zoekterm_in_omschrijving_uppercase, sectie, categorie, confidence)
MERCHANT_MAPPING = [
    # --- INKOMEN ---
    # UWV
    ('UWV', 'inkomsten', 'UWV/Uitkeringen', 0.95),
    # Kinderbijslag / Kindregelingen
    ('SVB KINDERBIJSLAG', 'inkomsten', 'Kinderbijslag/Kindregelingen', 0.99),
    ('SVB', 'inkomsten', 'Kinderbijslag/Kindregelingen', 0.80),
    # Belastingteruggave (positieve bedragen van Belastingdienst)
    # → wordt apart afgehandeld in de functie (bedrag-afhankelijk)
    # Toeslagen
    ('ZORGTOESLAG', 'inkomsten', 'Toeslagen', 0.99),
    ('HUURTOESLAG', 'inkomsten', 'Toeslagen', 0.99),
    ('KINDGEBONDEN BUDGET', 'inkomsten', 'Toeslagen', 0.99),
    # DGA-loon: generieke patronen die in heel Nederland voorkomen
    # Specifieke BV-namen worden NIET hardcoded — AI herkent DGA-patronen
    # op basis van vast maandelijks bedrag van een BV.

    # --- BELASTINGDIENST (negatief = betaling, positief = teruggave) ---
    # Wordt apart afgehandeld in de functie (bedrag-afhankelijk)

    # --- VASTE LASTEN ---
    # Hypotheek / Woonlasten
    ('ASR', 'vaste_lasten', 'Hypotheek/Huur', 0.85),  # ASR = meestal hypotheek-gekoppeld
    ('A.S.R', 'vaste_lasten', 'Hypotheek/Huur', 0.85),
    ('NATIONALE-NEDERLANDEN', 'vaste_lasten', 'Hypotheek/Huur', 0.80),
    ('AEGON', 'vaste_lasten', 'Hypotheek/Huur', 0.80),
    ('DELTA LLOYD', 'vaste_lasten', 'Hypotheek/Huur', 0.80),
    ('VVE', 'vaste_lasten', 'Hypotheek/Huur', 0.90),
    # Energie
    ('FRANK ENERGIE', 'vaste_lasten', 'Energie', 0.99),
    ('VATTENFALL', 'vaste_lasten', 'Energie', 0.99),
    ('ENECO', 'vaste_lasten', 'Energie', 0.99),
    ('ESSENT', 'vaste_lasten', 'Energie', 0.99),
    ('BUDGET ENERGIE', 'vaste_lasten', 'Energie', 0.99),
    ('GREENCHOICE', 'vaste_lasten', 'Energie', 0.99),
    # Water
    ('VITENS', 'vaste_lasten', 'Water', 0.99),
    ('BRABANT WATER', 'vaste_lasten', 'Water', 0.99),
    ('PWN', 'vaste_lasten', 'Water', 0.95),
    ('DUNEA', 'vaste_lasten', 'Water', 0.99),
    ('WATERNET', 'vaste_lasten', 'Water', 0.99),
    # Zorgverzekering
    ('CZ GROEP', 'vaste_lasten', 'Zorgverzekering', 0.99),
    ('CZ ZORGVERZEKERING', 'vaste_lasten', 'Zorgverzekering', 0.99),
    ('ZILVEREN KRUIS', 'vaste_lasten', 'Zorgverzekering', 0.99),
    ('MENZIS', 'vaste_lasten', 'Zorgverzekering', 0.99),
    ('OHRA', 'vaste_lasten', 'Zorgverzekering', 0.95),
    # Gemeentebelasting
    ('GEMEENTELIJKE BELASTING', 'vaste_lasten', 'Gemeentebelasting/OZB/Waterschapsbelasting', 0.99),
    ('GBLT', 'vaste_lasten', 'Gemeentebelasting/OZB/Waterschapsbelasting', 0.99),
    ('WATERSCHAP', 'vaste_lasten', 'Gemeentebelasting/OZB/Waterschapsbelasting', 0.99),
    # Internet/TV
    ('ZIGGO', 'vaste_lasten', 'Internet/TV', 0.95),
    ('KPN', 'vaste_lasten', 'Internet/TV', 0.85),
    # Mobiele telefonie
    ('T-MOBILE', 'vaste_lasten', 'Mobiele telefonie', 0.95),
    ('VODAFONE', 'vaste_lasten', 'Mobiele telefonie', 0.95),
    # Streaming/Digitaal
    ('NETFLIX', 'vaste_lasten', 'Streaming/Digitaal', 0.99),
    ('SPOTIFY', 'vaste_lasten', 'Streaming/Digitaal', 0.99),
    ('DISNEY', 'vaste_lasten', 'Streaming/Digitaal', 0.95),
    ('APPLE.COM/BILL', 'vaste_lasten', 'Streaming/Digitaal', 0.90),
    ('ICLOUD', 'vaste_lasten', 'Streaming/Digitaal', 0.95),
    ('YOUTUBE PREMIUM', 'vaste_lasten', 'Streaming/Digitaal', 0.99),
    # Donaties
    ('GIVT', 'vaste_lasten', 'Donaties/Goede doelen', 0.95),
    ('KWF', 'vaste_lasten', 'Donaties/Goede doelen', 0.95),
    ('RODE KRUIS', 'vaste_lasten', 'Donaties/Goede doelen', 0.95),
    ('OXFAM', 'vaste_lasten', 'Donaties/Goede doelen', 0.95),
    ('PARTIJ VOOR DE DIEREN', 'vaste_lasten', 'Donaties/Goede doelen', 0.95),

    # --- VARIABELE KOSTEN ---
    # Supermarkten
    ('ALBERT HEIJN', 'variabele_kosten', 'Boodschappen/Supermarkt', 0.99),
    ('JUMBO', 'variabele_kosten', 'Boodschappen/Supermarkt', 0.95),
    ('LIDL', 'variabele_kosten', 'Boodschappen/Supermarkt', 0.99),
    ('PLUS SUPERMARKT', 'variabele_kosten', 'Boodschappen/Supermarkt', 0.95),
    ('DIRK', 'variabele_kosten', 'Boodschappen/Supermarkt', 0.95),
    # Drogist
    ('ETOS', 'variabele_kosten', 'Drogist', 0.95),
    ('KRUIDVAT', 'variabele_kosten', 'Drogist', 0.95),
    # Tankstations
    ('SHELL', 'variabele_kosten', 'Benzine/Diesel/Laden', 0.90),
    ('BP ', 'variabele_kosten', 'Benzine/Diesel/Laden', 0.85),
    ('TOTALENERGIES', 'variabele_kosten', 'Benzine/Diesel/Laden', 0.95),
    ('TANGO', 'variabele_kosten', 'Benzine/Diesel/Laden', 0.95),
    ('TINQ', 'variabele_kosten', 'Benzine/Diesel/Laden', 0.95),
    # OV
    ('NS GROEP', 'variabele_kosten', 'OV/Trein', 0.95),
    ('OV-CHIPKAART', 'variabele_kosten', 'OV/Trein', 0.99),
    ('CONNEXXION', 'variabele_kosten', 'OV/Trein', 0.95),
    # Afhaal/Bezorging
    ('THUISBEZORGD', 'variabele_kosten', 'Afhaal/Bezorging', 0.99),
    ('UBER EATS', 'variabele_kosten', 'Afhaal/Bezorging', 0.99),
    ('DELIVEROO', 'variabele_kosten', 'Afhaal/Bezorging', 0.99),
    # Kleding
    ('H&M', 'variabele_kosten', 'Kleding', 0.90),
    ('ZARA', 'variabele_kosten', 'Kleding', 0.90),
    ('C&A', 'variabele_kosten', 'Kleding', 0.90),
    ('PRIMARK', 'variabele_kosten', 'Kleding', 0.95),
    # Elektronica/Online
    ('BOL.COM', 'variabele_kosten', 'Elektronica/Gadgets', 0.80),
    ('COOLBLUE', 'variabele_kosten', 'Elektronica/Gadgets', 0.90),
    ('AMAZON', 'variabele_kosten', 'Elektronica/Gadgets', 0.75),
    # Vakantie
    ('BOOKING.COM', 'variabele_kosten', 'Vakantie/Reizen', 0.95),
    ('AIRBNB', 'variabele_kosten', 'Vakantie/Reizen', 0.95),
    ('TRANSAVIA', 'variabele_kosten', 'Vakantie/Reizen', 0.99),
    ('KLM', 'variabele_kosten', 'Vakantie/Reizen', 0.95),
    ('RYANAIR', 'variabele_kosten', 'Vakantie/Reizen', 0.99),
    # Huishoudelijk
    ('ACTION', 'variabele_kosten', 'Huishoudelijke artikelen', 0.85),
    ('HEMA', 'variabele_kosten', 'Huishoudelijke artikelen', 0.80),
    # Apotheek
    ('BENU', 'variabele_kosten', 'Apotheek/Medicijnen', 0.95),
    ('APOTHEEK', 'variabele_kosten', 'Apotheek/Medicijnen', 0.90),

    # --- SPAREN & BELEGGEN ---
    ('SAXO BANK', 'sparen_beleggen', 'Effectenrekening', 0.95),
    ('SAXO', 'sparen_beleggen', 'Effectenrekening', 0.90),
    ('DEGIRO', 'sparen_beleggen', 'Effectenrekening', 0.95),
    ('IBKR', 'sparen_beleggen', 'Effectenrekening', 0.95),
    ('INTERACTIVE BROKERS', 'sparen_beleggen', 'Effectenrekening', 0.95),
    ('MINTOS', 'sparen_beleggen', 'Crowdlending', 0.99),
    ('LENDAHAND', 'sparen_beleggen', 'Crowdlending', 0.99),
    ('PEERBERRY', 'sparen_beleggen', 'Crowdlending', 0.99),
    ('BRAND NEW DAY', 'sparen_beleggen', 'Pensioenopbouw', 0.99),

    # --- CREDITCARD (geen consumptie, interne verschuiving) ---
    ('ICS/INT CARD', 'intern', 'Creditcard-aflossing', 0.90),
    ('ICS ', 'intern', 'Creditcard-aflossing', 0.85),
    ('INTERNATIONAL CARD SERVICES', 'intern', 'Creditcard-aflossing', 0.95),
    ('VISA CARD', 'intern', 'Creditcard-aflossing', 0.85),

    # --- TIKKIE (terugbetaling gedeelde kosten, geen inkomen) ---
    ('TIKKIE', 'intern', 'Tikkie-terugbetaling', 0.90),

    # --- FAMILIELEDEN / HUISHOUDLEDEN ---
    # NIET hardcoded — wordt automatisch gedetecteerd door _detecteer_huishoudleden()
    # op basis van bidirectionele geldstromen met persoonsnamen.
]


def _classificeer_rule_based(df: pd.DataFrame) -> pd.DataFrame:
    """Classificeer transacties op basis van harde regels VOORDAT de AI eraan te pas komt.

    Voegt kolommen toe:
    - 'regel_sectie': inkomsten/vaste_lasten/variabele_kosten/sparen_beleggen/intern/None
    - 'regel_categorie': specifieke categorie of None
    - 'regel_confidence': 0.0-1.0
    - 'classificatie_bron': 'rule' of 'ai' (wordt later ingevuld)

    Belastingdienst wordt apart afgehandeld: positief = teruggave (inkomsten), negatief = betaling (vaste lasten).
    """
    df['regel_sectie'] = None
    df['regel_categorie'] = None
    df['regel_confidence'] = 0.0
    df['classificatie_bron'] = None

    n_geclassificeerd = 0

    for idx, row in df.iterrows():
        if row.get('is_intern', False):
            continue  # Al gemarkeerd als intern

        omschr = str(row.get('Omschrijving', '')).upper()
        bedrag = float(row.get('bedrag', 0))

        # Speciaal geval: Belastingdienst
        if 'BELASTINGDIENST' in omschr or 'BELASTING DIENST' in omschr:
            if bedrag > 0:
                df.at[idx, 'regel_sectie'] = 'inkomsten'
                df.at[idx, 'regel_categorie'] = 'Belastingteruggave'
                df.at[idx, 'regel_confidence'] = 0.95
            else:
                # Probeer type belasting te herkennen
                if 'IB' in omschr or 'INKOMSTENBELASTING' in omschr or 'INKOMSTENBEL' in omschr:
                    df.at[idx, 'regel_categorie'] = 'Inkomstenbelasting/Voorlopige aanslag'
                elif 'OB' in omschr or 'OMZETBELASTING' in omschr or 'BTW' in omschr:
                    df.at[idx, 'regel_categorie'] = 'BTW/Omzetbelasting'
                elif 'ZVW' in omschr or 'ZORGVERZEKERINGSWET' in omschr:
                    df.at[idx, 'regel_categorie'] = 'Zorgverzekering'
                elif 'MRB' in omschr or 'MOTORRIJTUIGEN' in omschr:
                    df.at[idx, 'regel_categorie'] = 'Overige belastingen'
                else:
                    df.at[idx, 'regel_categorie'] = 'Inkomstenbelasting/Voorlopige aanslag'
                df.at[idx, 'regel_sectie'] = 'vaste_lasten'
                df.at[idx, 'regel_confidence'] = 0.90
            df.at[idx, 'classificatie_bron'] = 'rule'
            n_geclassificeerd += 1
            continue

        # Merchant mapping doorlopen
        for zoekterm, sectie, categorie, confidence in MERCHANT_MAPPING:
            if zoekterm in omschr:
                # Creditcard en Tikkie: markeer als intern
                if sectie == 'intern':
                    df.at[idx, 'regel_sectie'] = 'intern'
                    df.at[idx, 'regel_categorie'] = categorie
                    df.at[idx, 'regel_confidence'] = confidence
                    df.at[idx, 'classificatie_bron'] = 'rule'
                    n_geclassificeerd += 1
                    break
                # Effectenrekening: positief = terugstorting (niet als "inkomen")
                elif sectie == 'sparen_beleggen' and bedrag > 0:
                    df.at[idx, 'regel_sectie'] = 'inkomsten'
                    df.at[idx, 'regel_categorie'] = 'Effectenrekening (terugstorting)'
                    df.at[idx, 'regel_confidence'] = confidence
                    df.at[idx, 'classificatie_bron'] = 'rule'
                    n_geclassificeerd += 1
                    break
                # Tankstations: positief bedrag >€100 is GEEN benzine (Shell Energy etc.)
                # Laat AI dit classificeren (vaak energie-terugbetaling)
                elif categorie == 'Benzine/Diesel/Laden' and bedrag > 100:
                    continue  # Skip, laat AI beslissen
                else:
                    df.at[idx, 'regel_sectie'] = sectie
                    df.at[idx, 'regel_categorie'] = categorie
                    df.at[idx, 'regel_confidence'] = confidence
                    df.at[idx, 'classificatie_bron'] = 'rule'
                    n_geclassificeerd += 1
                    break

    # Statistieken
    n_totaal = len(df[~df.get('is_intern', False)])
    n_onzeker = n_totaal - n_geclassificeerd
    pct = (n_geclassificeerd / n_totaal * 100) if n_totaal > 0 else 0
    logger.info(f"Rule-based classificatie: {n_geclassificeerd}/{n_totaal} transacties ({pct:.0f}%) "
                f"geclassificeerd, {n_onzeker} naar AI")

    # Log de verdeling per sectie
    for sectie in ['inkomsten', 'vaste_lasten', 'variabele_kosten', 'sparen_beleggen', 'intern']:
        n = len(df[df['regel_sectie'] == sectie])
        if n > 0:
            bedrag = abs(df[df['regel_sectie'] == sectie]['bedrag'].sum())
            logger.info(f"  {sectie}: {n} transacties, EUR {bedrag:,.0f}")

    return df


def _detecteer_vast_inkomen(df: pd.DataFrame) -> pd.DataFrame:
    """Detecteer ALLE vormen van vast inkomen — GEEN hardcoded namen.

    Werkt voor ALLE Nederlandse huishoudens:
    - DGA's met Holding/Management B.V. → DGA-loon/Managementfee
    - Werknemers bij B.V./N.V./Stichting/Gemeente/Overheid → Netto salaris
    - Iedereen met "SALARIS"/"LOON" in transactieomschrijving → Netto salaris
    - Onbekende bron maar vast maandelijks patroon ≥€800 → Netto salaris

    Drie detectie-lagen (van meest naar minst betrouwbaar):
    1. KEYWORD: "SALARIS"/"LOON" in omschrijving → confidence 0.95
    2. RECHTSVORM: B.V./Stichting/N.V./Gemeente etc. → confidence 0.88-0.90
    3. PATROON: onbekende bron, ≥6x vast bedrag ≥€800 → confidence 0.80
    """
    if 'Omschrijving' not in df.columns:
        return df

    # Alleen niet-interne, niet-reeds-geclassificeerde, POSITIEVE transacties
    mask = (~df['is_intern']) & (df['classificatie_bron'].isna()) & (df['bedrag'] > 0)
    df_kandidaat = df[mask].copy()

    if len(df_kandidaat) == 0:
        return df

    # Bekende merchants uitsluiten
    bekende_merchants = set()
    for zoekterm, _, _, _ in MERCHANT_MAPPING:
        bekende_merchants.add(zoekterm)

    n_dga = 0
    n_salaris = 0
    n_keyword = 0
    gevonden_ibans = set()

    # =========================================================================
    # LAAG 1: KEYWORD — "SALARIS" / "LOON" in omschrijving
    # =========================================================================
    for idx, row in df_kandidaat.iterrows():
        omschr = str(row.get('Omschrijving', '')).upper()
        bedrag = float(row.get('bedrag', 0))

        salaris_keywords = ['SALARIS', ' LOON ', 'LOON/', '/LOON', 'SALARY',
                            'NETTO LOON', 'NETTOLOON', 'LOONBETALING',
                            'SALARISBETALING', 'MAANDLOON']
        if any(kw in omschr or omschr.startswith(kw.lstrip()) for kw in salaris_keywords):
            if bedrag >= 200:
                df.at[idx, 'regel_sectie'] = 'inkomsten'
                df.at[idx, 'regel_categorie'] = 'Netto salaris'
                df.at[idx, 'regel_confidence'] = 0.95
                df.at[idx, 'classificatie_bron'] = 'rule'
                n_keyword += 1
                if 'Tegenrekening' in df.columns:
                    tegen = _normaliseer_iban(str(row.get('Tegenrekening', '')))
                    if tegen:
                        gevonden_ibans.add(tegen)

    if n_keyword > 0:
        logger.info(f"SALARIS-KEYWORD: {n_keyword} transacties met 'SALARIS'/'LOON' in omschrijving")

    # Update kandidaten
    mask = (~df['is_intern']) & (df['classificatie_bron'].isna()) & (df['bedrag'] > 0)
    df_kandidaat = df[mask].copy()

    # =========================================================================
    # LAAG 2: RECHTSVORM — B.V., Stichting, N.V., Gemeente, etc.
    # =========================================================================
    groepeer_col = 'Tegenrekening' if 'Tegenrekening' in df.columns else 'Omschrijving'

    for key, groep in df_kandidaat.groupby(groepeer_col):
        key_str = str(key).upper().strip()

        if 'Tegenrekening' in df.columns:
            tegen_norm = _normaliseer_iban(key_str)
            if tegen_norm in gevonden_ibans:
                continue

        omschr_alle = ' '.join(groep['Omschrijving'].astype(str).str.upper())
        tekst_check = key_str + ' ' + omschr_alle

        is_merchant = any(m in tekst_check for m in bekende_merchants)
        if is_merchant:
            continue

        bv_markers = ['B.V.', ' BV ', ' BV,', 'B.V ', ' B.V', ' BV.']
        holding_markers = ['HOLDING', 'HLDG']
        werkgever_markers = ['STICHTING', 'GEMEENTE', 'MINISTERIE', 'PROVINCIE',
                             'UNIVERSITEIT', 'HOGESCHOOL', 'POLITIE', 'RIJKS',
                             'WATERSCHAP', 'GGD', 'GGZ', 'ZIEKENHUIS']
        nv_markers = ['N.V.', ' NV ', ' NV,', 'N.V ']
        mgmt_met_bedrijf = ('MANAGEMENT' in tekst_check and
                            any(m in tekst_check for m in bv_markers + holding_markers +
                                ['CONSULTANCY', 'ADVIES', 'DIENSTEN']))

        heeft_bv = any(marker in tekst_check for marker in bv_markers)
        heeft_holding = any(marker in tekst_check for marker in holding_markers)
        heeft_werkgever = any(marker in tekst_check for marker in werkgever_markers)
        heeft_nv = any(marker in tekst_check for marker in nv_markers)

        if not (heeft_bv or heeft_holding or mgmt_met_bedrijf or heeft_werkgever or heeft_nv):
            continue

        groep_pos = groep[groep['bedrag'] > 0]
        if len(groep_pos) < 3:
            continue

        bedragen = groep_pos['bedrag'].astype(float)
        gemiddeld = bedragen.mean()
        if gemiddeld < 500:
            continue

        std = bedragen.std()
        variatie = (std / gemiddeld) if gemiddeld > 0 else 1.0
        if variatie >= 0.25:
            continue

        is_dga = (heeft_holding or mgmt_met_bedrijf) and not heeft_werkgever
        heeft_salaris_kw = any(kw in tekst_check for kw in ['SALARIS', 'LOON', 'SALARY'])

        if is_dga and not heeft_salaris_kw:
            categorie = 'DGA-loon/Managementfee'
            confidence = 0.90
        else:
            categorie = 'Netto salaris'
            confidence = 0.88

        mask_match = df.index.isin(groep_pos.index)
        df.loc[mask_match, 'regel_sectie'] = 'inkomsten'
        df.loc[mask_match, 'regel_categorie'] = categorie
        df.loc[mask_match, 'regel_confidence'] = confidence
        df.loc[mask_match, 'classificatie_bron'] = 'rule'

        totaal = bedragen.sum()
        if categorie == 'DGA-loon/Managementfee':
            n_dga += len(groep_pos)
        else:
            n_salaris += len(groep_pos)
        if 'Tegenrekening' in df.columns:
            gevonden_ibans.add(_normaliseer_iban(key_str))
        logger.info(
            f"{'DGA-LOON' if is_dga else 'SALARIS'} GEDETECTEERD: {key_str[:50]} — "
            f"{len(groep_pos)}x, gem EUR {gemiddeld:,.0f}, {variatie*100:.0f}% var → {categorie}"
        )

    # =========================================================================
    # LAAG 3: PATROON — onbekende bron maar vast maandelijks bedrag
    # Strengere eisen: ≥6 betalingen, ≥€800, <20% variatie
    # =========================================================================
    mask = (~df['is_intern']) & (df['classificatie_bron'].isna()) & (df['bedrag'] > 0)
    df_rest = df[mask].copy()

    if len(df_rest) > 0 and 'Tegenrekening' in df.columns:
        df_rest['_tegen_norm'] = df_rest['Tegenrekening'].apply(_normaliseer_iban)
        df_rest = df_rest[(df_rest['_tegen_norm'] != '') & (~df_rest['_tegen_norm'].isin(gevonden_ibans))]

        for tegen_rek, groep in df_rest.groupby('_tegen_norm'):
            groep_pos = groep[groep['bedrag'] > 0]
            if len(groep_pos) < 6:
                continue

            omschr_check = ' '.join(groep_pos['Omschrijving'].astype(str).str.upper().head(5))
            if any(m in omschr_check for m in bekende_merchants):
                continue

            bedragen = groep_pos['bedrag'].astype(float)
            gemiddeld = bedragen.mean()
            if gemiddeld < 800:
                continue

            std = bedragen.std()
            variatie = (std / gemiddeld) if gemiddeld > 0 else 1.0
            if variatie < 0.20:
                mask_match = df.index.isin(groep_pos.index)
                df.loc[mask_match, 'regel_sectie'] = 'inkomsten'
                df.loc[mask_match, 'regel_categorie'] = 'Netto salaris'
                df.loc[mask_match, 'regel_confidence'] = 0.80
                df.loc[mask_match, 'classificatie_bron'] = 'rule'
                n_salaris += len(groep_pos)
                naam = groep_pos.iloc[0]['Omschrijving']
                logger.info(
                    f"SALARIS-PATROON: {tegen_rek} ({str(naam)[:30]}) — "
                    f"{len(groep_pos)}x, gem EUR {gemiddeld:,.0f}, {variatie*100:.0f}% var"
                )

    totaal_gevonden = n_keyword + n_dga + n_salaris
    if totaal_gevonden > 0:
        logger.info(f"VAST INKOMEN: {totaal_gevonden} tx — {n_keyword} keyword, {n_dga} DGA, {n_salaris} salaris")
    else:
        logger.info("VAST INKOMEN: geen vast inkomen gedetecteerd")

    return df


def _detecteer_huurinkomsten(df: pd.DataFrame) -> pd.DataFrame:
    """Detecteer huurinkomsten op basis van patronen — GEEN hardcoded namen.

    Werkt voor ALLE verhuurders in Nederland.

    Heuristiek:
    1. Zoek tegenpartijen met regelmatige POSITIEVE betalingen (minstens 4x)
    2. Die GEEN bedrijf zijn (niet in merchant mapping, geen B.V./Holding)
    3. Met een (semi-)vast bedrag (std < 25% van gemiddelde)
    4. Waar het geld OVERWEGEND één kant op gaat (max 15% negatief)
       → relaxer dan 0%, want soms geeft een verhuurder eenmalig borg terug
    5. Met een gemiddeld bedrag van minstens €300 (realistisch voor huur)

    Fallback: als Tegenrekening leeg is, groepeer op genormaliseerde naam
    uit de omschrijving (eerste woorden, hoofdletters).

    Classificeert als: inkomsten / Huurinkomsten
    """
    # Alleen niet-interne, niet-reeds-geclassificeerde, POSITIEVE transacties
    mask = (~df['is_intern']) & (df['classificatie_bron'].isna()) & (df['bedrag'] > 0)
    df_kandidaat = df[mask].copy()

    if len(df_kandidaat) == 0:
        return df

    # Bekende merchants uitsluiten
    bekende_merchants = set()
    for zoekterm, _, _, _ in MERCHANT_MAPPING:
        bekende_merchants.add(zoekterm)

    # Rechtsvorm-markers (al afgevangen door DGA-loon detectie)
    rechtsvorm_markers = ['B.V.', ' BV ', ' BV,', 'B.V ', ' B.V', ' BV.',
                          'HOLDING', 'HLDG', 'STICHTING', 'VERENIGING', 'N.V.', ' NV ']

    n_gedetecteerd = 0

    # Bepaal groepeersleutel: Tegenrekening (IBAN) als beschikbaar, anders naam
    heeft_tegenrek = 'Tegenrekening' in df.columns

    if heeft_tegenrek:
        df_kandidaat['_groep_key'] = df_kandidaat['Tegenrekening'].apply(_normaliseer_iban)
        # Fallback voor lege tegenrekening: gebruik eerste 3 woorden van omschrijving
        lege_mask = df_kandidaat['_groep_key'] == ''
        if lege_mask.any():
            df_kandidaat.loc[lege_mask, '_groep_key'] = (
                df_kandidaat.loc[lege_mask, 'Omschrijving']
                .astype(str).str.upper().str.split().str[:3].str.join(' ')
            )
    else:
        # Geen Tegenrekening kolom: groepeer op eerste 3 woorden van omschrijving
        df_kandidaat['_groep_key'] = (
            df_kandidaat['Omschrijving']
            .astype(str).str.upper().str.split().str[:3].str.join(' ')
        )

    df_kandidaat = df_kandidaat[df_kandidaat['_groep_key'] != '']

    for groep_key, groep in df_kandidaat.groupby('_groep_key'):
        if len(groep) < 4:
            continue

        # Check: geen rechtsvorm (die zijn al DGA-loon of bedrijf)
        omschr_alle = ' '.join(groep['Omschrijving'].astype(str).str.upper())
        is_rechtsvorm = any(m in omschr_alle for m in rechtsvorm_markers)
        if is_rechtsvorm:
            continue

        # Check: geen bekende merchant
        is_merchant = False
        for merchant_zoek in bekende_merchants:
            if merchant_zoek in omschr_alle:
                is_merchant = True
                break
        if is_merchant:
            continue

        # Check: OVERWEGEND unidirectioneel (max 15% negatieve transacties)
        # Dit is relaxer dan "absoluut geen negatief", want soms geeft een
        # verhuurder borg terug of corrigeert een bedrag.
        if heeft_tegenrek:
            # Zoek ALLE transacties (ook negatieve) van deze tegenpartij
            df['_temp_groep'] = df['Tegenrekening'].apply(_normaliseer_iban)
            lege_temp = df['_temp_groep'] == ''
            if lege_temp.any():
                df.loc[lege_temp, '_temp_groep'] = (
                    df.loc[lege_temp, 'Omschrijving']
                    .astype(str).str.upper().str.split().str[:3].str.join(' ')
                )
            alle_van_tegenpartij = df[
                (df['_temp_groep'] == groep_key) & (~df['is_intern'])
            ]
            df.drop(columns=['_temp_groep'], inplace=True)
        else:
            df['_temp_groep'] = (
                df['Omschrijving'].astype(str).str.upper().str.split().str[:3].str.join(' ')
            )
            alle_van_tegenpartij = df[
                (df['_temp_groep'] == groep_key) & (~df['is_intern'])
            ]
            df.drop(columns=['_temp_groep'], inplace=True)

        n_negatief = (alle_van_tegenpartij['bedrag'] < 0).sum()
        n_totaal_tp = len(alle_van_tegenpartij)
        pct_negatief = n_negatief / n_totaal_tp if n_totaal_tp > 0 else 0

        if pct_negatief > 0.15:
            continue  # Te veel geld terug = waarschijnlijk geen huur

        # Check: (semi-)vast bedrag (25% tolerantie)
        bedragen = groep['bedrag'].astype(float)
        gemiddeld = bedragen.mean()
        if gemiddeld < 300:
            continue  # Minder dan €300 gemiddeld is waarschijnlijk geen huur

        std = bedragen.std()
        variatie = (std / gemiddeld) if gemiddeld > 0 else 1.0
        if variatie < 0.25:
            # Vast bedrag, regelmatig, overwegend één richting = huurinkomsten
            mask_huur = df.index.isin(groep.index)
            df.loc[mask_huur, 'regel_sectie'] = 'inkomsten'
            df.loc[mask_huur, 'regel_categorie'] = 'Huurinkomsten'
            df.loc[mask_huur, 'regel_confidence'] = 0.85
            df.loc[mask_huur, 'classificatie_bron'] = 'rule'

            totaal = bedragen.sum()
            n_gedetecteerd += len(groep)
            naam = groep.iloc[0]['Omschrijving']
            logger.info(
                f"HUURINKOMSTEN GEDETECTEERD: {groep_key[:40]} ({str(naam)[:30]}) — "
                f"{len(groep)} betalingen, gemiddeld EUR {gemiddeld:,.0f}, "
                f"totaal EUR {totaal:,.0f}, {variatie*100:.0f}% variatie, "
                f"{pct_negatief*100:.0f}% negatief"
            )

    if n_gedetecteerd > 0:
        logger.info(f"HUURINKOMSTEN: {n_gedetecteerd} transacties als huurinkomsten geclassificeerd")
    else:
        logger.info("HUURINKOMSTEN: geen huurinkomsten patroon gedetecteerd")

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

def bouw_prompt(df: pd.DataFrame, feiten: dict, top: dict, eigen_rekeningen: set = None) -> str:
    # Splits interne transfers van echte transacties
    if 'is_intern' in df.columns:
        df_extern = df[~df['is_intern']].copy()
        df_intern = df[df['is_intern']].copy()
        n_intern = len(df_intern)
        bedrag_intern_in = round(float(df_intern[df_intern['bedrag'] > 0]['bedrag'].sum()), 2)
        bedrag_intern_uit = round(float(df_intern[df_intern['bedrag'] < 0]['bedrag'].sum()), 2)
    else:
        df_extern = df.copy()
        df_intern = pd.DataFrame()
        n_intern = 0
        bedrag_intern_in = 0
        bedrag_intern_uit = 0

    # Geen harde limiet meer — moderne LLMs (Claude Opus, GPT-5.4) hebben
    # 200K+ context window. 3000 transacties × ~200 tokens = 600K tokens, past ruim.
    if len(df_extern) > 5000:
        logger.warning(f"Bestand bevat {len(df_extern)} externe transacties — gelimiteerd tot 5000")
        df_extern = df_extern.head(5000)

    # Transacties die rule-based als 'intern' zijn geclassificeerd (Tikkie, creditcard)
    # worden ook uit de lijst voor AI verwijderd
    if 'regel_sectie' in df_extern.columns:
        intern_regel_mask = df_extern['regel_sectie'] == 'intern'
        n_intern_regel = intern_regel_mask.sum()
        if n_intern_regel > 0:
            logger.info(f"Prompt: {n_intern_regel} regel-intern transacties (Tikkie/creditcard) verwijderd uit AI-lijst")
            df_extern = df_extern[~intern_regel_mask].copy()

    regels = []
    for _, row in df_extern.iterrows():
        # Als rule-based geclassificeerd: voeg classificatie toe als hint voor AI
        pre_class = ''
        if row.get('classificatie_bron') == 'rule' and row.get('regel_categorie'):
            pre_class = f'|[REGEL:{row["regel_sectie"]}:{row["regel_categorie"]}]'
        regels.append(
            f"{row['datum'].strftime('%Y-%m-%d')}|{row['Rekeningnummer']}|"
            f"{row['bedrag']:>10.2f}|{str(row['Omschrijving'])[:200]}{pre_class}"
        )

    # Eigen rekeningen info voor in de prompt
    eigen_rek_tekst = ""
    if eigen_rekeningen:
        eigen_rek_tekst = "\n## EIGEN REKENINGNUMMERS VAN DE KLANT\n"
        eigen_rek_tekst += "De klant heeft de volgende rekeningen (ALLE rekeningen hieronder zijn van dezelfde persoon/huishouden):\n"
        for rek in sorted(eigen_rekeningen):
            eigen_rek_tekst += f"- {rek}\n"
        eigen_rek_tekst += "\nBELANGRIJK: Overboekingen tussen twee rekeningen uit deze lijst zijn INTERNE VERSCHUIVINGEN.\n"
        eigen_rek_tekst += "Deze zijn AL verwijderd uit de transactielijst hieronder. Ze worden apart getoond in het rapport.\n"

    # Interne transfers samenvatting
    intern_tekst = ""
    if n_intern > 0:
        intern_tekst = f"""
## INTERNE VERSCHUIVINGEN (al verwijderd uit onderstaande transacties)
Er zijn {n_intern} interne overboekingen gedetecteerd tussen eigen rekeningen.
Totaal bijschrijvingen (ontvangst eigen rekening): EUR {bedrag_intern_in:,.2f}
Totaal afschrijvingen (verzending eigen rekening): EUR {bedrag_intern_uit:,.2f}
Deze tellen NIET mee als inkomen of uitgave. Ze worden apart in het rapport vermeld.
"""

    # Tel pre-geclassificeerde transacties
    n_preclassified = len(df_extern[df_extern.get('classificatie_bron', pd.Series()) == 'rule']) if 'classificatie_bron' in df_extern.columns else 0

    return f"""Je bent een financieel analist voor vermogende particulieren en DGA's in Nederland.
Hieronder staan {len(df_extern)} banktransacties (interne overboekingen, Tikkies en creditcard-aflossingen zijn al verwijderd).
{eigen_rek_tekst}{intern_tekst}

## PRE-CLASSIFICATIE
Van de {len(df_extern)} transacties zijn er {n_preclassified} al rule-based geclassificeerd.
Deze transacties hebben een [REGEL:sectie:Categorie] tag achter de omschrijving, bv [REGEL:variabele_kosten:Benzine/Diesel/Laden].
ABSOLUUT VERPLICHT:
- Je MOET deze classificaties EXACT overnemen — zowel de SECTIE als de CATEGORIE.
- [REGEL:variabele_kosten:Benzine/Diesel/Laden] → zet in variabele_kosten als "Benzine/Diesel/Laden", ZELFS als het bedrag positief is (dat is een terugbetaling/refund).
- [REGEL:vaste_lasten:Energie] → zet in vaste_lasten als "Energie", ZELFS als het bedrag positief is.
- [REGEL:inkomsten:Netto salaris] → zet in inkomsten als "Netto salaris".
- Een positief bedrag bij een niet-inkomsten-categorie is een TERUGBETALING, geen inkomen. Zet het in de sectie die de [REGEL:] tag aangeeft.
- Je mag [REGEL:...] transacties NIET verplaatsen naar een andere sectie of categorie.
- Categoriseer alleen de transacties ZONDER [REGEL:...] tag.

## REGELS
1. Categoriseer ELKE transactie in precies één categorie uit onderstaande lijst.
   Gebruik EXACT deze categorienamen (niet afwijken, niet samenvoegen, niet verzinnen).
   Als een transactie nergens past, gebruik dan de "Overig" variant van de juiste sectie.
   BELANGRIJK: "Overig" categorieën mogen MAXIMAAL 5% van het totaalbedrag per sectie bevatten.
   Als er veel in "Overig" dreigt te belanden, kies dan de best passende bestaande categorie.

## CONSISTENTIE — KRITIEK
- Dezelfde tegenpartij MOET ALTIJD dezelfde categorie krijgen.
- Zoek patronen: als "Sevi B.V." 12x voorkomt met vast bedrag, classificeer ze ALLEMAAL hetzelfde.
- RICHTING: Positief bedrag van een winkel/tankstation = Retour/Terugbetaling, NIET een inkomstencategorie.
  Positief bedrag van een B.V./Stichting/werkgever = wél inkomen.

## ABN AMRO TRANSACTIEFORMAAT
ABN-omschrijvingen zijn lang (150-200 tekens). De tegenpartijnaam staat vaak PAS NA "Naam:" of "NAME/".
Voorbeeld: "/TRTP/SEPA OVERBOEKING/IBAN/NL50BUNQ2208185153/BIC/BUNQNL2A/NAME/Sevi B.V./REMI/2025 12 Sevi"
→ De naam "Sevi B.V." staat pas op positie ~90. LEES ALTIJD DE HELE OMSCHRIJVING.
ING-omschrijvingen bevatten de naam juist aan het BEGIN.

2. INKOMSTEN (12 categorieën):
   - Netto salaris (loon van werkgever of eigen BV)
   - UWV/Uitkeringen (WW, WIA, Ziektewet, bijstand)
   - DGA-loon/Managementfee (vanuit eigen BV)
   - Huurinkomsten (ontvangen huur van huurders)
   - Toeslagen (zorgtoeslag, huurtoeslag, kindgebonden budget)
   - Belastingteruggave (teruggave IB, BTW, voorlopige aanslag)
   - Kinderbijslag/Kindregelingen
   - Effectenrekening (terugstorting) (geld terug van Saxo, DeGiro, broker → GEEN inkomen, wel bijschrijving)
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

## CATEGORISATIE-HINTS (generiek voor Nederlandse huishoudens)
- UWV → UWV/Uitkeringen
- Saxo Bank / DeGiro / IBKR → Effectenrekening
- Mintos / Lendahand / PeerBerry → Crowdlending
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
- GIVT / KWF / Partij voor de Dieren / Rode Kruis / Oxfam → Donaties/Goede doelen
- Thuisbezorgd / Uber Eats / Deliveroo → Afhaal/Bezorging
- Uber / Bolt (taxi) → Taxi/Uber
- Booking.com / Airbnb / Transavia / KLM / Ryanair → Vakantie/Reizen
- Action / HEMA / IKEA huishoudelijk → Huishoudelijke artikelen
- IKEA meubels/inrichting → Meubels/Inrichting
- Apotheek / BENU → Apotheek/Medicijnen
- SALARIS/LOON UIT B.V.: Een vast maandelijks bedrag van een B.V. (bevat "B.V." of "BV" in de naam) is waarschijnlijk salaris. Als de B.V.-naam "Holding" of "Management" bevat → classificeer als "DGA-loon/Managementfee". Anders → classificeer als "Netto salaris". Neem NIET aan dat iemand eigenaar is van een B.V. alleen omdat die persoon geld ontvangt.
- HUURINKOMSTEN HERKENNING: Regelmatige (maandelijkse) bijschrijvingen van dezelfde persoon met een vast bedrag zijn waarschijnlijk huurinkomsten.

## NEDERLANDSE FINANCIELE CONTEXT
- HYPOTHEEK-GEKOPPELDE VERZEKERINGEN: Maandelijkse betalingen aan ASR, Nationale-Nederlanden, Aegon, Delta Lloyd, VIVAT, Reaal, a.s.r., NN Group die een levensverzekering of kapitaalverzekering betreffen, zijn in Nederland bijna altijd onderdeel van een hypotheekconstructie (spaarhypotheek, beleggingshypotheek). Categoriseer als Hypotheek/Huur, NIET als Sparen & Beleggen. Vermeld in de analyse dat het waarschijnlijk een hypotheek-gekoppelde verzekering betreft.
- INTERNE VERSCHUIVINGEN: Overboekingen tussen eigen rekeningen zijn AL verwijderd uit de transactielijst. Ze tellen NIET mee als inkomen of uitgave. Categoriseer NOOIT een transactie als "Overig inkomen" als het eigenlijk een interne overboeking is.
- SALARIS UIT B.V.: Een maandelijks vast bedrag van een BV (herkenbaar aan "B.V." of "BV" in de naam) is waarschijnlijk salaris of DGA-loon, niet "Overig inkomen". Alleen als "Holding" of "Management" in de naam staat → "DGA-loon/Managementfee". Anders → "Netto salaris".
- CREDITCARD-AFLOSSING: Een betaling aan een creditcardmaatschappij (ICS, VISA, Mastercard, American Express) is geen consumptie maar een aflossing. Categoriseer als Interne verschuivingen of negeer als de onderliggende transacties al apart staan.

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
    "samenvatting": "3-4 alinea's. Schrijf als een senior financieel adviseur die een vermogende particulier of DGA informeert — rustig, zakelijk, respectvol. Begin met het totaalbeeld: hoeveel komt er structureel binnen, hoeveel gaat er structureel uit, hoeveel gaat naar vermogensopbouw. Benoem dan de cashflowdynamiek: zijn er grote interne verschuivingen, beleggingstransacties, of seizoenseffecten die het beeld vertekenen? Eindig met de kern: waar zit de financiele kracht en waar de kwetsbaarheid. Noem altijd concrete bedragen. TOON-REGELS: (1) Gebruik NOOIT een oordelende of budgetcoach-achtige toon. Gebruik neutrale financiele taal. (2) Wees EERLIJK over onzekerheden: als een classificatie onzeker is, zeg dat. Schrijf 'Op basis van de transactiepatronen lijkt dit...' in plaats van stellige beweringen. (3) Spreek de gebruiker NOOIT aan met 'uw BV' tenzij je ZEKER weet dat het een eigen BV is. (4) Als er grote bedragen zijn die niet eenduidig te classificeren zijn, benoem dit expliciet als punt van aandacht. Een betrouwbaar rapport dat eerlijk is over zijn beperkingen is meer waard dan een zelfverzekerd rapport dat fouten bevat.",
    "sterke_punten": ["Noem 3-5 financiele sterktes met concrete bedragen. Schrijf bevestigend en zakelijk, bv: 'Stabiel structureel inkomen van gemiddeld €X/mnd via DGA-loon en huurinkomsten', 'Actieve vermogensopbouw: gemiddeld €X/mnd naar beleggingen en pensioen'"],
    "aandachtspunten": ["Noem 3-5 signalen die aandacht verdienen. Gebruik GEEN oordelende taal. Schrijf als observaties, bv: 'Discretionaire uitgaven vertonen maandelijkse spreiding van €X tot €Y — mogelijke grip-verbetering', 'Liquiditeitsmarge na vaste lasten en vermogensopbouw is beperkt tot ca. €X/mnd'"],
    "aanbevelingen": ["Geef 3-5 concrete, strategische aanbevelingen. Denk op het niveau van financieel advies, niet budgetcoaching. Bv: 'Overweeg een liquiditeitsbuffer van 3-6 maanden vaste lasten (ca. €X) aan te houden alvorens extra beleggingen', 'Consolidatie van beleggingsrekeningen kan beheerkosten en overzicht verbeteren'"],
    "verrassende_inzichten": ["Geef 2-3 patronen of inzichten die een drukke vermogende particulier NIET zelf zou zien maar die een AI wel opvalt. Denk aan: seizoenspatronen in cashflow, verborgen belastingoptimalisatie-mogelijkheden, structurele mismatch tussen inkomen en vermogensopbouw, ongewone correlaties, DGA-loon vs dividendoptimalisatie, effectief belastingtarief dat te hoog lijkt, categorieën die relatief hoog zijn vergeleken met vergelijkbare huishoudens. Dit is de WOW-factor van het rapport — maak het concreet met bedragen en vertel iets dat de klant verrast."]
  }}
}}"""


def _rapport_kwaliteitscheck(data: dict, df: pd.DataFrame, eigen_rekeningen: set):
    """Blokkeer rapport als er grove classificatiefouten in zitten.

    Controleert (BLOKKEREND - rapport wordt niet gegenereerd):
    1. 'Overig inkomen' > 40% van totaal inkomen → classificatiefout
    2. Transfer in inkomen → AI heeft interne transfer als inkomen gezet
    3. Groot bedrag (>€2.000/jaar) in AI-only categorie zonder rule-backup

    Controleert (WAARSCHUWING - rapport wordt gegenereerd met disclaimer):
    4. 'Overig inkomen' > 15% van totaal inkomen
    5. Hypotheek-verzekeringen in beleggingen
    6. Hoge AI-afhankelijkheid (>60% transacties alleen door AI geclassificeerd)

    Raises ValueError als een blokkerende check faalt.
    Retourneert lijst met waarschuwingen voor disclaimer in rapport.
    """
    blockers = []
    warnings_found = []

    jaartotalen = data.get('jaartotalen', {})

    # =========================================================================
    # BLOKKERENDE CHECKS — rapport wordt NIET gegenereerd
    # =========================================================================

    # Check 1: 'Overig inkomen' mag niet groter zijn dan 40% van totaal inkomen
    for rek, totalen in jaartotalen.items():
        inkomsten = totalen.get('inkomsten', {})
        if isinstance(inkomsten, dict):
            overig = abs(float(inkomsten.get('Overig inkomen', 0)))
            totaal_ink = sum(abs(float(v)) for v in inkomsten.values() if isinstance(v, (int, float)))
            if totaal_ink > 0 and overig > 0:
                ratio = overig / totaal_ink
                if ratio > 0.40:
                    blockers.append(
                        f"BLOKKADE: Rekening {rek}: 'Overig inkomen' is {ratio:.0%} van totaal inkomen "
                        f"(EUR {overig:,.0f} / EUR {totaal_ink:,.0f}). "
                        f"Dit duidt op fout-geclassificeerde interne overboekingen."
                    )
                elif ratio > 0.15:
                    warnings_found.append(
                        f"Rekening {rek}: 'Overig inkomen' is {ratio:.0%} van totaal inkomen "
                        f"(EUR {overig:,.0f}). Mogelijk zijn niet alle inkomstenbronnen herkend."
                    )

    # Check 2: Transfer in inkomen of variabele kosten
    # Controleer of eigen rekening-IBANs voorkomen als tegenpartij in AI-geclassificeerde transacties
    if eigen_rekeningen and 'Tegenrekening' in df.columns:
        df_ai_only = df[(df['classificatie_bron'] != 'rule') & (~df['is_intern'])]
        for idx, row in df_ai_only.iterrows():
            tegen = _normaliseer_iban(str(row.get('Tegenrekening', '')))
            if tegen in eigen_rekeningen:
                bedrag = abs(float(row.get('bedrag', 0)))
                if bedrag > 100:
                    blockers.append(
                        f"BLOKKADE: Transfer naar eigen rekening ({tegen}) "
                        f"niet als intern gemarkeerd (EUR {bedrag:,.0f}). "
                        f"Dit mag nooit in het rapport als inkomen of uitgave staan."
                    )
                    break  # Eén blocker is genoeg

    # =========================================================================
    # WAARSCHUWINGS-CHECKS — rapport wordt gegenereerd MET disclaimer
    # =========================================================================

    # Check 3: Hypotheek-verzekeringen in beleggingen
    for rek, totalen in jaartotalen.items():
        sparen = totalen.get('sparen_beleggen', {})
        if isinstance(sparen, dict):
            for cat, bedrag in sparen.items():
                cat_lower = cat.lower()
                if ('levensverzekering' in cat_lower or 'kapitaalverzekering' in cat_lower):
                    bedrag_abs = abs(float(bedrag)) if isinstance(bedrag, (int, float)) else 0
                    if bedrag_abs > 500:
                        warnings_found.append(
                            f"'{cat}' (EUR {bedrag_abs:,.0f}) staat onder Sparen & Beleggen. "
                            f"Levensverzekeringen zijn in Nederland vaak hypotheek-gekoppeld."
                        )

    # Check 4: Confidence / AI-afhankelijkheid
    df_extern = df[~df.get('is_intern', False)]
    n_totaal = len(df_extern)
    n_rule = len(df_extern[df_extern['classificatie_bron'] == 'rule'])
    n_ai = n_totaal - n_rule
    pct_ai = (n_ai / n_totaal * 100) if n_totaal > 0 else 0
    if pct_ai > 60:
        warnings_found.append(
            f"{pct_ai:.0f}% van de transacties is alleen door AI geclassificeerd "
            f"({n_ai} van {n_totaal}). Overweeg meer merchants toe te voegen aan de regelslaag."
        )

    # Check 5: Grote AI-only bedragen (>€2.000/jaar per categorie)
    # Dit zijn potentieel onbetrouwbare classificaties
    # (voor nu waarschuwing, later mogelijk blokkade)
    if n_ai > 0:
        df_ai_bedragen = df_extern[df_extern['classificatie_bron'] != 'rule']
        if len(df_ai_bedragen) > 0:
            groot_ai_totaal = df_ai_bedragen['bedrag'].apply(lambda x: abs(float(x))).sum()
            if groot_ai_totaal > 5000:
                warnings_found.append(
                    f"EUR {groot_ai_totaal:,.0f} aan transacties is alleen door AI geclassificeerd "
                    f"zonder rule-based backup. Bij grote bedragen kan dit onbetrouwbaar zijn."
                )

    # =========================================================================
    # RESULTAAT
    # =========================================================================

    # Blokkerende fouten → rapport NIET genereren
    if blockers:
        for b in blockers:
            logger.error(f"KWALITEITSCHECK: {b}")
        raise ValueError(
            f"Rapport geblokkeerd door {len(blockers)} kwaliteitscheck(s): "
            + " | ".join(blockers)
        )

    # Waarschuwingen → rapport WEL genereren, maar loggen
    if warnings_found:
        logger.warning(f"KWALITEITSCHECK: {len(warnings_found)} waarschuwing(en)")
        for w in warnings_found:
            logger.warning(f"  ⚠ {w}")
        # Sla waarschuwingen op zodat ze in het rapport kunnen worden verwerkt
        data['_kwaliteitswaarschuwingen'] = warnings_found
    else:
        logger.info("KWALITEITSCHECK: alle checks geslaagd, geen waarschuwingen")
        data['_kwaliteitswaarschuwingen'] = []

    return warnings_found


def _forceer_rule_classificaties(ai_data: dict, df: pd.DataFrame) -> dict:
    """Overschrijf AI-output met rule-based classificaties.

    Dit is de DEFINITIEVE correctie: als de rule-based engine een transactie heeft
    geclassificeerd, dan is dat definitief — ongeacht wat de AI ervan vindt.

    Werkt op de jaartotalen en maandoverzicht: herberekent de bedragen per categorie
    op basis van de rule-based tags in het DataFrame.
    """
    if 'classificatie_bron' not in df.columns:
        return ai_data

    # Alleen externe transacties die rule-based geclassificeerd zijn
    df_regel = df[(df['classificatie_bron'] == 'rule') & (~df.get('is_intern', False))].copy()
    # Filter ook regel_sectie == 'intern' eruit (die zitten niet in de AI output)
    df_regel = df_regel[df_regel['regel_sectie'] != 'intern']

    if len(df_regel) == 0:
        logger.info("POST-PROCESSING: geen rule-based transacties om te corrigeren")
        return ai_data

    logger.info(f"POST-PROCESSING: {len(df_regel)} rule-based transacties forceren in AI-output")

    # Bouw een correctie-tabel: per rekening, per sectie, per categorie → som bedragen
    correcties = {}  # {rekening: {sectie: {categorie: bedrag}}}
    correcties_maand = {}  # {rekening: {maand: {sectie: {categorie: {bedrag, aantal}}}}}

    for _, row in df_regel.iterrows():
        rek = str(row['Rekeningnummer'])
        sectie = row['regel_sectie']
        cat = row['regel_categorie']
        bedrag = float(row['bedrag'])
        maand = row['datum'].strftime('%Y-%m') if hasattr(row['datum'], 'strftime') else str(row.get('maand', ''))

        # Jaartotalen
        if rek not in correcties:
            correcties[rek] = {}
        if sectie not in correcties[rek]:
            correcties[rek][sectie] = {}
        correcties[rek][sectie][cat] = correcties[rek][sectie].get(cat, 0) + bedrag

        # Maandoverzicht
        if rek not in correcties_maand:
            correcties_maand[rek] = {}
        if maand not in correcties_maand[rek]:
            correcties_maand[rek][maand] = {}
        if sectie not in correcties_maand[rek][maand]:
            correcties_maand[rek][maand][sectie] = {}
        if cat not in correcties_maand[rek][maand][sectie]:
            correcties_maand[rek][maand][sectie][cat] = {'bedrag': 0, 'aantal': 0}
        correcties_maand[rek][maand][sectie][cat]['bedrag'] += bedrag
        correcties_maand[rek][maand][sectie][cat]['aantal'] += 1

    # Nu: verwijder deze bedragen uit de VERKEERDE AI-secties en zet ze in de JUISTE
    jaartotalen = ai_data.get('jaartotalen', {})
    maandoverzicht = ai_data.get('maandoverzicht', {})

    for rek, secties in correcties.items():
        if rek not in jaartotalen:
            jaartotalen[rek] = {}

        for sectie, cats in secties.items():
            if sectie not in jaartotalen[rek]:
                jaartotalen[rek][sectie] = {}

            for cat, bedrag in cats.items():
                bedrag_rounded = round(bedrag, 2)

                # Stap 1: Verwijder dit bedrag uit ANDERE secties waar de AI het misschien heeft gezet
                for andere_sectie in ['inkomsten', 'vaste_lasten', 'variabele_kosten', 'sparen_beleggen']:
                    if andere_sectie == sectie:
                        continue
                    ai_secties = jaartotalen[rek].get(andere_sectie, {})
                    if isinstance(ai_secties, dict) and cat in ai_secties:
                        ai_bedrag = float(ai_secties[cat]) if isinstance(ai_secties[cat], (int, float)) else 0
                        # Verwijder het rule-bedrag uit de verkeerde sectie
                        rest = round(ai_bedrag - bedrag_rounded, 2)
                        if abs(rest) < 0.01:
                            del ai_secties[cat]
                            logger.info(f"POST-PROCESSING: '{cat}' (EUR {bedrag_rounded:,.2f}) verwijderd uit {andere_sectie} voor {rek}")
                        else:
                            ai_secties[cat] = rest
                            logger.info(f"POST-PROCESSING: '{cat}' deels gecorrigeerd in {andere_sectie} voor {rek}: EUR {ai_bedrag:,.2f} → EUR {rest:,.2f}")

                # Stap 2: Zet het bedrag in de JUISTE sectie
                huidige = float(jaartotalen[rek][sectie].get(cat, 0)) if isinstance(jaartotalen[rek][sectie], dict) else 0
                if isinstance(jaartotalen[rek][sectie], dict):
                    jaartotalen[rek][sectie][cat] = round(huidige + bedrag_rounded, 2) if huidige == 0 else round(bedrag_rounded, 2)
                    logger.info(f"POST-PROCESSING: '{cat}' (EUR {bedrag_rounded:,.2f}) gezet in {sectie} voor {rek}")

    # Maandoverzicht corrigeren (zelfde logica maar per maand)
    for rek, maanden in correcties_maand.items():
        if rek not in maandoverzicht:
            maandoverzicht[rek] = {}
        for maand, secties in maanden.items():
            if maand not in maandoverzicht[rek]:
                maandoverzicht[rek][maand] = {}
            for sectie, cats in secties.items():
                if sectie not in maandoverzicht[rek][maand]:
                    maandoverzicht[rek][maand][sectie] = {}
                for cat, data in cats.items():
                    bedrag_rounded = round(data['bedrag'], 2)
                    aantal = data['aantal']

                    # Verwijder uit verkeerde secties
                    for andere_sectie in ['inkomsten', 'vaste_lasten', 'variabele_kosten', 'sparen_beleggen']:
                        if andere_sectie == sectie:
                            continue
                        ai_maand_sectie = maandoverzicht[rek][maand].get(andere_sectie, {})
                        if isinstance(ai_maand_sectie, dict) and cat in ai_maand_sectie:
                            del ai_maand_sectie[cat]

                    # Zet in juiste sectie
                    if isinstance(maandoverzicht[rek][maand][sectie], dict):
                        maandoverzicht[rek][maand][sectie][cat] = {
                            'bedrag': bedrag_rounded,
                            'aantal': aantal
                        }

    ai_data['jaartotalen'] = jaartotalen
    ai_data['maandoverzicht'] = maandoverzicht

    logger.info("POST-PROCESSING: rule-based correcties toegepast op AI-output")
    return ai_data


def _vraag_claude(prompt: str, model: str) -> dict:
    """Roep Anthropic Claude API aan."""
    from anthropic import Anthropic

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY niet geconfigureerd")

    import httpx
    client = Anthropic(
        api_key=api_key,
        timeout=httpx.Timeout(600.0, connect=30.0),
    )

    logger.info(f"Claude aanroepen ({model}), prompt: {len(prompt)} tekens (~{len(prompt)//4} tokens)")

    response = client.messages.create(
        model=model,
        max_tokens=32000,
        messages=[{"role": "user", "content": prompt}],
    )

    tekst = response.content[0].text
    tokens_in = response.usage.input_tokens
    tokens_out = response.usage.output_tokens
    logger.info(f"Claude klaar: {tokens_in} in, {tokens_out} out")

    return tekst, tokens_in, tokens_out


def _vraag_openai(prompt: str, model: str) -> dict:
    """Roep OpenAI GPT API aan."""
    from openai import OpenAI

    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OPENAI_API_KEY niet geconfigureerd — stel in via Railway Variables")

    client = OpenAI(
        api_key=api_key,
        timeout=600.0,
    )

    logger.info(f"OpenAI aanroepen ({model}), prompt: {len(prompt)} tekens (~{len(prompt)//4} tokens)")

    response = client.chat.completions.create(
        model=model,
        max_completion_tokens=32000,
        messages=[{"role": "user", "content": prompt}],
    )

    tekst = response.choices[0].message.content
    tokens_in = response.usage.prompt_tokens
    tokens_out = response.usage.completion_tokens
    logger.info(f"OpenAI klaar: {tokens_in} in, {tokens_out} out")

    return tekst, tokens_in, tokens_out


# Model configuratie:
# AI_PROVIDER = 'claude' of 'openai'
# CLAUDE_MODEL = 'claude-opus-4-6' (default)
# OPENAI_MODEL = 'gpt-5.4' (default)
# Kwaliteit > snelheid. Altijd het slimste model.

def vraag_ai(prompt: str) -> dict:
    """Generieke AI-aanroep — kiest automatisch Claude of GPT op basis van AI_PROVIDER env var."""
    provider = os.environ.get('AI_PROVIDER', 'claude').lower()

    if provider == 'openai':
        model = os.environ.get('OPENAI_MODEL', 'gpt-5.4')
        tekst, tokens_in, tokens_out = _vraag_openai(prompt, model)
    else:
        model = os.environ.get('CLAUDE_MODEL', 'claude-opus-4-6')
        tekst, tokens_in, tokens_out = _vraag_claude(prompt, model)

    # Parse JSON uit response
    if '```json' in tekst:
        tekst = tekst.split('```json')[1].split('```')[0]
    elif '```' in tekst:
        tekst = tekst.split('```')[1].split('```')[0]

    try:
        return {
            'data': json.loads(tekst),
            'tokens': {'input': tokens_in, 'output': tokens_out},
            'model': model,
            'provider': provider,
        }
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        return {
            'data': None,
            'raw': tekst[:2000],
            'error': str(e),
            'tokens': {'input': tokens_in, 'output': tokens_out},
            'model': model,
            'provider': provider,
        }


# Backward compatible alias
def vraag_claude(prompt: str) -> dict:
    return vraag_ai(prompt)


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

        # 1a2. IBAN extractie uit omschrijvingen (essentieel voor ABN XLSX)
        df = _vul_lege_tegenrekening(df)

        # 1b. Huishoudregister & interne-overboekingen detectie
        update('Eigen rekeningen herkennen...', 17)
        eigen_rekeningen = _bouw_huishoudregister(df)
        df = _markeer_interne_transfers(df, eigen_rekeningen)
        n_intern_rek = df['is_intern'].sum()
        update(f'{n_intern_rek} interne overboekingen (eigen rekeningen) gedetecteerd', 19)

        # 1b2. Huishoudleden-detectie (partner, kinderen — automatisch)
        update('Huishoudleden detecteren...', 20)
        df = _detecteer_huishoudleden(df)
        n_intern_totaal = df['is_intern'].sum()
        n_huishoud = n_intern_totaal - n_intern_rek
        if n_huishoud > 0:
            update(f'{n_huishoud} huishoudoverboeking(en) gedetecteerd', 21)

        # 1c. Rule-based classificatie (vóór AI)
        update('Transacties classificeren...', 22)
        df = _classificeer_rule_based(df)
        n_regel = len(df[df['classificatie_bron'] == 'rule'])
        update(f'{n_regel} transacties rule-based geclassificeerd', 24)

        # 1d. Patroon-detectie: vast inkomen en huurinkomsten (vóór AI)
        update('Vast inkomen en huurinkomsten detecteren...', 24)
        df = _detecteer_vast_inkomen(df)
        df = _detecteer_huurinkomsten(df)
        n_regel_na = len(df[df['classificatie_bron'] == 'rule'])
        n_patroon = n_regel_na - n_regel
        if n_patroon > 0:
            update(f'{n_patroon} extra transacties via patroondetectie', 25)
        else:
            update('Patroondetectie afgerond', 25)

        # 2. Deterministisch rekenen
        update('Bedragen berekenen en controleren...', 27)
        feiten = bereken_feiten(df)
        top = bereken_top(df)
        update(f'Feiten berekend voor {len(feiten)} rekening(en)', 30)

        # 3. AI categoriseren + analyseren (langste stap: 30s-300s)
        provider = os.environ.get('AI_PROVIDER', 'claude').lower()
        model_naam = os.environ.get('OPENAI_MODEL', 'gpt-5.4') if provider == 'openai' else os.environ.get('CLAUDE_MODEL', 'claude-opus-4-6')
        update(f'AI analyseert uw transacties ({model_naam})...', 35)
        prompt = bouw_prompt(df, feiten, top, eigen_rekeningen=eigen_rekeningen)
        logger.info(f"[{job_id}] Prompt: {len(prompt)} tekens, {len(df)} transacties")
        claude_result = vraag_claude(prompt)

        if not claude_result.get('data'):
            raise ValueError(f"AI-analyse ongeldig: {claude_result.get('error', 'onbekend')}")

        # 3b. Post-processing: forceer rule-based classificaties in AI-output
        claude_result['data'] = _forceer_rule_classificaties(claude_result['data'], df)
        update('Rule-based correcties toegepast', 65)

        # 3c. Report quality checks (blokkeer bij grove fouten, raises ValueError)
        kwaliteitswaarschuwingen = _rapport_kwaliteitscheck(claude_result['data'], df, eigen_rekeningen)
        if kwaliteitswaarschuwingen:
            update(f'AI-analyse compleet, {len(kwaliteitswaarschuwingen)} waarschuwing(en)', 70)
        else:
            update('AI-analyse compleet, kwaliteitscheck geslaagd', 70)

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
