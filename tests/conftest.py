"""Shared test fixtures voor peterheijen-finance-api tests."""
import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_transactions():
    """Minimale DataFrame met realistische Nederlandse banktransacties."""
    data = [
        {'datum': '2025-06-01', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': 3500.00,
         'Omschrijving': 'STICHTING ZIEKENHUIS ABC — Salaris juni', 'Tegenrekening': 'NL99INGB9999999901',
         'tegenpartij_naam': 'STICHTING ZIEKENHUIS ABC', 'Beginsaldo': 5000.00, 'Eindsaldo': 8500.00,
         'is_intern': False},
        {'datum': '2025-06-01', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': -1200.00,
         'Omschrijving': 'OBVION HYPOTHEEK — Maandtermijn', 'Tegenrekening': 'NL99ABNA8888888801',
         'tegenpartij_naam': 'OBVION', 'Beginsaldo': 8500.00, 'Eindsaldo': 7300.00,
         'is_intern': False},
        {'datum': '2025-06-02', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': -150.00,
         'Omschrijving': 'ALBERT HEIJN 1234 — BEA', 'Tegenrekening': '',
         'tegenpartij_naam': 'ALBERT HEIJN', 'Beginsaldo': 7300.00, 'Eindsaldo': 7150.00,
         'is_intern': False},
        {'datum': '2025-06-03', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': 500.00,
         'Omschrijving': 'BELASTINGDIENST — Teruggave IB 2024', 'Tegenrekening': 'NL86INGB0002445588',
         'tegenpartij_naam': 'BELASTINGDIENST', 'Beginsaldo': 7150.00, 'Eindsaldo': 7650.00,
         'is_intern': False},
        {'datum': '2025-06-04', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': -500.00,
         'Omschrijving': 'DEGIRO — Storting effectenrekening', 'Tegenrekening': 'NL99DEGI0001234501',
         'tegenpartij_naam': 'DEGIRO', 'Beginsaldo': 7650.00, 'Eindsaldo': 7150.00,
         'is_intern': False},
        {'datum': '2025-06-05', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': 200.00,
         'Omschrijving': 'DEGIRO — Dividend VWRL', 'Tegenrekening': 'NL99DEGI0001234501',
         'tegenpartij_naam': 'DEGIRO', 'Beginsaldo': 7150.00, 'Eindsaldo': 7350.00,
         'is_intern': False},
        {'datum': '2025-06-06', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': -45.00,
         'Omschrijving': 'ICS/INT CARD SERVICES — Maandafrekening', 'Tegenrekening': 'NL99ICS00012345',
         'tegenpartij_naam': 'ICS', 'Beginsaldo': 7350.00, 'Eindsaldo': 7305.00,
         'is_intern': False},
        {'datum': '2025-06-07', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': 1000.00,
         'Omschrijving': 'Mw J Jansen — Overboeking', 'Tegenrekening': 'NL99RABO7777777701',
         'tegenpartij_naam': 'Mw J Jansen', 'Beginsaldo': 7305.00, 'Eindsaldo': 8305.00,
         'is_intern': True},
        {'datum': '2025-06-15', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': -200.00,
         'Omschrijving': 'CZ GROEP ZORGVERZEKERING — Premie juli', 'Tegenrekening': 'NL99ABNA6666666601',
         'tegenpartij_naam': 'CZ GROEP', 'Beginsaldo': 8305.00, 'Eindsaldo': 8105.00,
         'is_intern': False},
        {'datum': '2025-06-20', 'Rekeningnummer': 'NL01INGB0001234567', 'bedrag': 89.00,
         'Omschrijving': 'SVB — KINDERBIJSLAG Q2', 'Tegenrekening': 'NL99INGB5555555501',
         'tegenpartij_naam': 'SVB', 'Beginsaldo': 8105.00, 'Eindsaldo': 8194.00,
         'is_intern': False},
    ]
    df = pd.DataFrame(data)
    df['bedrag'] = df['bedrag'].astype(float)
    df['maand'] = pd.to_datetime(df['datum']).dt.strftime('%Y-%m')
    return df


@pytest.fixture
def eigen_rekeningen():
    return {'NL01INGB0001234567'}
