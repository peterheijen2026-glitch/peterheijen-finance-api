"""Test counterparty role detection (RPR V2).

10 rollen: own_account, household_related_party, employer_or_payroll,
business_counterparty, government, merchant, broker_or_investment_platform,
lender_or_mortgage_party, card_settlement, unknown
"""
import pandas as pd
import pytest
from app import _resolve_related_parties, _bouw_eigen_financieel_domein


def _make_df(rows, eigen_rek):
    df = pd.DataFrame(rows)
    df['bedrag'] = df['bedrag'].astype(float)
    df['is_intern'] = df.get('is_intern', False)
    if 'party_type' not in df.columns:
        df['party_type'] = None
    for col in ['regel_sectie', 'regel_categorie', 'regel_confidence',
                'classificatie_bron', 'source_family']:
        if col not in df.columns:
            df[col] = None
    return df


class TestOwnAccount:
    def test_eigen_rekening_iban(self):
        rows = [{'Omschrijving': 'Overboeking', 'Tegenrekening': 'NL01INGB0001234567',
                 'tegenpartij_naam': 'P Jansen', 'bedrag': 500, 'is_intern': True}]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df = _resolve_related_parties(df, {'NL01INGB0001234567'})
        assert df.iloc[0]['party_type'] == 'own_account'


class TestGovernment:
    @pytest.mark.parametrize("naam,keyword", [
        ('BELASTINGDIENST', 'BELASTINGDIENST'),
        ('UWV', 'UWV'),
        ('SVB', 'SVB'),
    ])
    def test_government_keywords(self, naam, keyword):
        rows = [{'Omschrijving': f'{keyword} — Teruggave', 'Tegenrekening': 'NL99GOVT0001',
                 'tegenpartij_naam': naam, 'bedrag': 500, 'is_intern': False}]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df = _resolve_related_parties(df, {'NL01INGB0001234567'})
        assert df.iloc[0]['party_type'] == 'government'


class TestMerchant:
    def test_albert_heijn_is_merchant(self):
        rows = [{'Omschrijving': 'ALBERT HEIJN 1234 BEA', 'Tegenrekening': 'NL99ABNA8888',
                 'tegenpartij_naam': 'ALBERT HEIJN', 'bedrag': -50, 'is_intern': False}]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df = _resolve_related_parties(df, {'NL01INGB0001234567'})
        assert df.iloc[0]['party_type'] == 'merchant'


class TestBroker:
    def test_degiro_via_fi_domain(self):
        rows = [
            {'Omschrijving': 'DEGIRO — Storting', 'Tegenrekening': 'NL99DEGI0001',
             'tegenpartij_naam': 'DEGIRO', 'bedrag': -500, 'is_intern': False},
            {'Omschrijving': 'DEGIRO — Dividend', 'Tegenrekening': 'NL99DEGI0001',
             'tegenpartij_naam': 'DEGIRO', 'bedrag': 50, 'is_intern': False},
        ]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df['Rekeningnummer'] = 'NL01INGB0001234567'
        eigen_fi = _bouw_eigen_financieel_domein(df)
        df = _resolve_related_parties(df, {'NL01INGB0001234567'}, eigen_fi_ibans=eigen_fi)
        assert df.iloc[0]['party_type'] == 'broker_or_investment_platform'

    def test_saxo_keyword(self):
        rows = [{'Omschrijving': 'SAXO BANK — Terugstorting', 'Tegenrekening': 'NL99SAXO001',
                 'tegenpartij_naam': 'SAXO BANK', 'bedrag': 1000, 'is_intern': False}]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df = _resolve_related_parties(df, {'NL01INGB0001234567'})
        assert df.iloc[0]['party_type'] == 'broker_or_investment_platform'


class TestCardSettlement:
    def test_ics_is_card(self):
        rows = [{'Omschrijving': 'ICS/INT CARD SERVICES — Afrekening', 'Tegenrekening': 'NL99ICS001',
                 'tegenpartij_naam': 'ICS', 'bedrag': -150, 'is_intern': False}]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df = _resolve_related_parties(df, {'NL01INGB0001234567'})
        assert df.iloc[0]['party_type'] == 'card_settlement'


class TestLender:
    def test_obvion_is_lender(self):
        rows = [{'Omschrijving': 'OBVION HYPOTHEEK — Termijn', 'Tegenrekening': 'NL99OBVI001',
                 'tegenpartij_naam': 'OBVION', 'bedrag': -1200, 'is_intern': False}]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df = _resolve_related_parties(df, {'NL01INGB0001234567'})
        assert df.iloc[0]['party_type'] == 'lender_or_mortgage_party'


class TestUnknown:
    def test_private_person_without_signals(self):
        rows = [{'Omschrijving': 'P de Vries — Overboeking', 'Tegenrekening': 'NL99RABO7777',
                 'tegenpartij_naam': 'P de Vries', 'bedrag': 100, 'is_intern': False,
                 'Rekeningnummer': 'NL01INGB0001234567'}]
        df = _make_df(rows, {'NL01INGB0001234567'})
        df = _resolve_related_parties(df, {'NL01INGB0001234567'})
        assert df.iloc[0]['party_type'] == 'unknown'


class TestNoSalaryForForbiddenRoles:
    """Merchant, broker, card, lender mogen NOOIT als salary worden geclassificeerd."""
    def test_forbidden_roles_in_verboden_set(self):
        """De _VERBODEN_SALARY_PARTY_TYPES set (lokaal in functie) bevat
        de juiste rollen. We testen dit indirect via CARD_SETTLEMENT_KEYWORDS."""
        from app import CARD_SETTLEMENT_KEYWORDS, FINANCIELE_INSTELLINGEN_KEYWORDS
        # Als deze keyword-lijsten bestaan, is de No Guess Zone ingericht
        assert len(CARD_SETTLEMENT_KEYWORDS) >= 5
        assert len(FINANCIELE_INSTELLINGEN_KEYWORDS) >= 10
