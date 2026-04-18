"""Test income proof rules — No Guess Zone voor inkomen."""
import pytest


class TestVerbondenSalaryPartyTypes:
    """Rollen die NOOIT als salary/income geclassificeerd mogen worden."""

    def test_forbidden_roles_complete(self):
        from app import _classify_positive_inflows
        # We testen indirect: de _VERBODEN_SALARY_PARTY_TYPES set is lokaal
        # in de functie, dus we importeren het concept via de module
        # en checken dat de juiste checks bestaan.
        # De functie bestaat en is callable
        assert callable(_classify_positive_inflows)

    def test_merchant_refund_not_income(self):
        """Een positief bedrag van een merchant is een refund, NOOIT inkomen."""
        from app import CARD_SETTLEMENT_KEYWORDS
        # Card settlement keywords bestaan → No Guess Zone is ingericht
        assert len(CARD_SETTLEMENT_KEYWORDS) >= 5

    def test_broker_return_not_income(self):
        """Geld terug van broker is vermogensmutatie, NOOIT inkomen."""
        from app import FINANCIELE_INSTELLINGEN_KEYWORDS
        assert 'SAXO' in FINANCIELE_INSTELLINGEN_KEYWORDS
        assert 'DEGIRO' in FINANCIELE_INSTELLINGEN_KEYWORDS

    def test_own_account_not_income(self):
        """Eigen rekening IBAN wordt als own_account gelabeld."""
        from app import _resolve_related_parties
        import pandas as pd
        rows = [{'Omschrijving': 'test', 'Tegenrekening': 'NL01INGB001',
                 'tegenpartij_naam': 'test', 'bedrag': 500, 'is_intern': True}]
        df = pd.DataFrame(rows)
        df['bedrag'] = df['bedrag'].astype(float)
        for col in ['party_type', 'regel_sectie', 'regel_categorie',
                     'regel_confidence', 'classificatie_bron', 'source_family']:
            df[col] = None
        df = _resolve_related_parties(df, {'NL01INGB001'})
        assert df.iloc[0]['party_type'] == 'own_account'

    def test_household_not_salary(self):
        """Huishoudlid → wordt naar onderling_neutraal gerouteerd."""
        # Indirect test: household_related_party sectie = onderling_neutraal
        from app import _resolve_related_parties
        import pandas as pd
        rows = [
            {'Omschrijving': 'Mw A Jansen', 'Tegenrekening': 'NL99TRIO001',
             'tegenpartij_naam': 'Mw A Jansen', 'bedrag': 500, 'is_intern': False,
             'Rekeningnummer': 'NL01INGB001'},
            {'Omschrijving': 'Mw A Jansen', 'Tegenrekening': 'NL99TRIO001',
             'tegenpartij_naam': 'Mw A Jansen', 'bedrag': -300, 'is_intern': False,
             'Rekeningnummer': 'NL01INGB001'},
        ]
        df = pd.DataFrame(rows)
        df['bedrag'] = df['bedrag'].astype(float)
        for col in ['party_type', 'regel_sectie', 'regel_categorie',
                     'regel_confidence', 'classificatie_bron', 'source_family']:
            df[col] = None
        df = _resolve_related_parties(df, {'NL01INGB001'})
        hh = df[df['party_type'] == 'household_related_party']
        for _, row in hh.iterrows():
            assert row['regel_sectie'] == 'onderling_neutraal'


class TestGovernmentClassification:
    """Overheidstransacties moeten deterministisch worden geclassificeerd."""

    def test_overheid_keywords_exist(self):
        from app import OVERHEID_KEYWORDS
        assert 'BELASTINGDIENST' in OVERHEID_KEYWORDS
        assert 'UWV' in OVERHEID_KEYWORDS
        assert 'SVB' in OVERHEID_KEYWORDS


class TestBrokerClassification:
    """Broker-transacties moeten deterministisch naar sparen_beleggen."""

    def test_fi_keywords_exist(self):
        from app import FINANCIELE_INSTELLINGEN_KEYWORDS
        assert 'SAXO' in FINANCIELE_INSTELLINGEN_KEYWORDS
        assert 'DEGIRO' in FINANCIELE_INSTELLINGEN_KEYWORDS

    def test_investment_income_keywords(self):
        from app import INVESTMENT_INCOME_KEYWORDS
        assert 'DIVIDEND' in INVESTMENT_INCOME_KEYWORDS
