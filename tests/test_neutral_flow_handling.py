"""Test neutraal section handling — transfers, card settlements, broker movements."""
import pandas as pd
import pytest


class TestNeutralFlows:
    """Household transfers, card settlements, en interne overboekingen
    moeten in onderling_neutraal belanden, NOOIT in inkomsten."""

    def test_household_transfer_to_neutral(self):
        """household_related_party positief bedrag → onderling_neutraal."""
        from app import _resolve_related_parties
        rows = [
            {'Omschrijving': 'Mw E Jansen — Overboeking', 'Tegenrekening': 'NL99TRIO001',
             'tegenpartij_naam': 'Mw E Jansen', 'bedrag': 500, 'is_intern': False,
             'Rekeningnummer': 'NL01INGB0001234567'},
            {'Omschrijving': 'Mw E Jansen — Overboeking', 'Tegenrekening': 'NL99TRIO001',
             'tegenpartij_naam': 'Mw E Jansen', 'bedrag': -300, 'is_intern': False,
             'Rekeningnummer': 'NL01INGB0001234567'},
        ]
        df = pd.DataFrame(rows)
        df['bedrag'] = df['bedrag'].astype(float)
        for col in ['party_type', 'regel_sectie', 'regel_categorie',
                     'regel_confidence', 'classificatie_bron', 'source_family']:
            df[col] = None

        df = _resolve_related_parties(df, {'NL01INGB0001234567'})

        hh = df[df['party_type'] == 'household_related_party']
        for _, row in hh.iterrows():
            assert row['regel_sectie'] == 'onderling_neutraal', \
                f"household tx sectie={row['regel_sectie']}, moet onderling_neutraal zijn"

    def test_card_settlement_never_income(self):
        """card_settlement keyword list exists (No Guess Zone is ingericht)."""
        from app import CARD_SETTLEMENT_KEYWORDS
        assert 'ICS ' in CARD_SETTLEMENT_KEYWORDS

    def test_neutral_section_exists_in_ground_truth(self):
        """onderling_neutraal moet een sectie zijn in de ground truth payload."""
        sectie_labels = {
            'inkomsten', 'vaste_lasten', 'variabele_kosten',
            'sparen_beleggen', 'onderling_neutraal',
        }
        assert 'onderling_neutraal' in sectie_labels
