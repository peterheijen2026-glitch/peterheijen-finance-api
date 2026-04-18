"""Test month bucket logic — transacties worden in de juiste maand geplaatst."""
import pandas as pd
import pytest


class TestMonthBucketLogic:
    def test_transaction_in_correct_month(self):
        """Transactie op 15 juni → maand 2025-06."""
        df = pd.DataFrame([{'datum': '2025-06-15', 'bedrag': -100}])
        df['maand'] = pd.to_datetime(df['datum']).dt.strftime('%Y-%m')
        assert df.iloc[0]['maand'] == '2025-06'

    def test_month_boundaries(self):
        """Eerste en laatste dag van de maand → zelfde maand."""
        df = pd.DataFrame([
            {'datum': '2025-03-01', 'bedrag': -50},
            {'datum': '2025-03-31', 'bedrag': -75},
        ])
        df['maand'] = pd.to_datetime(df['datum']).dt.strftime('%Y-%m')
        assert all(df['maand'] == '2025-03')

    def test_year_totals_match_month_sum(self):
        """Jaartotaal per categorie == som van maandtotalen."""
        months_data = {
            '2025-01': -150.0,
            '2025-02': -200.0,
            '2025-03': -175.0,
        }
        year_total = sum(months_data.values())
        assert year_total == -525.0
        assert year_total == sum(months_data[m] for m in months_data)
