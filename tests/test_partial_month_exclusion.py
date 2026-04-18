"""Test partial month exclusion — onvolledige maanden niet meetellen in 12m totalen."""
import pytest
from app import _bepaal_rapportperiode
import pandas as pd


class TestPartialMonthExclusion:
    def _make_df(self, months_tx_counts):
        """Bouw df met N transacties per maand."""
        rows = []
        for maand, n_tx in months_tx_counts.items():
            for i in range(n_tx):
                rows.append({
                    'datum': f'{maand}-{(i % 28) + 1:02d}',
                    'Rekeningnummer': 'NL01INGB001',
                    'bedrag': -10.0,
                    'maand': maand,
                })
        return pd.DataFrame(rows)

    def _make_recon(self, incomplete_months=None):
        """Maak reconciliatie dict met onvolledige maanden."""
        checks = []
        for m in (incomplete_months or []):
            checks.append({'type': 'ONVOLLEDIGE_MAAND', 'maand': m, 'status': 'ORANGE',
                           'detail': f'{m}: <15 transacties'})
        return {
            'status': 'GREEN',
            'checks': checks,
            'onvolledige_maanden': set(incomplete_months or []),
        }

    def test_partial_month_excluded(self):
        months = {'2025-01': 30, '2025-02': 25, '2025-03': 28, '2025-04': 5}
        df = self._make_df(months)
        recon = self._make_recon(['2025-04'])
        result = _bepaal_rapportperiode(df, recon)
        assert '2025-04' in result.get('onvolledige_maanden', [])
        assert '2025-04' not in result.get('volle_maanden', [])

    def test_full_months_included(self):
        months = {'2025-01': 30, '2025-02': 25, '2025-03': 28}
        df = self._make_df(months)
        recon = self._make_recon()
        result = _bepaal_rapportperiode(df, recon)
        volle = result.get('volle_maanden', [])
        assert '2025-01' in volle
        assert '2025-02' in volle
        assert '2025-03' in volle

    def test_n_mnd_excludes_partial(self):
        months = {'2025-01': 30, '2025-02': 25, '2025-03': 28, '2025-04': 3}
        df = self._make_df(months)
        recon = self._make_recon(['2025-04'])
        result = _bepaal_rapportperiode(df, recon)
        assert result['n_mnd'] == 3
