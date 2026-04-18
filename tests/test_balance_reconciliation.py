"""Test balance reconciliation — saldo-checks per rekening per maand."""
import pandas as pd
import pytest
from app import _post_classificatie_reconciliatie


class TestBalanceReconciliation:
    def _make_df(self, saldo_begin, mutaties, saldo_eind, rek='NL01INGB001'):
        """Bouw minimale df met saldo en mutaties."""
        rows = []
        running = saldo_begin
        for i, bedrag in enumerate(mutaties):
            rows.append({
                'datum': f'2025-06-{i+1:02d}',
                'Rekeningnummer': rek,
                'bedrag': bedrag,
                'Beginsaldo': running,
                'Eindsaldo': running + bedrag,
                'maand': '2025-06',
                'Omschrijving': f'TX {i}',
                'is_intern': False,
                'regel_sectie': 'variabele_kosten',
                'regel_categorie': 'Overig variabel',
                'regel_confidence': 0.8,
                'classificatie_bron': 'rule',
            })
            running += bedrag
        df = pd.DataFrame(rows)
        df['bedrag'] = df['bedrag'].astype(float)
        # Forceer correct eindsaldo
        df.iloc[-1, df.columns.get_loc('Eindsaldo')] = saldo_eind
        return df

    def _make_feiten(self, rek='NL01INGB001', saldo_klopt=True):
        feiten = {rek: {'periode': {'van': '2025-06-01', 'tot': '2025-06-30'}}}
        if not saldo_klopt:
            feiten[rek]['saldo'] = {
                'klopt': False,
                'berekend_eind': 1200,
                'eindsaldo': 9999,
            }
        return feiten

    def test_balanced_account_green(self):
        """Als begin + mutaties = eind → GREEN."""
        df = self._make_df(1000, [-200, -100, 500], 1200)
        recon = _post_classificatie_reconciliatie(df, self._make_feiten())
        saldo_checks = [c for c in recon['checks'] if c['type'] == 'SALDO_MISMATCH']
        assert len(saldo_checks) == 0 or all(c['status'] != 'RED' for c in saldo_checks)

    def test_unbalanced_account_red(self):
        """Als begin + mutaties ≠ eind → RED."""
        df = self._make_df(1000, [-200, -100, 500], 9999)  # wrong end balance
        recon = _post_classificatie_reconciliatie(df, self._make_feiten(saldo_klopt=False))
        saldo_checks = [c for c in recon['checks'] if c['type'] == 'SALDO_MISMATCH']
        assert any(c['status'] == 'RED' for c in saldo_checks)
