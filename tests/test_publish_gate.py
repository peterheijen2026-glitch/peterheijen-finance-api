"""Test publish gate (no-send gate) — rapport mag niet verzonden worden bij RED."""
import pytest
from app import _no_send_gate


class TestPublishGate:
    def _make_ground_truth(self, sectie_totalen=None, maand_sectie=None, categorie_totalen=None):
        default_sectie = sectie_totalen or {
            'inkomsten': 50000, 'vaste_lasten': -20000,
            'variabele_kosten': -15000, 'sparen_beleggen': -5000,
            'onderling_neutraal': 0,
        }
        # Default: consistente maand-data die optelt tot jaar-totalen
        if maand_sectie is None and sectie_totalen is None:
            maand_sectie = {}
            for m in range(1, 13):
                maand_sectie[f'2025-{m:02d}'] = {
                    s: round(v / 12, 2) for s, v in default_sectie.items()
                }
        return {
            'versie': 'V3',
            'periode': {'n_mnd': 12, 'volle_maanden': [f'2025-{m:02d}' for m in range(1, 13)]},
            'saldo': {'totaal_begin': 10000, 'totaal_eind': 12000},
            'sectie_totalen_12m': default_sectie,
            'sectie_gemiddelden_pm': {'inkomsten': 4166, 'vaste_lasten': -1666},
            'categorie_totalen_12m': categorie_totalen or {
                'inkomsten': {'Netto salaris': 45000, 'Kinderbijslag/Kindregelingen': 5000},
                'vaste_lasten': {'Hypotheek/Huur': -15000, 'Verzekeringen': -5000},
            },
            'maand_sectie_totalen': maand_sectie or {},
            'income_sources': {},
            'vertrouwen_per_sectie': {},
        }

    def test_green_pass(self):
        """Goed rapport → SEND."""
        gt = self._make_ground_truth()
        recon = {'status': 'GREEN', 'checks': []}
        result = _no_send_gate(gt, recon, {}, {'pct_rule_based': 70})
        assert result['besluit'] == 'SEND'
        assert result['kleur'] == 'GREEN'

    def test_red_reconciliation_blocks(self):
        """RED reconciliatie → BLOCK."""
        gt = self._make_ground_truth()
        recon = {'status': 'RED', 'checks': [
            {'type': 'SALDO_MISMATCH', 'status': 'RED', 'detail': 'Saldo klopt niet'}
        ]}
        result = _no_send_gate(gt, recon, {}, {'pct_rule_based': 70})
        assert result['besluit'] == 'BLOCK'
        assert result['kleur'] == 'RED'

    def test_too_much_overig_blocks(self):
        """Te veel in Overig-categorieën → BLOCK."""
        gt = self._make_ground_truth(
            categorie_totalen={
                'inkomsten': {'Overig inkomen': 40000, 'Netto salaris': 10000},  # 80% overig!
            }
        )
        recon = {'status': 'GREEN', 'checks': []}
        result = _no_send_gate(gt, recon, {}, {'pct_rule_based': 70})
        assert result['kleur'] in ('ORANGE', 'RED')

    def test_low_rule_based_orange(self):
        """<40% rule-based → ORANGE/REVIEW."""
        gt = self._make_ground_truth()
        recon = {'status': 'GREEN', 'checks': []}
        result = _no_send_gate(gt, recon, {}, {'pct_rule_based': 20})
        assert result['kleur'] in ('ORANGE', 'RED')
        assert result['besluit'] in ('REVIEW', 'BLOCK')

    def test_year_month_mismatch_red(self):
        """Jaartotaal ≠ som maanden → RED."""
        maand_sectie = {
            '2025-01': {'inkomsten': 4000, 'vaste_lasten': -1600},
            '2025-02': {'inkomsten': 4000, 'vaste_lasten': -1600},
        }
        gt = self._make_ground_truth(
            sectie_totalen={'inkomsten': 50000, 'vaste_lasten': -20000},  # way off from 8000/-3200
            maand_sectie=maand_sectie,
        )
        recon = {'status': 'GREEN', 'checks': []}
        result = _no_send_gate(gt, recon, {}, {'pct_rule_based': 70})
        assert result['kleur'] == 'RED'
        assert any('JAARTOTAAL' in r for r in result['redenen'])
