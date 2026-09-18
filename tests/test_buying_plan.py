import unittest
from datetime import date
from copy import deepcopy

from pydantic import ValidationError
from buying_plan import BuyingProfile, compare_plan, compare_horizons


def market():
    return {
        'stations': [{'site_id': 'one', 'name': 'Local test station', 'price_cpl': 200}],
        'summary': {'median': 210}, 'health': {'fresh': True},
        'forecast': {'status': 'limited', 'recent_consecutive_days': 1, 'scores': [],
                     'points': [{'date': '2026-09-19', 'price': 190, 'low': None, 'high': None}]},
    }


class BuyingPlanTests(unittest.TestCase):
    def test_horizon_search_never_extrapolates_user_scenarios(self):
        result = compare_horizons(BuyingProfile(), market(), date(2026, 9, 16))
        self.assertEqual(len(result['timing']), 7)
        self.assertIsNone(result['best_wait_days'])
        result = compare_horizons(BuyingProfile(scenario_change_cpl=-20), market(), date(2026, 9, 16))
        self.assertEqual(result['timing'], [])

    def plan(self, **values):
        return compare_plan(BuyingProfile(station_id='one', **values), market(), date(2026, 9, 16))

    def test_postponed_litres_times_price_drop(self):
        result = self.plan(daily_km=50, consumption=10, remaining_l=10, scenario_change_cpl=-20)
        self.assertEqual(result['bridge_l'], 10)
        self.assertEqual(result['later_l'], 30)
        self.assertEqual(result['fill_cost'], 80)
        self.assertEqual(result['deferred_cost'], 74)
        self.assertEqual(result['saving'], 6)
        self.assertEqual(result['break_even_cpl'], 200)

    def test_extra_travel_preserves_equal_final_fuel(self):
        result = self.plan(daily_km=50, consumption=10, extra_km=20, scenario_change_cpl=-20)
        final = 10 + result['bridge_l'] + result['later_l'] - 15 - result['extra_fuel_l']
        self.assertAlmostEqual(final, result['end_l'])
        self.assertEqual(result['extra_fuel_l'], 2)
        self.assertLess(result['saving'], 6)
        self.assertLess(result['break_even_cpl'], 200)

    def test_capacity_changes_savings_not_minimum_bridge(self):
        small = self.plan(tank_l=40, scenario_change_cpl=-20)
        large = self.plan(tank_l=60, scenario_change_cpl=-20)
        self.assertEqual(small['bridge_l'], large['bridge_l'])
        self.assertAlmostEqual(large['saving'] - small['saving'], 4)

    def test_no_top_up_when_existing_fuel_covers_reserve(self):
        result = self.plan(remaining_l=30, scenario_change_cpl=-20)
        self.assertEqual(result['bridge_l'], 0)
        self.assertEqual(result['later_l'], 20)

    def test_unvalidated_forecast_never_recommends_waiting(self):
        result = self.plan()
        self.assertGreater(result['saving'], 0)
        self.assertEqual(result['headline'], 'No reliable price signal to justify waiting')
        self.assertEqual(result['basis'], 'Unvalidated baseline')

    def test_budget_and_impossible_range(self):
        result = self.plan(budget_now=1)
        self.assertFalse(result['bridge_within_budget'])
        self.assertIn('does not cover either plan', result['headline'])
        result = self.plan(daily_km=500)
        self.assertFalse(result['feasible'])
        self.assertIsNone(result['bridge_l'])
        self.assertIsNone(result['saving'])

    def test_time_can_eliminate_a_price_saving(self):
        result = self.plan(scenario_change_cpl=-20, extra_minutes=30, hourly_value=30)
        self.assertLess(result['saving'], 0)
        self.assertEqual(result['time_cost'], 15)

    def test_stale_prices_override_optimistic_scenario(self):
        data = market()
        data['health']['fresh'] = False
        result = compare_plan(BuyingProfile(scenario_change_cpl=-30), data, date(2026, 9, 16))
        self.assertEqual(result['headline'], 'Refresh prices before deciding')

    def test_rejects_impossible_profile_and_disappeared_station(self):
        for values in [{'remaining_l': 60}, {'reserve_l': 50}, {'consumption': 0}, {'daily_km': float('nan')}]:
            with self.assertRaises(ValidationError):
                BuyingProfile(**values)
        with self.assertRaises(ValueError):
            compare_plan(BuyingProfile(station_id='unknown'), market())

    def test_forecast_only_helps_when_worse_price_still_saves(self):
        data = deepcopy(market())
        data['forecast'].update(status='ready', recent_consecutive_days=30,
            scores=[{'id': 'trend', 'selected': True, 'mae': 2}, {'id': 'persistence', 'mae': 5}])
        data['forecast']['points'][0]['high'] = 195
        result = compare_plan(BuyingProfile(station_id='one'), data, date(2026, 9, 16))
        self.assertEqual(result['headline'], 'A small top-up could be worth considering')
        data['forecast']['points'][0]['high'] = 220
        result = compare_plan(BuyingProfile(station_id='one'), data, date(2026, 9, 16))
        self.assertEqual(result['headline'], 'The expected saving is not robust')


if __name__ == '__main__':
    unittest.main()
