"""Compare refuelling strategies over the same driving and ending fuel balance.

An extra detour is charged to the deferred strategy, including replacement fuel.
No unvalidated price scenario is promoted as a forecast-based recommendation.
"""
from datetime import datetime, timedelta
from math import ceil
from pydantic import BaseModel, Field, model_validator


class BuyingProfile(BaseModel):
    tank_l: float = Field(default=50, ge=5, le=200, allow_inf_nan=False)
    remaining_l: float = Field(default=10, ge=0, le=200, allow_inf_nan=False)
    reserve_l: float = Field(default=5, ge=0, le=50, allow_inf_nan=False)
    daily_km: float = Field(default=35, ge=0, le=1000, allow_inf_nan=False)
    consumption: float = Field(default=8, gt=0, le=40, allow_inf_nan=False)
    wait_days: int = Field(default=3, ge=1, le=7)
    station_id: str | None = Field(default=None, max_length=80)
    discount_cpl: float = Field(default=0, ge=0, le=50, allow_inf_nan=False)
    extra_km: float = Field(default=0, ge=0, le=300, allow_inf_nan=False)
    extra_minutes: float = Field(default=0, ge=0, le=300, allow_inf_nan=False)
    hourly_value: float = Field(default=0, ge=0, le=200, allow_inf_nan=False)
    budget_now: float | None = Field(default=None, gt=0, le=1000, allow_inf_nan=False)
    scenario_change_cpl: float | None = Field(default=None, ge=-100, le=100, allow_inf_nan=False)

    @model_validator(mode='after')
    def tank_limits(self):
        if self.remaining_l > self.tank_l:
            raise ValueError('Fuel remaining cannot exceed tank capacity.')
        if self.reserve_l >= self.tank_l:
            raise ValueError('Reserve must be smaller than tank capacity.')
        return self


def compare_plan(profile, dashboard, today=None):
    today = today or datetime.now().date()
    stations = dashboard['stations']
    station = next((s for s in stations if str(s['site_id']) == profile.station_id), None)
    if profile.station_id and station is None:
        raise ValueError('That station is no longer in the current snapshot. Select another station.')
    median = dashboard['summary']['median']
    raw_price = station['price_cpl'] if station else median
    if raw_price is None:
        raise ValueError('No saved prices are available to calculate a plan.')
    price = raw_price - profile.discount_cpl
    forecast = dashboard['forecast']
    target_date = (today + timedelta(days=profile.wait_days)).isoformat()
    point = next((p for p in forecast['points'] if p['date'] == target_date), None)
    scenario = profile.scenario_change_cpl is not None
    # Market change is applied to today's chosen station; this is an assumption,
    # not a station-specific price forecast. Same discount is assumed both days.
    change = profile.scenario_change_cpl if scenario else (point['price'] - median if point and median is not None else 0)
    later_price = max(1, price + change)
    daily_l = profile.daily_km * profile.consumption / 100
    driving_l = daily_l * profile.wait_days
    extra_l = profile.extra_km * profile.consumption / 100
    time_cost = profile.extra_minutes * profile.hourly_value / 60
    fill_l = profile.tank_l - profile.remaining_l
    fill_cost = fill_l * price / 100
    end_l = profile.tank_l - driving_l
    usable_l = max(0, profile.remaining_l - profile.reserve_l)
    bridge_l = max(0, driving_l + extra_l + profile.reserve_l - profile.remaining_l)
    # Round purchases upward to 0.1 L so the displayed plan respects the reserve.
    bridge_l = ceil(round(bridge_l, 8) * 10) / 10
    feasible = end_l >= profile.reserve_l and bridge_l <= fill_l
    later_l = max(0, fill_l - bridge_l + extra_l)
    now_cost = bridge_l * price / 100
    later_cost = later_l * later_price / 100
    total_cost = now_cost + later_cost + time_cost
    saving = fill_cost - total_cost
    break_even = (fill_cost - now_cost - time_cost) / later_l * 100 if later_l > 0 else None
    selected = next((m for m in forecast.get('scores', []) if m.get('selected')), {})
    baseline = next((m for m in forecast.get('scores', []) if m.get('id') == 'persistence'), {})
    validated = (forecast.get('status') == 'ready' and forecast.get('recent_consecutive_days', 0) >= 7
                 and point is not None and selected.get('mae') is not None
                 and baseline.get('mae') is not None and selected['mae'] <= baseline['mae'])
    fresh = dashboard['health']['fresh']
    budget = profile.budget_now
    bridge_budget = budget is None or now_cost <= budget
    fill_budget = budget is None or fill_cost <= budget
    high_price = max(1, price + point['high'] - median) if point and point.get('high') is not None else None
    downside_saving = fill_cost - (now_cost + later_l * high_price / 100 + time_cost) if high_price is not None else None
    if not feasible:
        headline = 'Choose an earlier refill date'
        explanation = 'This two-stop comparison cannot cover that distance while keeping your reserve. Shorten the wait or plan an intermediate stop.'
    elif not fresh:
        headline = 'Refresh prices before deciding'
        explanation = 'These costs use stale observations. The quantities are still useful, but the prices need a fresh check.'
    elif not bridge_budget and not fill_budget:
        headline = 'Your current budget does not cover either plan'
        explanation = 'Shorten the wait or adjust the budget. Neither option shown keeps the chosen reserve within your spend limit.'
    elif not bridge_budget:
        headline = 'The bridge purchase exceeds today’s budget'
        explanation = 'Shorten the wait or change the budget before considering this deferred purchase.'
    elif scenario:
        headline = 'Waiting wins in this scenario' if saving > 0 and bridge_budget else 'Filling now costs less in this scenario' if fill_budget else 'Budget limits the options'
        explanation = 'This is your what-if price change, not a prediction. Compare the break-even price and a price rise before acting.'
    elif not validated:
        headline = 'No reliable price signal to justify waiting'
        explanation = 'There is not enough validated recent history. Use the map for a lower price today; the bridge quantity is a contingency, not a buy-later recommendation.'
    elif saving > 0 and downside_saving is not None and downside_saving > 0 and bridge_budget:
        headline = 'A small top-up could be worth considering'
        explanation = 'The deferred plan is cheaper even at the upper historical error estimate. That range is not a guarantee; your chosen station can move differently.'
    else:
        headline = 'The expected saving is not robust'
        explanation = 'Waiting does not clearly pay after travel costs and historical price uncertainty. Recheck prices before committing to another stop.'
    warnings = ['Both options cover the same everyday driving and finish with the same fuel in the tank.',
                'Extra kilometres and minutes mean additional travel compared with filling now, not the whole trip.',
                'Station prices may move differently from the city median; discounts are assumed unchanged.']
    if profile.remaining_l < profile.reserve_l:
        warnings.append('Your entered fuel is below your reserve. Arrange a refill now rather than relying on the waiting window.')
    if later_l == 0:
        warnings.append('There is no fuel purchase left to defer, so waiting cannot create a price saving.')
    return {
        'headline': headline, 'explanation': explanation, 'feasible': feasible, 'fresh': fresh,
        'basis': 'Your what-if scenario' if scenario else 'Validated market-change estimate' if validated else 'Unvalidated baseline',
        'station': station['name'] if station else 'Brisbane market median (choose a station)',
        'target_date': target_date, 'price_now': round(price, 1), 'price_later': round(later_price, 1),
        'daily_l': round(daily_l, 2), 'range_km': round(usable_l / profile.consumption * 100),
        'days_to_reserve': round(usable_l / daily_l, 1) if daily_l else None,
        'fill_l': round(fill_l, 2), 'fill_cost': round(fill_cost, 2), 'fill_within_budget': fill_budget,
        'bridge_l': round(bridge_l, 2) if feasible else None, 'bridge_cost': round(now_cost, 2) if feasible else None,
        'later_l': round(later_l, 2) if feasible else None, 'later_cost': round(later_cost, 2) if feasible else None,
        'time_cost': round(time_cost, 2), 'extra_fuel_l': round(extra_l, 2),
        'deferred_cost': round(total_cost, 2) if feasible else None, 'bridge_within_budget': bridge_budget if feasible else False,
        'saving': round(saving, 2) if feasible else None, 'end_l': round(end_l, 2) if feasible else None,
        'break_even_cpl': round(break_even, 2) if feasible and break_even is not None else None,
        'scenarios': [{'change': shift, 'price': round(max(1, price + shift), 1),
                       'saving': round(fill_cost - (now_cost + later_l * max(1, price + shift)/100 + time_cost), 2)}
                      for shift in [-10, 0, 10]] if feasible else [],
        'warnings': warnings,
    }


def compare_horizons(profile, dashboard, today=None):
    """Compare all feasible dates; never spread a user's single-date what-if across a week."""
    result = compare_plan(profile, dashboard, today)
    result['timing'] = []
    result['best_wait_days'] = None
    if profile.scenario_change_cpl is not None:
        return result
    robust_choices = []
    for days in range(1, 8):
        candidate = compare_plan(profile.model_copy(update={'wait_days': days}), dashboard, today)
        result['timing'].append({
            'days': days, 'date': candidate['target_date'], 'selected': days == profile.wait_days,
            'feasible': candidate['feasible'], 'bridge_l': candidate['bridge_l'],
            'saving': candidate['saving'], 'budget_ok': candidate['bridge_within_budget'],
        })
        if candidate['headline'] == 'A small top-up could be worth considering':
            robust_choices.append((candidate['saving'], days))
    if robust_choices:
        # Prefer the earlier day in a tie.
        result['best_wait_days'] = max(robust_choices, key=lambda item: (item[0], -item[1]))[1]
    return result
