"""Hormuz-era retailer model: reported 1H26 anchors + 2H26 scenarios. Run: python hormuz_model.py"""
import sqlite3

# Reported 1H26 (A$m): Ampol/Viva results releases, 24-25 Aug 2026. Viva D&A = EBITDA-EBIT.
# ponytail: interest/tax are DERIVED at an assumed 30% tax rate, not reported.
CO = {
 "Ampol": dict(rev=20400, ebitda=1637, ebit=1392, npat_rc=857, npat_stat=1363, netdebt=3520, vol_ml=12300),
 "Viva":  dict(rev=16487.5, ebitda=774.4, ebit=616.1, npat_rc=371.1, npat_stat=452.0, netdebt=1720, vol_ml=8490, assets=13520),
}
TAX = 0.30
for n, c in CO.items():
    c["da"] = c["ebitda"] - c["ebit"]
    c["pbt"] = c["npat_rc"] / (1 - TAX)
    c["int"] = c["ebit"] - c["pbt"]
    c["tax"] = c["pbt"] - c["npat_rc"]
    c["margin"] = c["ebit"] / c["rev"]

# 2H26 scenarios: refiner margin (the Hormuz windfall) fades; retail/C&I roughly flat.
# Windfall share of 1H26 EBIT: Ampol ~ (28.26-7.44)/28.26 of refining; Viva E&I EBITDA 353.7 vs 18.4.
SCEN = {"Hormuz persists": 1.0, "Partial reopening": 0.6, "Reopened (margins normalise)": 0.2}
base_ex = {"Ampol": 1392 * 0.6, "Viva": 616.1 - (353.7 - 18.4)}  # EBIT ex windfall (Ampol 60% is an assumption)
wind = {"Ampol": 1392 - base_ex["Ampol"], "Viva": 353.7 - 18.4}
print(f"{'':8}{'1H26 actual':>14}")
for n, c in CO.items():
    print(f"{n}: rev {c['rev']:,.0f} EBITDA {c['ebitda']:,.0f} D&A {c['da']:,.0f} EBIT {c['ebit']:,.0f} "
          f"int~{c['int']:,.0f} tax~{c['tax']:,.0f} NPAT(RC) {c['npat_rc']:,.0f}  EBIT/rev {c['margin']:.1%}")
for s, k in SCEN.items():
    print(s)
    for n, c in CO.items():
        ebit = base_ex[n] + wind[n] * k
        npat = (ebit - c["int"]) * (1 - TAX)
        print(f"  {n}: 2H26 EBIT {ebit:,.0f}  NPAT {npat:,.0f}  FY26 NPAT {c['npat_rc'] + npat:,.0f}")

# retail-over-wholesale spread from our own data (QLD median vs AIP TGP, c/L)
db = sqlite3.connect("fuel_data.db")
print("date  retail_median  AIP_TGP  spread")
for d, r, t in db.execute("""select s.date, s.median_price, o.value from daily_stats s join context_observations o
    on o.source='aip' and o.observed_date=s.date where s.state='QLD' order by 1"""):
    print(d, r, t, round(r - t, 1))
