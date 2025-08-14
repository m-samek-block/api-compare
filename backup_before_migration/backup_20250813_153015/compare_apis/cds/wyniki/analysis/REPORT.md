# Raport porównania API vs ERA5

## Ranking (mediana RMSE po zmiennych)

1. **visualcrossing** — RMSE_med=1.403
2. **weatherapi** — RMSE_med=1.758
3. **openmeteo** — RMSE_med=7.025

## Wykrywanie opadów (CSI)

1. **weatherapi** — CSI=0.05, POD=1.00, FAR=0.95
2. **openmeteo** — CSI=0.00, POD=nan, FAR=1.00
3. **visualcrossing** — CSI=0.00, POD=nan, FAR=1.00

- [openmeteo](provider_openmeteo.md)
- [visualcrossing](provider_visualcrossing.md)
- [weatherapi](provider_weatherapi.md)