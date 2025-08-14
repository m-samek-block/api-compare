# Raport historyczny (wiele uruchomień)

## Rankingi RMSE (mediana po runach)

### precipitation
| provider | runs | RMSE_med | bias_med | cover_med% | trend_rmse/dzień |
|---|---:|---:|---:|---:|---:|
| openweather | 26 | 0.095 | -0.031 | 100.0 | nan |
| openmeteo | 49 | 0.108 | -0.030 | 66.7 | nan |
| weatherapi | 28 | 0.108 | -0.030 | 100.0 | nan |
| metno | 28 | 0.109 | -0.030 | 100.0 | nan |
| visualcrossing | 70 | 0.167 | -0.030 | 100.0 | nan |

![RMSE vs czas — precipitation](rmse_time_precipitation.png)


![Bias vs czas — precipitation](bias_time_precipitation.png)

### temperature_2m
| provider | runs | RMSE_med | bias_med | cover_med% | trend_rmse/dzień |
|---|---:|---:|---:|---:|---:|
| metno | 49 | 7.749 | 3.917 | 100.0 | nan |
| visualcrossing | 70 | 7.966 | 3.862 | 100.0 | nan |
| openweather | 28 | 8.033 | 5.908 | 100.0 | nan |
| openmeteo | 49 | 8.122 | 3.526 | 66.7 | nan |
| weatherapi | 28 | 8.563 | 4.502 | 100.0 | nan |

![RMSE vs czas — temperature_2m](rmse_time_temperature_2m.png)


![Bias vs czas — temperature_2m](bias_time_temperature_2m.png)

### wind_direction_100m
| provider | runs | RMSE_med | bias_med | cover_med% | trend_rmse/dzień |
|---|---:|---:|---:|---:|---:|
| weatherapi | 28 | 85.541 | -15.313 | 100.0 | nan |
| openweather | 28 | 88.423 | -14.478 | 100.0 | nan |
| openmeteo | 49 | 95.874 | -20.073 | 66.7 | nan |
| visualcrossing | 70 | 100.289 | -14.173 | 100.0 | nan |
| metno | 49 | 102.311 | -10.104 | 100.0 | nan |

![RMSE vs czas — wind_direction_100m](rmse_time_wind_direction_100m.png)


![Bias vs czas — wind_direction_100m](bias_time_wind_direction_100m.png)

### wind_speed_100m
| provider | runs | RMSE_med | bias_med | cover_med% | trend_rmse/dzień |
|---|---:|---:|---:|---:|---:|
| openweather | 28 | 2.773 | -0.381 | 100.0 | nan |
| visualcrossing | 70 | 2.808 | -0.227 | 100.0 | nan |
| metno | 49 | 2.880 | -1.306 | 100.0 | nan |
| weatherapi | 28 | 2.978 | -0.638 | 100.0 | nan |
| openmeteo | 49 | 13.035 | 10.634 | 66.7 | nan |

![RMSE vs czas — wind_speed_100m](rmse_time_wind_speed_100m.png)


![Bias vs czas — wind_speed_100m](bias_time_wind_speed_100m.png)


## Ranking ogólny (mediana RMSE_med po zmiennych)

1. **metno** — 5.314
2. **visualcrossing** — 5.387
3. **openweather** — 5.403
4. **weatherapi** — 5.771
5. **openmeteo** — 10.578

## Wzorce biasu (znak i wielkość)

### precipitation
| provider | bias_med | bias_mean | trend_bias | slope_bias/dzień |
|---|---:|---:|---|---:|
| openweather | -0.031 | 0.033 | zaniża | nan |
| metno | -0.030 | 0.011 | zaniża | nan |
| openmeteo | -0.030 | 0.031 | zaniża | nan |
| visualcrossing | -0.030 | 0.064 | zaniża | nan |
| weatherapi | -0.030 | 0.029 | zaniża | nan |
### temperature_2m
| provider | bias_med | bias_mean | trend_bias | slope_bias/dzień |
|---|---:|---:|---|---:|
| openmeteo | 3.526 | 3.536 | zawyża | nan |
| visualcrossing | 3.862 | 3.935 | zawyża | nan |
| metno | 3.917 | 3.381 | zawyża | nan |
| weatherapi | 4.502 | 3.646 | zawyża | nan |
| openweather | 5.908 | 4.410 | zawyża | nan |
### wind_direction_100m
| provider | bias_med | bias_mean | trend_bias | slope_bias/dzień |
|---|---:|---:|---|---:|
| openmeteo | -20.073 | -8.774 | zaniża | nan |
| weatherapi | -15.313 | -15.848 | zaniża | nan |
| openweather | -14.478 | -21.373 | zaniża | nan |
| visualcrossing | -14.173 | -13.868 | zaniża | nan |
| metno | -10.104 | -7.639 | zaniża | nan |
### wind_speed_100m
| provider | bias_med | bias_mean | trend_bias | slope_bias/dzień |
|---|---:|---:|---|---:|
| metno | -1.306 | -0.862 | zaniża | nan |
| weatherapi | -0.638 | -0.294 | zaniża | nan |
| openweather | -0.381 | -0.039 | zaniża | nan |
| visualcrossing | -0.227 | 0.045 | zaniża | nan |
| openmeteo | 10.634 | 13.339 | zawyża | nan |