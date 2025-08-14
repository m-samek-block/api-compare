# Weather Analysis System 🌤️

Zaawansowany system analizy i porównywania API pogodowych z prawdziwymi danymi ERA5 z Copernicus CDS.

## 🎯 Główne funkcje

- **Prawdziwe dane referencyjne** - ERA5 z Copernicus CDS zamiast syntetycznych
- **Centralne wyniki** - jeden plik `central_weather_results.csv` 
- **Inteligentny cache** - eliminuje duplikowanie danych ERA5 i providerów
- **Batch processing** - analiza wielu lokalizacji równolegle
- **Dashboard** - automatyczne generowanie wykresów i raportów
- **Automatyczne czyszczenie** - zarządzanie starymi danymi cache

## 📁 Architektura systemu

```
weather-analysis/
├── 📊 DANE I WYNIKI
│   ├── central_weather_results.csv         # 🎯 Centralne wyniki wszystkich analiz
│   ├── cache/
│   │   ├── era5/                          # 💾 Cache prawdziwych danych ERA5
│   │   └── providers/                     # 💾 Cache danych API pogodowych
│   ├── dashboard/                         # 📈 Automatyczne dashboardy
│   └── batch_results/                     # 🔄 Wyniki analiz batch
│
├── 🛠️ SKRYPTY GŁÓWNE
│   ├── run_weather_analysis.py            # 🚀 GŁÓWNY LAUNCHER
│   ├── consolidated_analysis.py           # 🧠 SILNIK ANALIZ
│   ├── batch_analyzer.py                  # 🔄 BATCH PROCESSING
│   └── analysis_viewer.py                 # 🔍 PRZEGLĄDARKA WYNIKÓW
│
├── 🌐 POBIERANIE DANYCH
│   ├── fetch_forecasts.py                 # 📡 API pogodowe (real data)
│   └── make_era5_cds.py                   # 🌍 Prawdziwe ERA5 z CDS
│
└── 🎲 TESTY I NARZĘDZIA
    ├── quick_random_test.ps1              # ⚡ Szybkie testy
    ├── random_weather_analysis.ps1        # 🎲 Losowe lokalizacje
    └── run_fetch_compare_cds.py           # 🔬 Legacy wrapper
```

## 📋 Szczegółowy opis plików

### 🚀 Główne skrypty użytkownika

#### `run_weather_analysis.py` - GŁÓWNY LAUNCHER
**Najbardziej przyjazny interfejs do systemu**

```bash
# 📍 Predefiniowane lokalizacje
python run_weather_analysis.py --location warszawa --date-preset yesterday --use-cds
python run_weather_analysis.py --location gdansk --date-preset forecast-3days

# 🗺️ Własne współrzędne  
python run_weather_analysis.py --coords 52.5 21.0 --date-preset last-week --use-cds

# 📅 Własne daty
python run_weather_analysis.py --location krakow \
    --custom-dates 2025-08-01T00:00:00Z 2025-08-08T00:00:00Z --use-cds
```

**Funkcje:**
- Automatyczne rozpoznawanie czy dane są historyczne czy prognozy
- Dla danych historycznych: pobiera ERA5 z CDS jako reference
- Dla prognoz: porównuje API między sobą
- Predefiniowane lokalizacje polskich miast
- Automatyczne zarządzanie kluczami API

**Predefiniowane lokalizacje:**
- warszawa, krakow, gdansk, wroclaw, poznan, szczecin, bydgoszcz, lublin, zakopane, hel

**Predefiniowane okresy:**
- **Historyczne:** yesterday, last-3days, last-week, last-month
- **Aktualne/prognozy:** today, forecast-3days, forecast-week, current-week

---

#### `batch_analyzer.py` - BATCH PROCESSING
**Analiza wielu lokalizacji równolegle**

```bash
# 🇵🇱 Główne miasta Polski, ostatni tydzień
python batch_analyzer.py --location-set poland_major --time-period last_week

# 🇪🇺 Stolice Europy, wczoraj, z CDS
python batch_analyzer.py --location-set europe_capitals --time-period yesterday --use-cds

# 🌍 Miasta świata, własny zakres dat, 5 workerów
python batch_analyzer.py --location-set world_major \
    --custom-time 2025-08-01T00:00:00Z 2025-08-08T00:00:00Z --max-workers 5
```

**Predefiniowane zestawy lokalizacji:**
- `poland_major` - 10 głównych miast Polski
- `europe_capitals` - 10 stolic europejskich  
- `world_major` - 10 miast na różnych kontynentach
- `coastal_vs_inland` - porównanie miast nadmorskich vs śródlądowych

**Generuje:**
- `batch_results/batch_report_*.md` - raport tekstowy
- `batch_results/batch_results_*.json` - szczegóły JSON
- `batch_results/comparative_analysis_*.md` - analiza porównawcza

---

#### `analysis_viewer.py` - PRZEGLĄDARKA WYNIKÓW
**Przeglądanie i analiza zebranych danych**

```bash
# 📊 Ogólny przegląd
python analysis_viewer.py overview

# 🆚 Porównanie providerów
python analysis_viewer.py compare

# 🗺️ Analiza per lokalizacja
python analysis_viewer.py locations

# 📈 Trendy czasowe (ostatnie 30 dni)
python analysis_viewer.py trends

# 📈 Trendy za ostatnie 7 dni
python analysis_viewer.py trends --days 7

# 💾 Eksport podsumowania
python analysis_viewer.py export --export-file summary.csv
```

**Funkcje:**
- Ładuje dane z `central_weather_results.csv`
- Generuje tekstowe raporty i statystyki
- Eksport do CSV
- Ranking providerów i lokalizacji

---

### 🧠 Silnik systemu

#### `consolidated_analysis.py` - SILNIK ANALIZ
**Główny silnik analiz pogodowych (zwykle uruchamiany przez wrapper-y)**

```bash
# 📡 Bezpośrednie uruchomienie (zaawansowane)
python consolidated_analysis.py \
    --lat 52.2297 --lon 21.0122 \
    --start 2025-08-12T00:00:00Z --end 2025-08-14T00:00:00Z \
    --providers openmeteo,metno,weatherapi,visualcrossing \
    --location-name "Warszawa"
```

**Funkcje:**
- Pobiera prawdziwe dane ERA5 z CDS używając `make_era5_cds.py`
- Pobiera dane z API pogodowych używając `fetch_forecasts.py`
- Oblicza metryki: RMSE, bias, korelacja vs ERA5
- Zapisuje wyniki do `central_weather_results.csv`
- Generuje dashboard i wykresy

---

### 🌐 Pobieranie danych

#### `fetch_forecasts.py` - API POGODOWE (REAL DATA)
**Pobiera prawdziwe dane z API pogodowych**

```bash
# 🌤️ Pobierz dane z wszystkich providerów
python fetch_forecasts.py --lat 52.2297 --lon 21.0122 \
    --start 2025-08-01T00:00:00Z --end 2025-08-08T00:00:00Z \
    --providers openmeteo,metno,weatherapi,visualcrossing,openweather

# 🔑 Z kluczami API
python fetch_forecasts.py --lat 52.2297 --lon 21.0122 \
    --start 2025-08-01T00:00:00Z --end 2025-08-08T00:00:00Z \
    --weatherapi-key YOUR_KEY --visualcrossing-key YOUR_KEY
```

**Obsługiwane providery:**
- **openmeteo** - darmowy, bez klucza, najlepsze modele
- **metno** - Met.no (Norwegia), tylko prognozy
- **weatherapi** - klucz API, dane historyczne + prognozy
- **visualcrossing** - klucz API, dane historyczne + prognozy  
- **openweather** - klucz API, wymaga płatnej subskrypcji dla historycznych

**PRAWDZIWE DANE:**
- Dla historycznych: prawdziwe obserwacje i re-analizy
- Dla prognoz: prawdziwe prognozy numeryczne
- BRAK danych syntetycznych

---

#### `make_era5_cds.py` - PRAWDZIWE ERA5 Z CDS
**Pobiera prawdziwe dane ERA5 z Copernicus Climate Data Store**

```bash
# 🌍 Pobierz ERA5 dla jednego punktu
python make_era5_cds.py --lat 52.2297 --lon 21.0122 \
    --start 2025-08-01T00:00:00Z --end 2025-08-08T00:00:00Z \
    --out cache/era5/warszawa_era5.csv
```

**Wymagania:**
- Konto na Climate Data Store: https://cds.climate.copernicus.eu
- Konfiguracja `~/.cdsapirc` z kluczem API
- Instalacja: `pip install cdsapi`

**Funkcje:**
- Pobiera temperature_2m, precipitation, wind_speed_100m, wind_direction_100m
- Automatyczna konwersja jednostek
- Format wyjściowy: `time,latitude,longitude,variable,value`

---

### 🎲 Testy i narzędzia

#### `quick_random_test.ps1` - SZYBKIE TESTY (PowerShell)
**Szybkie testy systemu na losowych lokalizacjach**

```powershell
# ⚡ Szybki test - 5 lokalizacji w Europie
.\quick_random_test.ps1 -Mode quick

# 🌍 Test walidatora - 10 lokalizacji na różnych kontynentach
.\quick_random_test.ps1 -Mode validator
```

**Tryby:**
- `quick` - 5 losowych lokalizacji w Europie
- `validator` - 10 lokalizacji na różnych kontynentach

---

#### `random_weather_analysis.ps1` - LOSOWE LOKALIZACJE (PowerShell)
**Zaawansowane testy na całkowicie losowych lokalizacjach**

```powershell
# 🎲 20 losowych lokalizacji lądowych
.\random_weather_analysis.ps1 -LocationCount 20

# 🌊 50 lokalizacji włącznie z oceanami, 5 workerów
.\random_weather_analysis.ps1 -LocationCount 50 -IncludeOceans -MaxWorkers 5

# 🧹 Nowy start z czyszczeniem
.\random_weather_analysis.ps1 -LocationCount 10 -CleanStart
```

**Funkcje:**
- Generowanie losowych współrzędnych na świecie
- Unikanie oceanów (opcjonalne)
- Analiza geograficzna wyników
- Raportowanie sukcesu per region

---

#### `run_fetch_compare_cds.py` - LEGACY WRAPPER
**Stary interfejs dla kompatybilności wstecznej**

```bash
# 🔬 Legacy analiza (używa starszego podejścia)
python run_fetch_compare_cds.py
```

*Uwaga: Ten skrypt jest zachowany dla kompatybilności, ale zaleca się używanie `run_weather_analysis.py`*

---

## 🚀 Szybki start

### 1. Podstawowa konfiguracja

```bash
# Zainstaluj wymagane pakiety
pip install pandas numpy matplotlib seaborn requests cdsapi xarray

# Skonfiguruj Copernicus CDS (opcjonalne, dla danych historycznych)
# 1. Załóż konto: https://cds.climate.copernicus.eu/user/register  
# 2. Pobierz klucz API: https://cds.climate.copernicus.eu/profile
# 3. Utwórz ~/.cdsapirc:
echo "url: https://cds.climate.copernicus.eu/api
key: UID:API-KEY" > ~/.cdsapirc

# Ustaw klucze API pogodowe (opcjonalne)
export WEATHERAPI_KEY="your_key"
export VISUALCROSSING_KEY="your_key"
export OPENWEATHER_KEY="your_key"
```

### 2. Pierwsze uruchomienie

```bash
# 🌤️ Analiza historyczna z prawdziwymi ERA5
python run_weather_analysis.py --location warszawa --date-preset yesterday --use-cds

# 📈 Analiza prognoz (porównanie API)
python run_weather_analysis.py --location gdansk --date-preset forecast-3days

# 📊 Przegląd wyników
python analysis_viewer.py overview
python analysis_viewer.py compare
```

### 3. Batch analysis

```bash
# 🇵🇱 Analiza głównych miast Polski
python batch_analyzer.py --location-set poland_major --time-period last_week --use-cds

# 📊 Przegląd wyników batch
python analysis_viewer.py locations
python analysis_viewer.py trends
```

## 📊 Format danych i wyniki

### Centralny plik wyników
**`central_weather_results.csv`** - jeden plik z wszystkimi wynikami:

```csv
hash,timestamp,location_name,lat,lon,start_time,end_time,provider,analysis_type,
temperature_2m_rmse,temperature_2m_bias,temperature_2m_correlation,temperature_2m_n,
precipitation_rmse,precipitation_bias,precipitation_correlation,precipitation_n,
wind_speed_100m_rmse,wind_speed_100m_bias,wind_speed_100m_correlation,wind_speed_100m_n,
overall_score,data_points_total,notes
```

### Generowane pliki
```
├── central_weather_results.csv         # 🎯 Główne wyniki
├── dashboard/
│   ├── weather_analysis_dashboard.png  # 📊 Główny dashboard
│   └── weather_analysis_summary.txt    # 📝 Podsumowanie tekstowe
├── cache/era5/                         # 💾 Cache ERA5 (współdzielone)
├── cache/providers/                    # 💾 Cache API (współdzielone)
└── batch_results/                      # 🔄 Wyniki batch (jeśli uruchamiano)
```

## 🆚 Porównanie z poprzednimi wersjami

| Aspekt | Poprzednio | Obecnie |
|--------|------------|---------|
| **Dane referencyjne** | Syntetyczne/losowe | Prawdziwe ERA5 z CDS |
| **Pliki wyników** | Setki rozproszonych CSV | Jeden `central_weather_results.csv` |
| **Cache** | Duplikaty wszędzie | Inteligentny cache współdzielony |
| **Interface** | Skomplikowane CLI | Przyjazny `run_weather_analysis.py` |
| **Batch processing** | Brak | Zaawansowany `batch_analyzer.py` |
| **Przeglądanie** | Ręczne | `analysis_viewer.py` |
| **Dashboard** | Statyczne pliki | Automatyczne generowanie |
| **Zarządzanie** | Ręczne czyszczenie | Automatyczne + narzędzia |

## 🔧 Zaawansowane opcje

### Klucze API
```bash
# W environment variables
export OPENWEATHER_KEY="your_key_here"
export WEATHERAPI_KEY="your_key_here"  
export VISUALCROSSING_KEY="your_key_here"

# Lub jako parametry
python run_weather_analysis.py --location warszawa --date-preset yesterday \
    --weatherapi-key YOUR_KEY --visualcrossing-key YOUR_KEY
```

### Cache management
```bash
# Sprawdzenie rozmiaru cache
du -sh cache/

# Czyszczenie starego cache (>30 dni)
find cache/ -type f -mtime +30 -delete

# Pełne czyszczenie cache
rm -rf cache/
```

### Custom locations i batch
```bash
# Własny zestaw lokalizacji (JSON)
echo '{
  "custom_cities": {
    "city1": [52.5, 21.0],
    "city2": [50.0, 19.9]
  }
}' > my_locations.json

python batch_analyzer.py --custom-locations my_locations.json \
    --time-period last_week
```

## 🐛 Rozwiązywanie problemów

### ERA5 / CDS Issues
```bash
# Problem: "No CDS configuration"
# Rozwiązanie: Skonfiguruj ~/.cdsapirc
echo "url: https://cds.climate.copernicus.eu/api
key: YOUR_UID:YOUR_API_KEY" > ~/.cdsapirc

# Problem: "ERA5 data not available"  
# Rozwiązanie: ERA5 ma opóźnienie 5-7 dni, użyj starszych dat

# Problem: "CDS queue too long"
# Rozwiązanie: Spróbuj później lub użyj bez --use-cds
```

### Performance Issues
```bash
# Problem: Wolne batch processing
# Rozwiązanie: Zwiększ liczbę workerów
python batch_analyzer.py ... --max-workers 10

# Problem: Zapełniony cache
# Rozwiązanie: Wyczyść stary cache
find cache/ -type f -mtime +30 -delete
```

### Data Issues
```bash
# Problem: "No provider data"
# Rozwiązanie: Sprawdź klucze API i połączenie internetowe

# Problem: "No central results file"  
# Rozwiązanie: Uruchom pierwszą analizę
python run_weather_analysis.py --location warszawa --date-preset yesterday
```

## 📈 Best Practices

### Dla analiz historycznych
1. **Zawsze używaj `--use-cds`** dla prawdziwych danych referencyjnych
2. **Sprawdź dostępność ERA5** - dane mają 5-7 dni opóźnienia
3. **Używaj okresów >24h** dla lepszych statystyk

### Dla analiz prognoz  
1. **Porównuj API między sobą** bez ERA5 reference
2. **Używaj krótszych okresów** (3-7 dni) dla aktualnych prognoz
3. **Sprawdzaj regularnie** dla śledzenia trendów

### Dla batch processing
1. **Zacznij od małych zestawów** (poland_major)
2. **Używaj cache** - nie czyść bez powodu
3. **Monitoruj memory usage** przy dużych batch

## 📞 Support

**Częste problemy:**
1. **CDS configuration** - sprawdź ~/.cdsapirc
2. **API keys** - sprawdź zmienne środowiskowe
3. **Cache issues** - usuń cache/ i spróbuj ponownie  
4. **Memory issues** - zmniejsz batch size lub zwiększ RAM

**Debug mode:**
```bash
# Więcej informacji o błędach
python -u run_weather_analysis.py --location warszawa --date-preset yesterday --use-cds

# Test połączenia CDS
python -c "import cdsapi; cdsapi.Client().info()"
```

---
*Weather Analysis System v2.1 - Real ERA5 Data + Intelligent Caching* 🌤️