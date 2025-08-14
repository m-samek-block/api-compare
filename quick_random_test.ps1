param(
    [ValidateSet("quick", "medium", "comprehensive", "validator")]
    [string]$Mode = "quick"
)

Write-Host "QUICK RANDOM WEATHER TEST" -ForegroundColor Green
Write-Host "=========================" -ForegroundColor Green

switch ($Mode) {
    "quick" {
        Write-Host "QUICK MODE - 5 random locations" -ForegroundColor Cyan
        Write-Host "Running 5 quick analyses..." -ForegroundColor Yellow
        Remove-Item central_weather_results.csv -ErrorAction SilentlyContinue

        python consolidated_analysis.py --lat 52.7891 --lon 18.2341 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Europe_1"
        python consolidated_analysis.py --lat 45.1234 --lon 8.9876 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Europe_2"
        python consolidated_analysis.py --lat 60.5432 --lon 12.3456 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Scandinavia"
        python consolidated_analysis.py --lat 41.8765 --lon -3.2109 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Iberia"
        python consolidated_analysis.py --lat 55.9876 --lon 25.4321 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Baltic"
    }

    "validator" {
        Write-Host "VALIDATOR MODE - 10 random locations" -ForegroundColor Cyan
        Write-Host "Running validator simulation..." -ForegroundColor Yellow
        Remove-Item central_weather_results.csv -ErrorAction SilentlyContinue

        # 10 losowych lokalizacji z rĂłĹĽnych kontynentĂłw
        python consolidated_analysis.py --lat 52.7891 --lon 18.2341 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Europe_1"
        python consolidated_analysis.py --lat 35.2847 --lon 102.5634 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Asia_1"
        python consolidated_analysis.py --lat -12.4567 --lon 28.8901 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Africa_1"
        python consolidated_analysis.py --lat 45.7623 --lon -98.5417 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_NorthAmerica_1"
        python consolidated_analysis.py --lat -23.8934 --lon -67.2341 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_SouthAmerica_1"
        python consolidated_analysis.py --lat -28.5612 --lon 138.7423 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Australia_1"
        python consolidated_analysis.py --lat 64.1892 --lon -21.7456 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Arctic_1"
        python consolidated_analysis.py --lat 67.8523 --lon 15.2341 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Arctic_2"
        python consolidated_analysis.py --lat 25.7834 --lon 55.2948 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Desert_1"
        python consolidated_analysis.py --lat -15.7823 --lon -47.8934 --start 2025-08-01T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name "Random_Tropical_1"
    }
}

Write-Host "Analysis complete! Running summary..." -ForegroundColor Green
python analysis_viewer.py overview
python analysis_viewer.py compare

if (Test-Path "advanced_bias_analysis.py") {
    Write-Host "Running advanced bias analysis..." -ForegroundColor Cyan
    python advanced_bias_analysis.py
}
