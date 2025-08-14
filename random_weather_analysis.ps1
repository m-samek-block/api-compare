# ================================================================
# 🎲 RANDOM LOCATION WEATHER ANALYSIS - FIXED VERSION
# Analiza providerów w całkowicie losowych lokalizacjach na świecie
# Symuluje rzeczywiste warunki pracy validatora
# ================================================================

param(
    [int]$LocationCount = 20,
    [int]$MaxWorkers = 3,
    [switch]$IncludeOceans = $false,
    [switch]$CleanStart = $false
)

Write-Host "🎲 RANDOM LOCATION WEATHER ANALYSIS" -ForegroundColor Green
Write-Host "===================================" -ForegroundColor Green
Write-Host "🌍 Generating $LocationCount random locations worldwide" -ForegroundColor Cyan
Write-Host "⚙️ Max parallel workers: $MaxWorkers" -ForegroundColor Cyan
Write-Host "🌊 Include oceans: $IncludeOceans" -ForegroundColor Cyan

# Wyczyść stare wyniki jeśli wymagane
if ($CleanStart) {
    Write-Host "🧹 Cleaning old results..." -ForegroundColor Yellow
    Remove-Item central_weather_results.csv -ErrorAction SilentlyContinue
    Remove-Item dashboard\* -ErrorAction SilentlyContinue
}

# Funkcja generowania losowych koordinat
function Get-RandomCoordinates {
    param(
        [bool]$IncludeOceans = $false
    )

    $locations = @()
    $attempts = 0
    $maxAttempts = $LocationCount * 5  # 5x więcej prób niż potrzeba

    # Obszary lądowe (przybliżone bbox dla kontynentów)
    $landAreas = @(
        # Europa
        @{MinLat=35; MaxLat=71; MinLon=-10; MaxLon=40; Name="Europe"},
        # Azja
        @{MinLat=10; MaxLat=70; MinLon=60; MaxLon=180; Name="Asia"},
        # Ameryka Północna
        @{MinLat=15; MaxLat=70; MinLon=-170; MaxLon=-50; Name="North_America"},
        # Ameryka Południowa
        @{MinLat=-55; MaxLat=15; MinLon=-85; MaxLon=-35; Name="South_America"},
        # Afryka
        @{MinLat=-35; MaxLat=37; MinLon=-20; MaxLon=52; Name="Africa"},
        # Australia
        @{MinLat=-45; MaxLat=-10; MinLon=110; MaxLon=160; Name="Australia"}
    )

    while ($locations.Count -lt $LocationCount -and $attempts -lt $maxAttempts) {
        $attempts++

        if ($IncludeOceans) {
            # Całkowicie losowe (włącznie z oceanami)
            $lat = (Get-Random -Minimum -90 -Maximum 90) + (Get-Random) * 0.0001
            $lon = (Get-Random -Minimum -180 -Maximum 180) + (Get-Random) * 0.0001
            $region = "Ocean_Random"
        } else {
            # Wybierz losowy obszar lądowy
            $area = $landAreas | Get-Random
            $lat = Get-Random -Minimum $area.MinLat -Maximum $area.MaxLat
            $lon = Get-Random -Minimum $area.MinLon -Maximum $area.MaxLon
            $region = $area.Name

            # Dodaj małą losowość dla precyzji
            $lat += (Get-Random) - 0.5
            $lon += (Get-Random) - 0.5

            # Ogranicz do prawidłowych wartości
            $lat = [Math]::Max(-90, [Math]::Min(90, $lat))
            $lon = [Math]::Max(-180, [Math]::Min(180, $lon))
        }

        # Unikaj duplikatów (sprawdź odległość > 1 stopień)
        $tooClose = $false
        foreach ($existing in $locations) {
            $distance = [Math]::Sqrt([Math]::Pow($lat - $existing.Lat, 2) + [Math]::Pow($lon - $existing.Lon, 2))
            if ($distance -lt 1.0) {
                $tooClose = $true
                break
            }
        }

        if (-not $tooClose) {
            $locationName = "Random_$($locations.Count + 1)_$($region.Replace(' ', ''))"
            $locations += @{
                Name = $locationName
                Lat = [Math]::Round($lat, 4)
                Lon = [Math]::Round($lon, 4)
                Region = $region
            }

            Write-Host "📍 Generated: $locationName ($($lat.ToString("F4")), $($lon.ToString("F4"))) in $region" -ForegroundColor White
        }
    }

    if ($locations.Count -lt $LocationCount) {
        Write-Host "⚠️ Generated only $($locations.Count) unique locations (attempted $attempts times)" -ForegroundColor Yellow
    }

    return $locations
}

# Funkcja analizy pojedynczej lokalizacji
function Invoke-LocationAnalysis {
    param(
        [hashtable]$Location,
        [int]$Index,
        [int]$Total
    )

    $name = $Location.Name
    $lat = $Location.Lat
    $lon = $Location.Lon
    $region = $Location.Region

    Write-Host "[$Index/$Total] 📍 Analyzing $name ($region)..." -ForegroundColor Cyan

    try {
        # Użyj krótszego okresu dla szybszej analizy
        $result = python consolidated_analysis.py --lat $lat --lon $lon --start 2025-08-02T00:00:00Z --end 2025-08-05T00:00:00Z --providers openmeteo,weatherapi,visualcrossing --location-name $name 2>&1

        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✅ Success: $name" -ForegroundColor Green
            return @{Status="Success"; Location=$name; Output=$result}
        } else {
            Write-Host "  ❌ Failed: $name (Exit code: $LASTEXITCODE)" -ForegroundColor Red
            return @{Status="Failed"; Location=$name; Error=$result}
        }
    } catch {
        Write-Host "  💥 Exception: $name - $($_.Exception.Message)" -ForegroundColor Red
        return @{Status="Exception"; Location=$name; Error=$_.Exception.Message}
    }
}

# Generuj losowe lokalizacje
Write-Host "`n🎲 GENERATING RANDOM COORDINATES..." -ForegroundColor Cyan
$randomLocations = Get-RandomCoordinates -IncludeOceans:$IncludeOceans

Write-Host "`n📊 SUMMARY OF GENERATED LOCATIONS:" -ForegroundColor Yellow
$regionCounts = $randomLocations | Group-Object Region | Sort-Object Count -Descending
foreach ($regionGroup in $regionCounts) {
    Write-Host "  🌍 $($regionGroup.Name): $($regionGroup.Count) locations" -ForegroundColor White
}

Write-Host "`n🚀 STARTING ANALYSIS..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green

# Analiza lokalizacji (sekwencyjna lub równoległa)
$results = @()
$successful = 0
$failed = 0

if ($MaxWorkers -eq 1) {
    # Analiza sekwencyjna
    Write-Host "📄 Running sequential analysis..." -ForegroundColor Cyan
    for ($i = 0; $i -lt $randomLocations.Count; $i++) {
        $location = $randomLocations[$i]
        $result = Invoke-LocationAnalysis -Location $location -Index ($i + 1) -Total $randomLocations.Count
        $results += $result

        if ($result.Status -eq "Success") {
            $successful++
        } else {
            $failed++
        }

        # Krótka przerwa między analizami
        Start-Sleep -Seconds 2
    }
} else {
    # Analiza równoległa (uproszczona)
    Write-Host "⚡ Running parallel analysis (simplified)..." -ForegroundColor Cyan

    # Podziel lokalizacje na batch-e
    $batchSize = [Math]::Min($MaxWorkers, $randomLocations.Count)
    $batches = @()

    for ($i = 0; $i -lt $randomLocations.Count; $i += $batchSize) {
        $end = [Math]::Min($i + $batchSize - 1, $randomLocations.Count - 1)
        $batches += ,$randomLocations[$i..$end]
    }

    foreach ($batch in $batches) {
        Write-Host "📦 Processing batch of $($batch.Count) locations..." -ForegroundColor Yellow

        foreach ($location in $batch) {
            $index = [Array]::IndexOf($randomLocations, $location) + 1
            $result = Invoke-LocationAnalysis -Location $location -Index $index -Total $randomLocations.Count
            $results += $result

            if ($result.Status -eq "Success") {
                $successful++
            } else {
                $failed++
            }
        }

        # Przerwa między batch-ami
        if ($batch -ne $batches[-1]) {
            Write-Host "⏸️ Batch complete, pausing..." -ForegroundColor Gray
            Start-Sleep -Seconds 5
        }
    }
}

# Podsumowanie wyników
Write-Host "`n🎉 ANALYSIS COMPLETE!" -ForegroundColor Green
Write-Host "===================" -ForegroundColor Green
Write-Host "📊 Results Summary:" -ForegroundColor Yellow

$successRate = if ($randomLocations.Count -gt 0) { [Math]::Round($successful/$randomLocations.Count*100, 1) } else { 0 }
$failRate = if ($randomLocations.Count -gt 0) { [Math]::Round($failed/$randomLocations.Count*100, 1) } else { 0 }

Write-Host "  ✅ Successful: $successful/$($randomLocations.Count) ($successRate)" -ForegroundColor Green
Write-Host "  ❌ Failed: $failed/$($randomLocations.Count) ($failRate)" -ForegroundColor Red

# Szczegóły niepowodzeń
$failedResults = $results | Where-Object { $_.Status -ne "Success" }
if ($failedResults.Count -gt 0) {
    Write-Host "`n⚠️ FAILED LOCATIONS:" -ForegroundColor Red
    foreach ($failure in $failedResults) {
        Write-Host "  ❌ $($failure.Location): $($failure.Status)" -ForegroundColor Red
    }
}

# Analiza geograficzna wyników
Write-Host "`n🌍 GEOGRAPHICAL DISTRIBUTION OF RESULTS:" -ForegroundColor Cyan
$successByRegion = $results | Where-Object { $_.Status -eq "Success" } | ForEach-Object {
    $loc = $randomLocations | Where-Object { $_.Name -eq $_.Location }
    [PSCustomObject]@{Region = $loc.Region; Status = $_.Status}
} | Group-Object Region

foreach ($regionGroup in $successByRegion) {
    $regionTotal = ($randomLocations | Where-Object { $_.Region -eq $regionGroup.Name }).Count
    $regionSuccess = $regionGroup.Count
    $regionSuccessRate = if ($regionTotal -gt 0) { [Math]::Round($regionSuccess / $regionTotal * 100, 1) } else { 0 }
    Write-Host "  🌍 $($regionGroup.Name): $regionSuccess/$regionTotal success ($regionSuccessRate%)" -ForegroundColor White
}

# Uruchom zaawansowaną analizę jeśli mamy wyniki
if ($successful -gt 0) {
    Write-Host "`n📈 RUNNING ADVANCED ANALYSIS..." -ForegroundColor Cyan

    # Podstawowe podsumowanie
    Write-Host "📋 Basic Analysis:" -ForegroundColor Yellow
    python analysis_viewer.py overview

    Write-Host "`n🆚 Provider Comparison:" -ForegroundColor Yellow
    python analysis_viewer.py compare

    Write-Host "`n🗺️ Location Analysis:" -ForegroundColor Yellow
    python analysis_viewer.py locations

    # Eksport wyników
    Write-Host "`n💾 Exporting Results:" -ForegroundColor Yellow
    python analysis_viewer.py export --export-file "random_locations_summary.csv"

    # Zaawansowana analiza bias (jeśli plik istnieje)
    if (Test-Path "advanced_bias_analysis.py") {
        Write-Host "`n🔬 Running Advanced Bias Analysis:" -ForegroundColor Yellow
        python advanced_bias_analysis.py
    } else {
        Write-Host "`n⚠️ advanced_bias_analysis.py not found - skipping bias analysis" -ForegroundColor Yellow
    }
} else {
    Write-Host "`n❌ No successful analyses - cannot run advanced analysis" -ForegroundColor Red
}

# Zapisz szczegóły lokalizacji
Write-Host "`n💾 SAVING LOCATION DETAILS..." -ForegroundColor Cyan
$locationDetails = @()
for ($i = 0; $i -lt $randomLocations.Count; $i++) {
    $loc = $randomLocations[$i]
    $result = $results[$i]

    $locationDetails += [PSCustomObject]@{
        Index = $i + 1
        Name = $loc.Name
        Latitude = $loc.Lat
        Longitude = $loc.Lon
        Region = $loc.Region
        Status = $result.Status
        Success = $result.Status -eq "Success"
    }
}

$locationDetails | Export-Csv "random_locations_details.csv" -NoTypeInformation
Write-Host "✅ Location details saved: random_locations_details.csv" -ForegroundColor Green

# Final summary
Write-Host "`n🎯 FINAL SUMMARY:" -ForegroundColor Green
Write-Host "===============" -ForegroundColor Green
Write-Host "📍 Random locations tested: $($randomLocations.Count)" -ForegroundColor White
Write-Host "✅ Successful analyses: $successful" -ForegroundColor Green
Write-Host "🌍 Regions covered: $($regionCounts.Count)" -ForegroundColor Cyan
Write-Host "📊 Success rate: $successRate%" -ForegroundColor Yellow

Write-Host "`n📁 Generated files:" -ForegroundColor Cyan
Write-Host "  📄 central_weather_results.csv (main results)" -ForegroundColor White
Write-Host "  📄 random_locations_details.csv (location info)" -ForegroundColor White
Write-Host "  📄 random_locations_summary.csv (analysis summary)" -ForegroundColor White
Write-Host "  📁 dashboard/ (charts and reports)" -ForegroundColor White

Write-Host "`n🚀 Next steps:" -ForegroundColor Green
Write-Host "  📊 Review dashboard/bias_analysis.png" -ForegroundColor White
Write-Host "  📋 Read dashboard/comprehensive_weather_report.md" -ForegroundColor White
Write-Host "  📁 Analyze patterns in random_locations_summary.csv" -ForegroundColor White