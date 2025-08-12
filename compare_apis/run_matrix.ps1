# --- config ---
$Providers    = "openmeteo,metno,openweather,weatherapi,visualcrossing"
$WindAlpha    = "0.143"
$SleepSeconds = 5

# paths relative to this script
$Root     = $PSScriptRoot
$DataDir  = Join-Path $Root "dane"
$OutDir   = Join-Path $Root "wyniki"
New-Item -Type Directory -Force $DataDir | Out-Null
New-Item -Type Directory -Force $OutDir  | Out-Null

# pick python exe (prefer venv)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  $pyCmd = Get-Command python -ErrorAction SilentlyContinue
  if ($pyCmd) { $Python = $pyCmd.Source } else { throw "python executable not found" }
}

# script files
$MakeEra5 = Join-Path $Root "make_era5.py"
$Fetch    = Join-Path $Root "fetch_forecasts.py"
$Compare  = Join-Path $Root "run_compare.py"

# API keys from ENV (optional)
$owm = $env:OPENWEATHER_KEY
$wap = $env:WEATHERAPI_KEY
$vc  = $env:VISUALCROSSING_KEY

function Slug($s) { ($s.ToLower() -replace '[^a-z0-9]+','-').Trim('-') }

# snap datetime to exact hour (UTC)
function SnapHourUtc([datetime]$dt) {
  $u = $dt.ToUniversalTime()
  return $u.AddMinutes(-$u.Minute).AddSeconds(-$u.Second).AddMilliseconds(-$u.Millisecond)
}

# cities: PL + abroad
$Cities = @(
  @{ name="Warszawa";  lat=52.2297;  lon=21.0122 },
  @{ name="Krakow";    lat=50.0647;  lon=19.9450 },
  @{ name="Gdansk";    lat=54.3520;  lon=18.6466 },
  @{ name="Poznan";    lat=52.4064;  lon=16.9252 },
  @{ name="Zakopane";  lat=49.2992;  lon=19.9496 },
  @{ name="Hel";       lat=54.6080;  lon=18.8000 },

  @{ name="London";    lat=51.5074;  lon=-0.1278 },
  @{ name="Bergen";    lat=60.39299; lon=5.32415 },
  @{ name="Reykjavik"; lat=64.1466;  lon=-21.9426 },

  @{ name="Athens";    lat=37.9838;  lon=23.7275 },
  @{ name="Lisbon";    lat=38.7223;  lon=-9.1393 },
  @{ name="Barcelona"; lat=41.3874;  lon=2.1686 },
  @{ name="Istanbul";  lat=41.0082;  lon=28.9784 },

  @{ name="Cairo";     lat=30.0444;  lon=31.2357 },
  @{ name="Dubai";     lat=25.2048;  lon=55.2708 },

  @{ name="Mumbai";    lat=19.0760;  lon=72.8777 },
  @{ name="Singapore"; lat=1.3521;   lon=103.8198 },

  @{ name="NewYork";   lat=40.7128;  lon=-74.0060 },
  @{ name="Miami";     lat=25.7617;  lon=-80.1918 },
  @{ name="Denver";    lat=39.7392;  lon=-104.9903 },

  @{ name="Sydney";    lat=-33.8688; lon=151.2093 }
)

# three 48h windows (hour-aligned)
$Windows = @(
  @{ offsetDays=0;  lengthHours=48 },
  @{ offsetDays=7;  lengthHours=48 },
  @{ offsetDays=14; lengthHours=48 }
)

foreach ($win in $Windows) {
  $base   = SnapHourUtc (Get-Date)
  $startD = $base.AddDays($win.offsetDays)
  $endD   = $startD.AddHours($win.lengthHours)

  $start  = $startD.ToString("yyyy-MM-ddTHH:mm:ssZ")
  $end    = $endD.ToString("yyyy-MM-ddTHH:mm:ssZ")
  $startDay = $start.Substring(0,10)
  $endDay   = $end.Substring(0,10)

  foreach ($c in $Cities) {
    $lat = [string]$c.lat; $lon = [string]$c.lon; $name = [string]$c.name
    $slug = Slug $name
    Write-Host ""
    Write-Host "=== $name [$lat,$lon] | $start -> $end ==="

    # 0) ERA5 for this point and window
    $eraArgs = @($MakeEra5,"--lat",$lat,"--lon",$lon,"--start",$start,"--end",$end,"--out",(Join-Path $DataDir "era5.csv"))
    Write-Host ">> $Python $($eraArgs -join ' ')"
    & $Python $eraArgs
    if ($LASTEXITCODE -ne 0) { throw "make_era5 failed for $name $start -> $end" }

    # 1) FETCH
    $fetchArgs = @(
      $Fetch,
      "--lat",$lat,"--lon",$lon,
      "--start",$start,"--end",$end,
      "--providers",$Providers,
      "--outdir",$DataDir
    )
    if ($owm) { $fetchArgs += @("--openweather-key",$owm) }
    if ($wap) { $fetchArgs += @("--weatherapi-key",$wap) }
    if ($vc)  { $fetchArgs += @("--visualcrossing-key",$vc) }
    Write-Host ">> $Python $($fetchArgs -join ' ')"
    & $Python $fetchArgs
    if ($LASTEXITCODE -ne 0) { throw "fetch_forecasts failed for $name $start -> $end" }

    # 2) COMPARE
    $compareArgs = @(
      $Compare,
      "--lat",$lat,"--lon",$lon,
      "--start",$start,"--end",$end,
      "--providers",$Providers,
      "--era5",(Join-Path $DataDir "era5.csv"),
      "--outdir",$OutDir,
      "--wind-alpha",$WindAlpha
    )
    if ($owm) { $compareArgs += @("--openweather-key",$owm) }
    if ($wap) { $compareArgs += @("--weatherapi-key",$wap) }
    if ($vc)  { $compareArgs += @("--visualcrossing-key",$vc) }
    Write-Host ">> $Python $($compareArgs -join ' ')"
    & $Python $compareArgs
    if ($LASTEXITCODE -ne 0) { throw "run_compare failed for $name $start -> $end" }

    # 3) rename summary: city + dates + timestamp
    $summary = Join-Path $OutDir "era5_comparison_summary.csv"
    if (Test-Path $summary) {
      $ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmssZ")
      $target = Join-Path $OutDir ("era5_comparison_summary_{0}_{1}_{2}_{3}.csv" -f $slug,$startDay,$endDay,$ts)
      Move-Item $summary $target -Force
      Write-Host "[OK] $target"
    } else {
      Write-Warning "No summary file to rename for $name ($startDay -> $endDay)."
    }

    Start-Sleep -Seconds $SleepSeconds
  }

  Start-Sleep -Seconds ($SleepSeconds * 2)
}

Write-Host ""
Write-Host "Done. Then run history analysis:"
Write-Host "  $env:MPLBACKEND=""Agg"" ; python .\analyze_summaries.py --summaries-dir $OutDir --outdir .\wyniki\analysis_history"
