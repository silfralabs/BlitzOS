# Local Windows prod build, the analog of scripts/dist-mac.sh. Output: release\BlitzOS Setup <version>.exe
#
# Order matters: build the native CU helper FIRST so electron-builder's win.extraResources finds the exe,
# then electron-vite build, then electron-builder --win.
#
# Signing: UNSIGNED by default (electron-builder builds an unsigned installer, which runs fine because the
# helper needs no TCC). For a SIGNED build set the standard electron-builder env vars before running:
#   $env:CSC_LINK = 'path\to\cert.pfx'; $env:CSC_KEY_PASSWORD = '...'
# Signing is a prerequisite for flipping app.manifest uiAccess="true" + a perMachine (Program Files)
# install, which is what lets the helper drive ELEVATED windows. Until then it stays asInvoker.
#
# SYNCED-FOLDER GOTCHA: electron-builder extracts Electron to <output>/win-unpacked.tmp then RENAMES it to
# win-unpacked. On a OneDrive / Dropbox / Google-Drive-synced checkout the sync client locks the freshly
# written files and the rename fails with "EPERM: operation not permitted, rename ... win-unpacked". Fix:
# point directories.output (electron-builder.yml) at a path OUTSIDE the synced tree (e.g. under %LOCALAPPDATA%),
# or pause sync for the repo before building. CI / non-synced checkouts are unaffected and use `release`.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot               # repo root
Set-Location $root

Write-Host "[dist-win] building native CU helper"
& (Join-Path $root 'native\computer-use-helper\build-win.ps1')
if ($LASTEXITCODE -ne 0) { throw "[dist-win] helper build failed ($LASTEXITCODE)" }

# The Windows terminal backend: the supermux ConPTY session-host (conpty-host.mjs drives it over host_rpc).
# Bundled via win.extraResources at vendor/bin/session-host.exe (resolveSessionHostBin reads it at
# process.resourcesPath/bin/session-host.exe). It is a VENDORED binary (like vendor/bin/tmux on mac): a
# checkout already carries it, so a plain build just uses it. To REFRESH from source, set $env:SUPERMUX_REPO
# to the supermux repo root and re-run. Self-contained (ldd: only system DLLs, no MinGW runtime).
$sessionHost = Join-Path $root 'vendor\bin\session-host.exe'
if ($env:SUPERMUX_REPO) {
  Write-Host "[dist-win] building supermux session-host from $env:SUPERMUX_REPO"
  Push-Location (Join-Path $env:SUPERMUX_REPO 'server')
  cargo build --release --bin session-host --target x86_64-pc-windows-gnu
  $built = Join-Path (Get-Location).Path 'target\x86_64-pc-windows-gnu\release\session-host.exe'
  Pop-Location
  if (-not (Test-Path $built)) { throw "[dist-win] session-host build produced no exe at $built" }
  New-Item -ItemType Directory -Force (Split-Path $sessionHost) | Out-Null
  Copy-Item $built $sessionHost -Force
  Write-Host "[dist-win] vendored session-host.exe ($([math]::Round((Get-Item $sessionHost).Length/1MB,1)) MB)"
} elseif (-not (Test-Path $sessionHost)) {
  throw "[dist-win] vendor\bin\session-host.exe missing and SUPERMUX_REPO not set. Set `$env:SUPERMUX_REPO to the supermux repo root to build it."
} else {
  Write-Host "[dist-win] using vendored vendor\bin\session-host.exe (set `$env:SUPERMUX_REPO to refresh from source)"
}

# PREREQUISITE (Windows): Git for Windows must be installed on the END-USER machine. The agent runtime is
# POSIX (claude runs wait.sh / curl / tail / cat via its Bash tool), and Claude Code makes Git Bash optional
# (it falls back to the PowerShell tool, which cannot run that runtime). conpty-host pins the agent to Git
# Bash (CLAUDE_CODE_GIT_BASH_PATH); document Git for Windows in the install instructions.

Write-Host "[dist-win] electron-vite build"
npm run build
if ($LASTEXITCODE -ne 0) { throw "[dist-win] electron-vite build failed ($LASTEXITCODE)" }

if (-not $env:CSC_LINK) {
  # No cert provided: build cleanly unsigned instead of letting electron-builder hunt the cert store.
  $env:CSC_IDENTITY_AUTO_DISCOVERY = 'false'
  Write-Host "[dist-win] UNSIGNED build (set CSC_LINK + CSC_KEY_PASSWORD for a signed installer)"
}

Write-Host "[dist-win] electron-builder --win"
npx electron-builder --win --x64 --publish never
if ($LASTEXITCODE -ne 0) { throw "[dist-win] electron-builder failed ($LASTEXITCODE)" }

Get-ChildItem (Join-Path $root 'release') -ErrorAction SilentlyContinue |
  Where-Object { -not $_.PSIsContainer } | Select-Object Name, @{n='MB';e={[math]::Round($_.Length/1MB,1)}}
