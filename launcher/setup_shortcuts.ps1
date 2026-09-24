# ─────────────────────────────────────────────────────────────────────────────
#  BIA Automatique — Création des raccourcis bureau
#  Exécutez ce script UNE SEULE FOIS sur le PC cible.
#  Clic droit → "Exécuter avec PowerShell"
# ─────────────────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"

# ── Chemins ──────────────────────────────────────────────────────────────────
$launcherDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir  = Split-Path -Parent $launcherDir
$desktop     = [Environment]::GetFolderPath("Desktop")
$iconPath    = Join-Path $launcherDir "bia_icon.ico"
$pngSource   = Join-Path $projectDir "static\devoteam_logo.png"

# ── Convertir le logo PNG en ICO (via System.Drawing) ────────────────────────
Write-Host "Création de l'icône..." -ForegroundColor Cyan

Add-Type -AssemblyName System.Drawing

function ConvertTo-Ico {
    param([string]$PngPath, [string]$IcoPath)

    $bmp = [System.Drawing.Bitmap]::new($PngPath)

    # Redimensionner en 256x256 pour une belle icône
    $resized = [System.Drawing.Bitmap]::new(256, 256)
    $g = [System.Drawing.Graphics]::FromImage($resized)
    $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
    $g.DrawImage($bmp, 0, 0, 256, 256)
    $g.Dispose()
    $bmp.Dispose()

    # Construire le format ICO manuellement (un seul frame 256x256)
    $ms = New-Object System.IO.MemoryStream
    $resized.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png)
    $pngBytes = $ms.ToArray()
    $ms.Dispose()
    $resized.Dispose()

    $fs = [System.IO.File]::OpenWrite($IcoPath)
    $bw = New-Object System.IO.BinaryWriter($fs)

    # ICO header
    $bw.Write([uint16]0)          # reserved
    $bw.Write([uint16]1)          # type = ICO
    $bw.Write([uint16]1)          # number of images

    # Image directory entry (16 bytes)
    $bw.Write([byte]0)            # width  0 = 256
    $bw.Write([byte]0)            # height 0 = 256
    $bw.Write([byte]0)            # color count
    $bw.Write([byte]0)            # reserved
    $bw.Write([uint16]1)          # color planes
    $bw.Write([uint16]32)         # bits per pixel
    $bw.Write([uint32]$pngBytes.Length)
    $bw.Write([uint32]22)         # offset to image data (6 header + 16 dir = 22)

    # PNG image data
    $bw.Write($pngBytes)
    $bw.Close()
    $fs.Close()
}

try {
    ConvertTo-Ico -PngPath $pngSource -IcoPath $iconPath
    Write-Host "Icône créée : $iconPath" -ForegroundColor Green
} catch {
    Write-Host "Avertissement : impossible de créer l'icône personnalisée. L'icône par défaut sera utilisée." -ForegroundColor Yellow
    $iconPath = $null
}

# ── Fonction générique pour créer un raccourci ───────────────────────────────
function New-Shortcut {
    param(
        [string]$ShortcutPath,
        [string]$Target,
        [string]$Arguments,
        [string]$Description,
        [string]$IconPath,
        [string]$WorkingDir
    )
    $shell    = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($ShortcutPath)
    $shortcut.TargetPath       = $Target
    $shortcut.Arguments        = $Arguments
    $shortcut.Description      = $Description
    $shortcut.WorkingDirectory = $WorkingDir
    if ($IconPath) { $shortcut.IconLocation = $IconPath }
    $shortcut.Save()
}

# ── Raccourci 1 : Démarrer BIA ───────────────────────────────────────────────
$startLnk = Join-Path $desktop "BIA Automatique.lnk"
New-Shortcut `
    -ShortcutPath $startLnk `
    -Target       "wscript.exe" `
    -Arguments    """$launcherDir\start_bia.vbs""" `
    -Description  "Lancer l'application BIA Automatique" `
    -IconPath     $(if ($iconPath) { $iconPath } else { $null }) `
    -WorkingDir   $projectDir

Write-Host "Raccourci créé : $startLnk" -ForegroundColor Green

# ── Raccourci 2 : Arrêter BIA ────────────────────────────────────────────────
$stopLnk = Join-Path $desktop "Arrêter BIA.lnk"
New-Shortcut `
    -ShortcutPath $stopLnk `
    -Target       "wscript.exe" `
    -Arguments    """$launcherDir\stop_bia.vbs""" `
    -Description  "Arrêter l'application BIA Automatique" `
    -IconPath     $(if ($iconPath) { $iconPath } else { $null }) `
    -WorkingDir   $projectDir

Write-Host "Raccourci créé : $stopLnk" -ForegroundColor Green

Write-Host ""
Write-Host "Installation terminee !" -ForegroundColor Green
Write-Host "  Deux icones ont ete ajoutees sur le bureau :" -ForegroundColor White
Write-Host "   - 'BIA Automatique'  : demarre l'app et ouvre le navigateur" -ForegroundColor White
Write-Host "   - 'Arreter BIA'      : arrete proprement le conteneur" -ForegroundColor White
Write-Host ""
Read-Host "Appuyez sur Entrée pour fermer"
