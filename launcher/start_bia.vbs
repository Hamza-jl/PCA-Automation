' ─────────────────────────────────────────────────────────────────
'  BIA Automatique — Démarrer l'application
'  Double-cliquez sur ce fichier pour lancer l'application.
'  Une fenêtre de progression s'affichera brièvement, puis le
'  navigateur s'ouvrira automatiquement sur http://localhost:8000
' ─────────────────────────────────────────────────────────────────

Set oShell = CreateObject("WScript.Shell")
Set oFSO   = CreateObject("Scripting.FileSystemObject")

' ── Trouver le dossier du projet (là où ce .vbs est installé) ────
Dim scriptDir
scriptDir = oFSO.GetParentFolderName(WScript.ScriptFullName)
' Le dossier parent de launcher\ est la racine du projet
Dim projectDir
projectDir = oFSO.GetParentFolderName(scriptDir)

' ── Vérifier que Docker Desktop est lancé ────────────────────────
Dim dockerRunning
dockerRunning = False
Dim i
For i = 1 To 30
    Dim checkResult
    checkResult = oShell.Run("cmd /c docker info >nul 2>&1", 0, True)
    If checkResult = 0 Then
        dockerRunning = True
        Exit For
    End If
    If i = 1 Then
        ' Lancer Docker Desktop si pas encore démarré
        oShell.Run "cmd /c start """" ""C:\Program Files\Docker\Docker\Docker Desktop.exe""", 0, False
    End If
    WScript.Sleep 3000
Next

If Not dockerRunning Then
    MsgBox "Docker Desktop n'a pas pu démarrer." & vbCrLf & _
           "Veuillez l'ouvrir manuellement et réessayer.", _
           vbExclamation, "BIA Automatique"
    WScript.Quit
End If

' ── Démarrer le conteneur ─────────────────────────────────────────
oShell.Run "cmd /c cd /d """ & projectDir & """ && docker compose up -d", 0, True

' ── Attendre que l'application réponde (max 30 s) ────────────────
Dim ready
ready = False
For i = 1 To 15
    WScript.Sleep 2000
    Dim pingResult
    pingResult = oShell.Run("cmd /c curl -s -o nul -w ""%{http_code}"" http://localhost:8000 | findstr 200 >nul 2>&1", 0, True)
    If pingResult = 0 Then
        ready = True
        Exit For
    End If
Next

' ── Ouvrir le navigateur ──────────────────────────────────────────
oShell.Run "cmd /c start http://localhost:8000", 0, False

If Not ready Then
    MsgBox "L'application démarre, cela peut prendre quelques secondes." & vbCrLf & _
           "Le navigateur va s'ouvrir sur http://localhost:8000", _
           vbInformation, "BIA Automatique"
End If
