' ─────────────────────────────────────────────────────────────────
'  BIA Automatique — Arrêter l'application
' ─────────────────────────────────────────────────────────────────

Set oShell = CreateObject("WScript.Shell")
Set oFSO   = CreateObject("Scripting.FileSystemObject")

Dim scriptDir
scriptDir = oFSO.GetParentFolderName(WScript.ScriptFullName)
Dim projectDir
projectDir = oFSO.GetParentFolderName(scriptDir)

Dim answer
answer = MsgBox("Arrêter BIA Automatique ?", vbYesNo + vbQuestion, "BIA Automatique")
If answer = vbNo Then WScript.Quit

oShell.Run "cmd /c cd /d """ & projectDir & """ && docker compose down", 0, True

MsgBox "Application arrêtée.", vbInformation, "BIA Automatique"
