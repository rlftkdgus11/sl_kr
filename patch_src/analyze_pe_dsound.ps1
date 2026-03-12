param(
    [Parameter(Mandatory = $true)]
    [string]$OldDll,
    [Parameter(Mandatory = $true)]
    [string]$CurDll,
    [string]$OutDir = "."
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-U16([byte[]]$b, [int]$o) {
    return [System.BitConverter]::ToUInt16($b, $o)
}

function Read-U32([byte[]]$b, [int]$o) {
    return [System.BitConverter]::ToUInt32($b, $o)
}

function Read-U64([byte[]]$b, [int]$o) {
    return [System.BitConverter]::ToUInt64($b, $o)
}

function Read-CString([byte[]]$b, [int]$o) {
    $sb = New-Object System.Text.StringBuilder
    $i = $o
    while ($i -lt $b.Length -and $b[$i] -ne 0) {
        [void]$sb.Append([char]$b[$i])
        $i++
    }
    return $sb.ToString()
}

function Extract-AsciiStrings([byte[]]$b, [int]$minLen = 6) {
    $res = New-Object System.Collections.Generic.List[string]
    $sb = New-Object System.Text.StringBuilder
    foreach ($x in $b) {
        if ($x -ge 32 -and $x -le 126) {
            [void]$sb.Append([char]$x)
        } else {
            if ($sb.Length -ge $minLen) {
                $res.Add($sb.ToString())
            }
            $null = $sb.Clear()
        }
    }
    if ($sb.Length -ge $minLen) {
        $res.Add($sb.ToString())
    }
    return $res
}

function Parse-PE([string]$path) {
    $bytes = [System.IO.File]::ReadAllBytes($path)
    if ((Read-U16 $bytes 0) -ne 0x5A4D) {
        throw "Not an MZ binary: $path"
    }
    $peOff = [int](Read-U32 $bytes 0x3C)
    if ((Read-U32 $bytes $peOff) -ne 0x00004550) {
        throw "Invalid PE signature: $path"
    }
    $coff = $peOff + 4
    $machine = Read-U16 $bytes $coff
    $numSections = Read-U16 $bytes ($coff + 2)
    $timeStamp = Read-U32 $bytes ($coff + 4)
    $sizeOpt = Read-U16 $bytes ($coff + 16)
    $opt = $coff + 20
    $magic = Read-U16 $bytes $opt
    $isPE32 = $magic -eq 0x10B
    $isPE64 = $magic -eq 0x20B
    if (-not ($isPE32 -or $isPE64)) {
        throw "Unknown optional header magic 0x{0:X}" -f $magic
    }

    $entryPointRva = Read-U32 $bytes ($opt + 16)
    $imageBase = if ($isPE32) { [uint64](Read-U32 $bytes ($opt + 28)) } else { Read-U64 $bytes ($opt + 24) }
    $numDataDirs = if ($isPE32) { [int](Read-U32 $bytes ($opt + 92)) } else { [int](Read-U32 $bytes ($opt + 108)) }
    $dataDirOff = if ($isPE32) { $opt + 96 } else { $opt + 112 }

    $sections = @()
    $secOff = $opt + $sizeOpt
    for ($i = 0; $i -lt $numSections; $i++) {
        $o = $secOff + ($i * 40)
        $nameBytes = $bytes[$o..($o + 7)]
        $name = ([System.Text.Encoding]::ASCII.GetString($nameBytes)).Trim([char]0)
        $vSize = Read-U32 $bytes ($o + 8)
        $vAddr = Read-U32 $bytes ($o + 12)
        $rawSize = Read-U32 $bytes ($o + 16)
        $rawPtr = Read-U32 $bytes ($o + 20)
        $chars = Read-U32 $bytes ($o + 36)
        $sections += [pscustomobject]@{
            Name = $name
            VirtualAddress = $vAddr
            VirtualSize = $vSize
            RawSize = $rawSize
            RawPointer = $rawPtr
            Characteristics = ('0x{0:X8}' -f $chars)
        }
    }

    function Rva-ToOffset([uint32]$rva) {
        foreach ($s in $sections) {
            $start = [uint32]$s.VirtualAddress
            $maxSize = [uint32][Math]::Max([int]$s.VirtualSize, [int]$s.RawSize)
            $end = $start + $maxSize
            if ($rva -ge $start -and $rva -lt $end) {
                return [int]([uint32]$s.RawPointer + ($rva - $start))
            }
        }
        return -1
    }

    $dirs = @{}
    $dirNames = @(
        "Export", "Import", "Resource", "Exception", "Security", "BaseReloc",
        "Debug", "Architecture", "GlobalPtr", "TLS", "LoadConfig", "BoundImport",
        "IAT", "DelayImport", "COMDescriptor", "Reserved"
    )
    for ($i = 0; $i -lt [Math]::Min($numDataDirs, 16); $i++) {
        $o = $dataDirOff + ($i * 8)
        $rva = Read-U32 $bytes $o
        $size = Read-U32 $bytes ($o + 4)
        $dirs[$dirNames[$i]] = [pscustomobject]@{
            RVA = $rva
            Size = $size
            Offset = if ($rva -ne 0) { Rva-ToOffset $rva } else { -1 }
        }
    }

    $exports = @()
    if ($dirs.ContainsKey("Export") -and $dirs["Export"].RVA -ne 0 -and $dirs["Export"].Offset -ge 0) {
        $eOff = [int]$dirs["Export"].Offset
        $baseOrd = Read-U32 $bytes ($eOff + 16)
        $numFuncs = Read-U32 $bytes ($eOff + 20)
        $numNames = Read-U32 $bytes ($eOff + 24)
        $aofRva = Read-U32 $bytes ($eOff + 28)
        $aonRva = Read-U32 $bytes ($eOff + 32)
        $aooRva = Read-U32 $bytes ($eOff + 36)
        $aofOff = Rva-ToOffset $aofRva
        $aonOff = Rva-ToOffset $aonRva
        $aooOff = Rva-ToOffset $aooRva

        $expStart = [uint32]$dirs["Export"].RVA
        $expEnd = $expStart + [uint32]$dirs["Export"].Size

        for ($i = 0; $i -lt $numNames; $i++) {
            $nameRva = Read-U32 $bytes ($aonOff + ($i * 4))
            $nameOff = Rva-ToOffset $nameRva
            $name = if ($nameOff -ge 0) { Read-CString $bytes $nameOff } else { "" }
            $ordIndex = Read-U16 $bytes ($aooOff + ($i * 2))
            $funcRva = Read-U32 $bytes ($aofOff + ($ordIndex * 4))
            $ord = [int]($baseOrd + $ordIndex)
            $isForwarder = ($funcRva -ge $expStart -and $funcRva -lt $expEnd)
            $fwd = ""
            if ($isForwarder) {
                $fwdOff = Rva-ToOffset $funcRva
                if ($fwdOff -ge 0) { $fwd = Read-CString $bytes $fwdOff }
            }
            $exports += [pscustomobject]@{
                Name = $name
                Ordinal = $ord
                RVA = ('0x{0:X8}' -f $funcRva)
                Forwarder = $fwd
            }
        }
    }

    $imports = @()
    if ($dirs.ContainsKey("Import") -and $dirs["Import"].RVA -ne 0 -and $dirs["Import"].Offset -ge 0) {
        $dOff = [int]$dirs["Import"].Offset
        $ptrSize = if ($isPE32) { 4 } else { 8 }
        $ordinalFlag = if ($isPE32) {
            [System.Convert]::ToUInt64("80000000", 16)
        } else {
            [System.Convert]::ToUInt64("8000000000000000", 16)
        }

        for ($idx = 0; $idx -lt 4096; $idx++) {
            $o = $dOff + ($idx * 20)
            $origThunk = Read-U32 $bytes $o
            $nameRva = Read-U32 $bytes ($o + 12)
            $firstThunk = Read-U32 $bytes ($o + 16)
            if (($origThunk -eq 0) -and ($nameRva -eq 0) -and ($firstThunk -eq 0)) { break }
            $dllName = ""
            $nameOff = Rva-ToOffset $nameRva
            if ($nameOff -ge 0) { $dllName = Read-CString $bytes $nameOff }
            $thunkRva = if ($origThunk -ne 0) { $origThunk } else { $firstThunk }
            $thunkOff = Rva-ToOffset $thunkRva
            if ($thunkOff -lt 0) { continue }

            for ($ti = 0; $ti -lt 16384; $ti++) {
                $to = $thunkOff + ($ti * $ptrSize)
                $val = if ($isPE32) { [uint64](Read-U32 $bytes $to) } else { Read-U64 $bytes $to }
                if ($val -eq 0) { break }
                $isOrd = (($val -band $ordinalFlag) -ne 0)
                if ($isOrd) {
                    $ord = [int]($val -band 0xFFFF)
                    $imports += [pscustomobject]@{ DLL = $dllName; Name = ""; Ordinal = $ord }
                } else {
                    $ibnRva = [uint32]$val
                    $ibnOff = Rva-ToOffset $ibnRva
                    if ($ibnOff -ge 0) {
                        $hint = Read-U16 $bytes $ibnOff
                        $fname = Read-CString $bytes ($ibnOff + 2)
                        $imports += [pscustomobject]@{ DLL = $dllName; Name = $fname; Ordinal = $hint }
                    }
                }
            }
        }
    }

    $allStrings = Extract-AsciiStrings $bytes 6
    $anchorRegex = '(build_id|MMFS2_|UNLOCK_|HASH|SWEEP|HANDUNIT|fingerprint|hash_only|forensic|subtitles_|sound_hash_map|sound_stream_chunk_map|dsound\.dll|DirectSound)'
    $anchors = $allStrings | Where-Object { $_ -match $anchorRegex } | Select-Object -Unique

    return [pscustomobject]@{
        Path = $path
        FileSize = $bytes.Length
        Machine = ('0x{0:X4}' -f $machine)
        TimeDateStamp = ('0x{0:X8}' -f $timeStamp)
        IsPE32 = $isPE32
        EntryPointRVA = ('0x{0:X8}' -f $entryPointRva)
        ImageBase = ('0x{0:X}' -f $imageBase)
        Sections = $sections
        DataDirectories = $dirs
        Exports = $exports
        Imports = $imports
        AnchorStrings = $anchors
    }
}

function Write-Json([object]$o, [string]$path) {
    $o | ConvertTo-Json -Depth 8 | Set-Content -Path $path -Encoding UTF8
}

function Write-Lines([string]$path, [string[]]$lines) {
    if ($null -eq $lines) {
        $lines = @()
    }
    [System.IO.File]::WriteAllLines($path, $lines, [System.Text.Encoding]::UTF8)
}

function Summarize([object]$pe) {
    $importDlls = $pe.Imports | Group-Object DLL | Sort-Object Count -Descending
    $topDlls = $importDlls | Select-Object -First 20
    return [pscustomobject]@{
        Path = $pe.Path
        FileSize = $pe.FileSize
        Machine = $pe.Machine
        IsPE32 = $pe.IsPE32
        ExportCount = @($pe.Exports).Count
        ImportCount = @($pe.Imports).Count
        ImportDllCount = @($importDlls).Count
        TopImportDlls = ($topDlls | ForEach-Object { "{0}:{1}" -f $_.Name, $_.Count })
        AnchorCount = @($pe.AnchorStrings).Count
    }
}

function Compare-Sets([string[]]$a, [string[]]$b) {
    $ha = New-Object System.Collections.Generic.HashSet[string]([StringComparer]::OrdinalIgnoreCase)
    $hb = New-Object System.Collections.Generic.HashSet[string]([StringComparer]::OrdinalIgnoreCase)
    foreach ($x in $a) { [void]$ha.Add($x) }
    foreach ($x in $b) { [void]$hb.Add($x) }
    $onlyA = @()
    $onlyB = @()
    foreach ($x in $ha) { if (-not $hb.Contains($x)) { $onlyA += $x } }
    foreach ($x in $hb) { if (-not $ha.Contains($x)) { $onlyB += $x } }
    return [pscustomobject]@{
        OnlyA = $onlyA | Sort-Object
        OnlyB = $onlyB | Sort-Object
    }
}

if (-not (Test-Path $OldDll)) { throw "OldDll not found: $OldDll" }
if (-not (Test-Path $CurDll)) { throw "CurDll not found: $CurDll" }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$old = Parse-PE $OldDll
$cur = Parse-PE $CurDll

$oldJson = Join-Path $OutDir ("old_dsound_pe_{0}.json" -f $stamp)
$curJson = Join-Path $OutDir ("cur_dsound_pe_{0}.json" -f $stamp)
Write-Json $old $oldJson
Write-Json $cur $curJson

$oldExportNames = $old.Exports | ForEach-Object { $_.Name }
$curExportNames = $cur.Exports | ForEach-Object { $_.Name }
$expCmp = Compare-Sets $oldExportNames $curExportNames

$oldImportNames = $old.Imports | ForEach-Object { "{0}!{1}" -f $_.DLL, ($(if ($_.Name) { $_.Name } else { "#"+$_.Ordinal })) }
$curImportNames = $cur.Imports | ForEach-Object { "{0}!{1}" -f $_.DLL, ($(if ($_.Name) { $_.Name } else { "#"+$_.Ordinal })) }
$impCmp = Compare-Sets $oldImportNames $curImportNames

$anchorCmp = Compare-Sets $old.AnchorStrings $cur.AnchorStrings

$summary = @()
$summary += "old_path=$($old.Path)"
$summary += "cur_path=$($cur.Path)"
$summary += "old_size=$($old.FileSize)"
$summary += "cur_size=$($cur.FileSize)"
$summary += "old_exports=$(@($old.Exports).Count)"
$summary += "cur_exports=$(@($cur.Exports).Count)"
$summary += "old_imports=$(@($old.Imports).Count)"
$summary += "cur_imports=$(@($cur.Imports).Count)"
$summary += "old_anchor_strings=$(@($old.AnchorStrings).Count)"
$summary += "cur_anchor_strings=$(@($cur.AnchorStrings).Count)"
$summary += "exports_only_old=$(@($expCmp.OnlyA).Count)"
$summary += "exports_only_cur=$(@($expCmp.OnlyB).Count)"
$summary += "imports_only_old=$(@($impCmp.OnlyA).Count)"
$summary += "imports_only_cur=$(@($impCmp.OnlyB).Count)"
$summary += "anchor_only_old=$(@($anchorCmp.OnlyA).Count)"
$summary += "anchor_only_cur=$(@($anchorCmp.OnlyB).Count)"

$summaryPath = Join-Path $OutDir ("dsound_pe_compare_summary_{0}.txt" -f $stamp)
Write-Lines $summaryPath $summary

$expOnlyOldPath = Join-Path $OutDir ("dsound_exports_only_old_{0}.txt" -f $stamp)
$expOnlyCurPath = Join-Path $OutDir ("dsound_exports_only_cur_{0}.txt" -f $stamp)
$impOnlyOldPath = Join-Path $OutDir ("dsound_imports_only_old_{0}.txt" -f $stamp)
$impOnlyCurPath = Join-Path $OutDir ("dsound_imports_only_cur_{0}.txt" -f $stamp)
$ancOnlyOldPath = Join-Path $OutDir ("dsound_anchor_only_old_{0}.txt" -f $stamp)
$ancOnlyCurPath = Join-Path $OutDir ("dsound_anchor_only_cur_{0}.txt" -f $stamp)

Write-Lines $expOnlyOldPath $expCmp.OnlyA
Write-Lines $expOnlyCurPath $expCmp.OnlyB
Write-Lines $impOnlyOldPath $impCmp.OnlyA
Write-Lines $impOnlyCurPath $impCmp.OnlyB
Write-Lines $ancOnlyOldPath $anchorCmp.OnlyA
Write-Lines $ancOnlyCurPath $anchorCmp.OnlyB

$oldTopImports = ($old.Imports | Group-Object DLL | Sort-Object Count -Descending | Select-Object -First 20 | ForEach-Object { "{0}`t{1}" -f $_.Name, $_.Count })
$curTopImports = ($cur.Imports | Group-Object DLL | Sort-Object Count -Descending | Select-Object -First 20 | ForEach-Object { "{0}`t{1}" -f $_.Name, $_.Count })

$oldTopImportsPath = Join-Path $OutDir ("old_dsound_top_import_dlls_{0}.tsv" -f $stamp)
$curTopImportsPath = Join-Path $OutDir ("cur_dsound_top_import_dlls_{0}.tsv" -f $stamp)
Write-Lines $oldTopImportsPath $oldTopImports
Write-Lines $curTopImportsPath $curTopImports

Write-Output ("summary={0}" -f (Resolve-Path $summaryPath).Path)
Write-Output ("old_json={0}" -f (Resolve-Path $oldJson).Path)
Write-Output ("cur_json={0}" -f (Resolve-Path $curJson).Path)
Write-Output ("exports_only_old={0}" -f (Resolve-Path $expOnlyOldPath).Path)
Write-Output ("exports_only_cur={0}" -f (Resolve-Path $expOnlyCurPath).Path)
Write-Output ("imports_only_old={0}" -f (Resolve-Path $impOnlyOldPath).Path)
Write-Output ("imports_only_cur={0}" -f (Resolve-Path $impOnlyCurPath).Path)
Write-Output ("anchor_only_old={0}" -f (Resolve-Path $ancOnlyOldPath).Path)
Write-Output ("anchor_only_cur={0}" -f (Resolve-Path $ancOnlyCurPath).Path)
Write-Output ("old_top_import_dlls={0}" -f (Resolve-Path $oldTopImportsPath).Path)
Write-Output ("cur_top_import_dlls={0}" -f (Resolve-Path $curTopImportsPath).Path)
