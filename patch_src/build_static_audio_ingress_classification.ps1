param(
    [string]$Root = (Split-Path -Parent $PSScriptRoot),
    [string]$OutFile = "static_audio_ingress_classification_20260225.tsv"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-Tsv {
    param([string]$Path)
    Import-Csv -Path $Path -Delimiter "`t"
}

function Parse-Int {
    param([object]$Value)
    $out = 0
    if ([int]::TryParse([string]$Value, [ref]$out)) {
        return $out
    }
    return $null
}

function Join-SortedUnique {
    param([System.Collections.IEnumerable]$Items)
    if (-not $Items) { return "" }
    $vals = @($Items | Where-Object { $_ -ne $null -and "$_" -ne "" } | ForEach-Object { "$_" } | Sort-Object -Unique)
    return ($vals -join ",")
}

$soundNamesPath = Join-Path $Root "sound_names.csv"
$flagsPath = Join-Path $Root "mfa_sound_flags.tsv"
$playRoutesPath = Join-Path $Root "._static_mfa_play_routes_numeric_20260223.tsv"
$semanticsPath = Join-Path $Root "_static_all_handles_effective_subtitle_semantics_20260219.tsv"
$actionMatrixPath = Join-Path $Root "_static_actionnum_dispatch_wrapper_route_matrix_20260219.tsv"
$byNameBitPath = Join-Path $Root "_static_active_action_ord1077_byname_bit_profile_20260219.tsv"
$babyPointsPath = Join-Path $Root "._static_mfa_babyfamily_route_points_20260223.tsv"

$required = @(
    $soundNamesPath,
    $flagsPath,
    $playRoutesPath,
    $semanticsPath,
    $actionMatrixPath,
    $byNameBitPath,
    $babyPointsPath
)
foreach ($p in $required) {
    if (-not (Test-Path $p)) {
        throw "required input missing: $p"
    }
}

$soundNames = Import-Csv -Path $soundNamesPath
$flags = Read-Tsv -Path $flagsPath
$playRoutes = Read-Tsv -Path $playRoutesPath
$semantics = Read-Tsv -Path $semanticsPath
$actionMatrix = Read-Tsv -Path $actionMatrixPath
$byNameBits = Read-Tsv -Path $byNameBitPath
$babyPoints = Read-Tsv -Path $babyPointsPath

$flagByHandle = @{}
foreach ($r in $flags) {
    $h = Parse-Int $r.handle
    if ($h -ne $null) { $flagByHandle[$h] = $r }
}

$semByHandle = @{}
foreach ($r in $semantics) {
    $h = Parse-Int $r.handle
    if ($h -ne $null) { $semByHandle[$h] = $r }
}

$actionMetaByNum = @{}
foreach ($r in $actionMatrix) {
    $n = Parse-Int $r.action_num
    if ($n -ne $null) { $actionMetaByNum[$n] = $r }
}

$byNameBitByAction = @{}
foreach ($r in $byNameBits) {
    $n = Parse-Int $r.action_num
    if ($n -ne $null) { $byNameBitByAction[$n] = $r.has_byname_bit }
}

$babySet = New-Object "System.Collections.Generic.HashSet[int]"
foreach ($r in $babyPoints) {
    $h = Parse-Int $r.canonical_handle
    if ($h -ne $null) { [void]$babySet.Add($h) }
}

$playByHandle = @{}
foreach ($r in $playRoutes) {
    $sample = Parse-Int $r.sample_handle
    if ($sample -eq $null) { continue }
    $h = $sample + 1
    if (-not $playByHandle.ContainsKey($h)) {
        $playByHandle[$h] = [ordered]@{
            count = 0
            actionNums = New-Object "System.Collections.Generic.List[int]"
            dispatchFuncs = New-Object "System.Collections.Generic.List[string]"
            routeSites = New-Object "System.Collections.Generic.List[string]"
            frames = New-Object "System.Collections.Generic.List[string]"
        }
    }
    $slot = $playByHandle[$h]
    $slot.count++
    $a = Parse-Int $r.action_num
    if ($a -ne $null) { $slot.actionNums.Add($a) }
    if ($r.dispatch_target_func) { $slot.dispatchFuncs.Add($r.dispatch_target_func) }
    if ($r.route_call_site) { $slot.routeSites.Add($r.route_call_site) }
    $frame = "$($r.frame):g$($r.group_index)"
    $slot.frames.Add($frame)
}

$rows = New-Object "System.Collections.Generic.List[object]"
foreach ($sn in ($soundNames | Sort-Object { [int]$_.handle })) {
    $h = Parse-Int $sn.handle
    if ($h -eq $null) { continue }

    $flag = $null
    if ($flagByHandle.ContainsKey($h)) { $flag = $flagByHandle[$h] }
    $sem = $null
    if ($semByHandle.ContainsKey($h)) { $sem = $semByHandle[$h] }
    $play = $null
    if ($playByHandle.ContainsKey($h)) { $play = $playByHandle[$h] }

    $wave = if ($flag) { [string]$flag.Wave } else { "" }
    $midi = if ($flag) { [string]$flag.MIDI } else { "" }
    $bankType = if ($flag) { [string]$flag.type } else { [string]$sn.type }

    $isStreamExpected = 0
    if ($sem -and (Parse-Int $sem.stream_expected) -eq 1) { $isStreamExpected = 1 }
    if ($babySet.Contains($h)) { $isStreamExpected = 1 }

    $dialogClass = "non_dialogue_or_unmapped"
    if ($sem) {
        $isDlg = Parse-Int $sem.is_dialogue
        $isSfxOnly = Parse-Int $sem.is_sound_only
        if ($isDlg -eq 1) {
            $dialogClass = "dialogue"
        } elseif ($isSfxOnly -eq 1) {
            $dialogClass = "sound_only"
        } elseif ($isDlg -eq 0 -and $isSfxOnly -eq 0) {
            $dialogClass = "subtitle_non_dialogue"
        }
    }

    $playCount = 0
    $actionNums = @()
    $dispatchFuncs = @()
    $routeSites = @()
    $frames = @()
    if ($play) {
        $playCount = [int]$play.count
        $actionNums = @($play.actionNums)
        $dispatchFuncs = @($play.dispatchFuncs)
        $routeSites = @($play.routeSites)
        $frames = @($play.frames)
    }

    $actionNumCsv = Join-SortedUnique $actionNums
    $dispatchCsv = Join-SortedUnique $dispatchFuncs
    $routeSiteCsv = Join-SortedUnique $routeSites
    $firstFrame = ""
    if ($frames.Count -gt 0) { $firstFrame = $frames[0] }

    $actionNames = New-Object "System.Collections.Generic.List[string]"
    $wrapperRoutes = New-Object "System.Collections.Generic.List[string]"
    $byNameFlags = New-Object "System.Collections.Generic.List[string]"
    foreach ($a in ($actionNums | Sort-Object -Unique)) {
        if ($actionMetaByNum.ContainsKey($a)) {
            $m = $actionMetaByNum[$a]
            if ($m.action_name) { $actionNames.Add([string]$m.action_name) }
            if ($m.wrapper_route) { $wrapperRoutes.Add([string]$m.wrapper_route) }
        }
        if ($byNameBitByAction.ContainsKey($a)) {
            $byNameFlags.Add([string]$byNameBitByAction[$a])
        }
    }

    $wrapperCsv = Join-SortedUnique $wrapperRoutes
    $actionNameCsv = Join-SortedUnique $actionNames
    $byNameCsv = Join-SortedUnique $byNameFlags
    if (-not $byNameCsv) { $byNameCsv = "unknown" }

    $backend = "unknown"
    if ($midi -eq "1") {
        $backend = "MCI_sequencer_possible"
    } elseif ($wave -eq "1") {
        $backend = "DirectSound_expected"
    }

    $expectedPath = "undetermined"
    if ($playCount -gt 0 -and ($wrapperCsv -match "(^|,)cef0(,|$)")) {
        $expectedPath = "PATH1_MMFS2_PlaySample_by_id_to_Ord1077_then_DSOUND_PlayUnlock"
    } elseif ($playCount -gt 0) {
        $expectedPath = "PATH1_MMFS2_play_wrapper_observed_then_DSOUND_or_backend_specific"
    } elseif ($backend -eq "DirectSound_expected") {
        $expectedPath = "bank_only_no_action_row_in_matrix__likely_PATH1_DSOUND"
    } elseif ($backend -eq "MCI_sequencer_possible") {
        $expectedPath = "MCI_sequencer_possible"
    }
    if ($isStreamExpected -eq 1) {
        $expectedPath += " + PATH2_stream_expected_unlock_fallback"
    }

    $confidence = "low"
    if ($playCount -gt 0 -and $backend -eq "DirectSound_expected") {
        $confidence = "high"
    } elseif ($playCount -gt 0 -or $backend -ne "unknown") {
        $confidence = "medium"
    }

    $evidence = "sound_names.csv;mfa_sound_flags.tsv;._static_mfa_play_routes_numeric_20260223.tsv;_static_actionnum_dispatch_wrapper_route_matrix_20260219.tsv;_static_active_action_ord1077_byname_bit_profile_20260219.tsv;_static_all_handles_effective_subtitle_semantics_20260219.tsv"
    if ($babySet.Contains($h)) {
        $evidence += ";._static_mfa_babyfamily_route_points_20260223.tsv"
    }

    $rows.Add([pscustomobject]@{
        handle = $h
        name = [string]$sn.name
        bank_type = [string]$bankType
        flags_wave = [string]$wave
        flags_midi = [string]$midi
        stream_expected = $isStreamExpected
        dialogue_class = $dialogClass
        play_rows = $playCount
        play_action_nums = $actionNumCsv
        play_action_names = $actionNameCsv
        dispatch_target_funcs = $dispatchCsv
        route_call_sites = $routeSiteCsv
        wrapper_routes = $wrapperCsv
        ord1077_byname_bit_profile = $byNameCsv
        first_frame_group = $firstFrame
        expected_backend = $backend
        expected_runtime_path = $expectedPath
        static_confidence = $confidence
        evidence_sources = $evidence
    })
}

$outPath = Join-Path $Root $OutFile
$rows | Export-Csv -Path $outPath -Delimiter "`t" -NoTypeInformation -Encoding UTF8

$summary = [pscustomobject]@{
    out_file = $outPath
    rows = $rows.Count
    high_confidence = @($rows | Where-Object { $_.static_confidence -eq "high" }).Count
    stream_expected = @($rows | Where-Object { [int]$_.stream_expected -eq 1 }).Count
}
$summary | Format-List
