<#
.SYNOPSIS
    Runs one or more builds of FASTER.benchmark.exe with multiple parameter permutations and generates corresponding directories of result files named for those permutations.

.DESCRIPTION
    This is intended to run performance-testing parameter permutations on one or more builds of FASTER.benchmark.exe, to be compared by compare_runs.ps1.
    The default execution of this script does a performance run on all FASTER.benchmark.exes identified in ExeDirs, and places their output into correspondingly-named
    result directories, to be evaluated with compare_runs.ps1.

    This script functions best if you have a dedicated performance-testing machine that is not your build machine. Use the following steps:
    1. Create a directory on the perf machine for your test
    2. Xcopy the baseline build's Release directory to your perf folder. This script will start at the netcoreapp3.1 directory to traverse to FASTER.benchmark.exe.
       Name this folder something that indicates its role, such as 'baseline'.
    3. Similarly, xcopy the new build's Release directory to your perf folder, naming it with some indication of what was changed, for example 'refactor_FASTERImpl".
    4. Copy this script and, if you will want to compare runs on the perf machine, compare_runs.ps1 to the perf folder.
    5. In a remote desktop on the perf machine, change to your folder, and run this file with those directory names. See .EXAMPLE for details.

.PARAMETER ExeDirs
    One or more directories from which to run FASTER.benchmark.exe builds. This is a Powershell array of strings; thus from the windows command line 
    the directory names should be joined by , (comma) with no spaces:
        pwsh -c ./run_benchmark.ps1 './baseline','./refactor_FASTERImpl'
    Single (or double) quotes are optional and may be omitted if the directory paths do not contain spaces. 

.PARAMETER RunSeconds
    Used primarily to debug changes to this script or do a quick one-off run; the default is 30 seconds.

.PARAMETER NumThreads
    Used primarily to debug changes to this script or do a quick one-off run; the default is multiple counts as defined in the script.

.PARAMETER UseRecover
    Used primarily to debug changes to this script or do a quick one-off run; the default is false.

.EXAMPLE
    ./run_benchmark.ps1 './baseline','./refactor_FASTERImpl'

    If run from your perf directory using the setup from .DESCRIPTION, this will create and populate the following folders:
        results_baseline
        results_refactor_FASTERImpl
    You can then run compare.ps1 on those two directories.

.EXAMPLE
    ./run_benchmark.ps1 './baseline','./refactor_FASTERImpl' -RunSeconds 3 -NumThreads 8 -UseRecover

    Does a quick run (e.g. test changes to this file).
#>
param (
  [Parameter(Mandatory=$true)] [String[]]$ExeDirs,
  [Parameter(Mandatory=$false)] [uint]$RunSeconds = 30,
  [Parameter(Mandatory=$false)] [uint]$NumThreads,
  [Parameter(Mandatory=$false)] [switch]$UseRecover
)

if (-not(Test-Path d:/data)) {
    throw "Cannot find d:/data"
}

$benchmarkExe = "netcoreapp3.1/win7-x64/FASTER.benchmark.exe"
$exeNames = [String[]]($ExeDirs | ForEach-Object{"$_/$benchmarkExe"})

Foreach ($exeName in $exeNames) {
    if (Test-Path "$exeName") {
        Write-Host "Found: $exeName"
        continue
    }
    throw "Cannot find: $exeName"
}

$resultDirs = [String[]]($ExeDirs | ForEach-Object{"./" + (Get-Item $_).Name})
Foreach ($resultDir in $resultDirs) {
    Write-Host $resultDir
    if (Test-Path $resultDir) {
        throw "$resultDir already exists (or possible duplication of leaf name in ExeDirs)"
    }
    New-Item "$resultDir" -ItemType Directory
}

$iterations = 1
$distributions = ("uniform", "zipf")
$readPercents = (0, 100)
$threadCounts = (1, 16, 32, 48, 64)
$indexModes = (0, 1, 2) #, 3)
$lockModes = (0, 1)
$smallDatas = (0) #, 1)
$smallMemories = (0) #, 1)
$syntheticDatas = (0) #, 1)
$k = ""

if ($NumThreads) {
    $threadCounts = ($NumThreads)
}
if ($UseRecover) {
    $k = "-k"
}

# Numa will always be either 0 or 1, so "Numas.Count" is 1
$permutations = $distributions.Count *
                $readPercents.Count *
                $threadCounts.Count *
                $indexModes.Count *
                $lockModes.Count *
                $smallDatas.Count *
                $smallMemories.Count *
                $syntheticDatas.Count

$permutation = 1
foreach ($d in $distributions) {
    foreach ($r in $readPercents) {
        foreach ($t in $threadCounts) {
            foreach ($x in $indexModes) {
                foreach ($z in $lockModes) {
                    foreach ($sd in $smallDatas) {
                        foreach ($sm in $smallMemories) {
                            foreach ($sy in $syntheticDatas) {
                                Write-Host
                                Write-Host "Permutation $permutation of $permutations"
                                ++$permutation

                                # Only certain combinations of Numa/Threads are supported
                                $n = ($t -lt 64) ? 0 : 1;
 
                                for($ii = 0; $ii -lt $exeNames.Count; ++$ii) {
                                    $exeName = $exeNames[$ii]
                                    $resultDir = $resultDirs[$ii]

                                    Write-Host
                                    Write-Host "Generating $($ii + 1) of $($exeNames.Count) results to $resultDir for: -n $n -d $d -r $r -t $t -x $x -z $z -i $iterations --runsec $RunSeconds $k"

                                    # RunSec and Recover are for one-off operations and are not recorded in the filenames.
                                    & "$exeName" -b 0 -n $n -d $d -r $r -t $t -x $x -z $z -i $iterations --runsec $RunSeconds $k | Tee-Object "$resultDir/results_n-$($n)_d-$($d)_r-$($r)_t-$($t)_x-$($x)_z-$($z).txt"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
