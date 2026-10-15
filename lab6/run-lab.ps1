# Lab 3 - Job and CronJob : execute toutes les sections du lab dans l'ordre.
# Usage : depuis le dossier lab-3 ->  .\run-lab.ps1
# Prerequis : minikube demarre (minikube start) et image pullee (minikube image pull python:3.13-slim)

$ErrorActionPreference = 'Continue'
Set-Location $PSScriptRoot
$ns = 'lab-job-cronjob'

function Step($title) { Write-Host "`n===== $title =====" -ForegroundColor Cyan }

Step 'Setup : namespace'
kubectl apply -f namespace.yaml

Step '1. Job simple'
kubectl apply -f job-simple.yaml
kubectl -n $ns wait --for=condition=complete job/data-import-job --timeout=60s
kubectl -n $ns get job
kubectl -n $ns logs -l job-name=data-import-job

Step '1. Job avec retry (backoffLimit: 3) - environ 1 minute'
kubectl -n $ns delete job data-transform-job --ignore-not-found
kubectl apply -f job-with-retry.yaml
kubectl -n $ns wait --for=condition=failed job/data-transform-job --timeout=180s
kubectl -n $ns get pods -l job-name=data-transform-job
kubectl -n $ns describe job data-transform-job

Step '1. Job avec retry (activeDeadlineSeconds: 10)'
kubectl -n $ns delete job data-transform-job
kubectl apply -f job-with-retry-deadline.yaml
kubectl -n $ns wait --for=condition=failed job/data-transform-job --timeout=60s
kubectl -n $ns get pods -l job-name=data-transform-job
kubectl -n $ns describe job data-transform-job

Step '2. Job parallele indexe (4 shards)'
kubectl -n $ns delete job batch-export-job --ignore-not-found
kubectl apply -f job-parallel.yaml
Start-Sleep 3
kubectl -n $ns get pods -l job-name=batch-export-job -L batch.kubernetes.io/job-completion-index
kubectl -n $ns wait --for=condition=complete job/batch-export-job --timeout=90s
kubectl -n $ns logs -l job-name=batch-export-job --prefix

Step '3. CronJobs (pipeline toutes les 2 min + backup quotidien)'
kubectl apply -f cronjob-pipeline.yaml -f cronjob-backup.yaml
kubectl -n $ns get cronjob

Step '5. Scenario ETL : ConfigMap + CronJob + execution manuelle'
kubectl apply -f configmap-etl.yaml -f cronjob-etl.yaml
kubectl -n $ns delete job etl-manual-run --ignore-not-found
kubectl -n $ns create job etl-manual-run --from=cronjob/etl-bronze-to-silver
kubectl -n $ns wait --for=condition=complete job/etl-manual-run --timeout=60s
kubectl -n $ns logs job/etl-manual-run

Step '3/5. Attente des executions planifiees (~5 min)'
$deadline = (Get-Date).AddMinutes(7)
do {
    Start-Sleep 20
    $jobs = kubectl -n $ns get jobs --no-headers
    $pipeline = ($jobs | Select-String 'bronze-to-silver-pipeline-').Count
    $etl = ($jobs | Select-String 'etl-bronze-to-silver-').Count
    Write-Host "  $(Get-Date -Format HH:mm:ss) pipeline=$pipeline etl=$etl"
} until (($pipeline -ge 2 -and $etl -ge 1) -or (Get-Date) -gt $deadline)
Start-Sleep 10
kubectl -n $ns get cronjob
kubectl -n $ns get jobs
$last = kubectl -n $ns get jobs -o name | Select-String 'bronze-to-silver-pipeline-' | Select-Object -Last 1
if ($last) { kubectl -n $ns logs $last.ToString() }

Step '4. Monitoring : suspend / resume du CronJob'
kubectl -n $ns patch cronjob bronze-to-silver-pipeline --type=merge -p '{\"spec\":{\"suspend\":true}}'
kubectl -n $ns get cronjob bronze-to-silver-pipeline
kubectl -n $ns patch cronjob bronze-to-silver-pipeline --type=merge -p '{\"spec\":{\"suspend\":false}}'
kubectl -n $ns get cronjob bronze-to-silver-pipeline

Write-Host "`nLab termine. Pour tout nettoyer : .\teardown.ps1" -ForegroundColor Green
