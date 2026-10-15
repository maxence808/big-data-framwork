# Lab 3 : Job et CronJob (réponses)

Exécuté sur minikube (Kubernetes v1.37.0), dans le namespace `lab-job-cronjob`.

## 1. Job simple

```
NAME              STATUS     COMPLETIONS   DURATION   AGE
data-import-job   Complete   1/1           6s         6s
Starting data import...
Processing 1000 records...
Import completed!
```

## 1. Job avec retry

```
NAME                       READY   STATUS   RESTARTS   AGE
data-transform-job-9sn7l   0/1     Error    0          64s
data-transform-job-lhkn8   0/1     Error    0          75s
data-transform-job-npln2   0/1     Error    0          3s
data-transform-job-qg27v   0/1     Error    0          44s

Pods Statuses:  0 Active (0 Ready) / 0 Succeeded / 4 Failed
Events:
  Normal   SuccessfulCreate      75s   job-controller  Created pod: data-transform-job-lhkn8
  Normal   SuccessfulCreate      64s   job-controller  Created pod: data-transform-job-9sn7l
  Normal   SuccessfulCreate      44s   job-controller  Created pod: data-transform-job-qg27v
  Normal   SuccessfulCreate      3s    job-controller  Created pod: data-transform-job-npln2
  Warning  BackoffLimitExceeded  0s    job-controller  Job has reached the specified backoff limit
```

**Combien de Pods ont été créés ? Pourquoi sont-ils conservés après l'échec ?**

Le Job a créé **4 Pods** : 1 tentative initiale et 3 retries (`backoffLimit: 3`). Avec `restartPolicy: Never`, chaque
retry crée un nouveau Pod au lieu de redémarrer le conteneur. L'écart entre deux créations augmente à chaque tentative
(~11 s, ~20 s, ~41 s) : c'est le back-off exponentiel (10 s, 20 s, 40 s… plafonné à 6 min).

Les Pods sont conservés pour qu'on puisse diagnostiquer l'échec : sans eux, on ne pourrait plus lire leurs logs
(`kubectl logs`) ni leur statut ou code de sortie. Ils ne sont supprimés que si l'on supprime le Job (suppression en
cascade) ou si `ttlSecondsAfterFinished` est défini.

**Quelle est la raison de l'échec du Job dans `describe` ?**

`BackoffLimitExceeded` : *« Job has reached the specified backoff limit »*. Les 4 tentatives ont toutes échoué
(`sys.exit(1)`), donc la limite de retries est atteinte.

**Avec `activeDeadlineSeconds: 10`, quelle est la nouvelle raison ?**

```
NAME                       READY   STATUS   RESTARTS   AGE
data-transform-job-p6lrq   0/1     Error    0          10s

Failed: DeadlineExceeded - Job was active longer than specified deadline
Events:
  Normal   SuccessfulCreate  10s   job-controller  Created pod: data-transform-job-p6lrq
  Warning  DeadlineExceeded  0s    job-controller  Job was active longer than specified deadline
```

`DeadlineExceeded` : *« Job was active longer than specified deadline »*. Le premier Pod échoue presque immédiatement,
puis le Job attend ~10 s de back-off avant le retry suivant. Les 10 s de deadline expirent avant ce retry, donc
**un seul Pod** est créé. `activeDeadlineSeconds` est prioritaire sur `backoffLimit` : quand la deadline est atteinte,
le Job est marqué en échec et ses Pods encore actifs sont arrêtés, quel que soit le nombre de retries restants.

## 2. Parallélisation de Job

```
NAME                       READY   STATUS    RESTARTS   AGE   JOB-COMPLETION-INDEX
batch-export-job-0-7l4hs   1/1     Running   0          3s    0
batch-export-job-1-bwcft   1/1     Running   0          3s    1
batch-export-job-2-6bpsr   1/1     Running   0          3s    2
batch-export-job-3-gnbbq   1/1     Running   0          3s    3

[pod/batch-export-job-0-7l4hs/export] Pod batch-export-job-0: Exporting shard 0 (users with id % 4 == 0)...
[pod/batch-export-job-0-7l4hs/export] Pod batch-export-job-0: Shard 0 export completed!
[pod/batch-export-job-1-bwcft/export] Pod batch-export-job-1: Exporting shard 1 (users with id % 4 == 1)...
[pod/batch-export-job-1-bwcft/export] Pod batch-export-job-1: Shard 1 export completed!
[pod/batch-export-job-2-6bpsr/export] Pod batch-export-job-2: Exporting shard 2 (users with id % 4 == 2)...
[pod/batch-export-job-2-6bpsr/export] Pod batch-export-job-2: Shard 2 export completed!
[pod/batch-export-job-3-gnbbq/export] Pod batch-export-job-3: Exporting shard 3 (users with id % 4 == 3)...
[pod/batch-export-job-3-gnbbq/export] Pod batch-export-job-3: Shard 3 export completed!
```

Les 4 Pods tournent en même temps (même `AGE`), et le Job se termine en ~11 s au lieu de ~4 × 5 s en séquentiel. Chaque
Pod reçoit un index distinct (0 à 3) via `JOB_COMPLETION_INDEX`, et donc un shard distinct. En mode `Indexed`, le
hostname du Pod vaut `<job>-<index>` (`batch-export-job-0`…), d'où la valeur de `HOSTNAME` dans les logs. Grâce à
`ttlSecondsAfterFinished: 600`, le Job et ses Pods sont supprimés automatiquement 10 minutes après leur fin.

## 3. CronJob

```
NAME                        SCHEDULE      TIMEZONE       SUSPEND   ACTIVE   LAST SCHEDULE   AGE
bronze-to-silver-pipeline   */2 * * * *   <none>         False     0        30s             3m8s
daily-backup                0 2 * * *     Europe/Paris   False     0        <none>          3m8s
etl-bronze-to-silver        */5 * * * *   <none>         False     0        90s             3m8s

NAME                                 STATUS     COMPLETIONS   DURATION   AGE
bronze-to-silver-pipeline-29867864   Complete   1/1           5s         2m31s
bronze-to-silver-pipeline-29867866   Complete   1/1           6s         31s
etl-bronze-to-silver-29867865        Complete   1/1           3s         91s
```

Logs de `bronze-to-silver-pipeline-29867866` :

```
[2026-10-15 13:46:00.915768] Bronze→Silver ETL started...
[2026-10-15 13:46:02.916016] Completed! 500 records processed
```

- Le CronJob crée un Job toutes les 2 minutes (13:44, 13:46 UTC). Le suffixe du nom du Job est l'heure planifiée en
  minutes depuis l'epoch Unix : 29867866 − 29867864 = 2 minutes.
- `successfulJobsHistoryLimit: 3` : au-delà de 3 Jobs réussis, les plus anciens sont supprimés.
- `concurrencyPolicy: Forbid` : une nouvelle exécution est sautée si la précédente tourne encore.
- `daily-backup` n'a pas encore tourné (`LAST SCHEDULE <none>`) : il est planifié à 2 h, heure de Paris
  (`timeZone: Europe/Paris`), au lieu de l'UTC du controller manager.

## 4. Monitoring

Suspendre puis reprendre le CronJob :

```
cronjob.batch/bronze-to-silver-pipeline patched
NAME                        SCHEDULE      TIMEZONE   SUSPEND   ACTIVE   LAST SCHEDULE   AGE
bronze-to-silver-pipeline   */2 * * * *   <none>     True      0        42s             3m20s
cronjob.batch/bronze-to-silver-pipeline patched
NAME                        SCHEDULE      TIMEZONE   SUSPEND   ACTIVE   LAST SCHEDULE   AGE
bronze-to-silver-pipeline   */2 * * * *   <none>     False     0        43s             3m21s
```

Quand il est suspendu (`SUSPEND True`), le CronJob ne crée plus de nouveaux Jobs, mais les Jobs déjà lancés continuent.

## 5. Scénario de pipeline de données

Exécution manuelle depuis le template du CronJob (`kubectl create job etl-manual-run --from=cronjob/etl-bronze-to-silver`) :

```
[2026-10-15T13:43:24.148478] ETL: bronze -> silver
[2026-10-15T13:43:24.148478] Batch size: 1000
[2026-10-15T13:43:24.148478] Reading, validating, deduplicating...
[2026-10-15T13:43:24.148478] Writing to silver layer...
[2026-10-15T13:43:24.148478] Completed!
```

Les variables `SOURCE_LAYER`, `TARGET_LAYER` et `BATCH_SIZE` viennent de la ConfigMap `etl-config`, injectée avec
`envFrom`. Pour changer les paramètres du pipeline, il suffit de modifier la ConfigMap : l'image reste la même. L'heure
affichée est en UTC (13:43 UTC = 15:43 à Paris), parce que le conteneur n'a pas de fuseau horaire configuré.
