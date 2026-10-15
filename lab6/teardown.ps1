# Lab 3 - Teardown : supprime tous les objets du lab.
$ns = 'lab-job-cronjob'
kubectl -n $ns delete job --all
kubectl -n $ns delete cronjob --all
kubectl delete namespace $ns
kubectl get namespace $ns   # Doit afficher "NotFound"
