bin/ycsb.sh load kafka \
-P workloads/workload \
-p static_col.dt=20220101 \
-p static_col.ht=02 \
-p insertstart=0 \
-p insertcount=20




bin/ycsb.sh run kafka \
-P workloads/workload \
-p operationcount=2 \
-p recordcount=1 \
-p insertstart=0 \
-p insertcount=1 \
-p readproportion=0 \
-p updateproportion=0.5 \
-p scanproportion=0 \
-p insertproportion=0.5