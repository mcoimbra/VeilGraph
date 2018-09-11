dataset = "Facebook"
algorithm = "PR"

package = "test." + algorithm.lower() + "." + dataset.lower() + "."
exact_class = package + algorithm + dataset + "Exact"
approx_class = package + algorithm + dataset + "Approx"
local_dir = "/home/rrosa"
remote_dir = "/home/rrosa"
python_dir = remote_dir + "/Flink-Graph-Approx/python"
iterations = 20
base_threshold = 0.0
top_threshold = 0.2
step_threshold = 0.1
base_neigh = 0
top_neigh = 2
output_size = 1000

print("#!/bin/bash")
print('')
print("mkdir -p {0}/Eval/{1}".format(remote_dir, algorithm))
print("java -cp .:flink-graph-tester-0.3.jar:dependency/* {5} {0} {1} {2} {3} {4}-exact.csv"
      .format(local_dir, remote_dir, iterations, output_size, dataset, exact_class))
print('')

th = base_threshold
while th <= top_threshold:
    neigh = base_neigh
    while neigh <= top_neigh:
        th = round(th, 2)
        print("java -cp .:flink-graph-tester-0.3.jar:dependency/* {0} {1} {2} {3} {4:0.2f} {5} {6} {7}-{4:0.2f}-{5}.csv"
              .format(approx_class, local_dir, remote_dir, iterations, th, neigh, output_size, dataset))
        print(python_dir + "/batch-rank-evaluator.py {0}/Results/{4}/{1}-{2:0.2f}-{3} {0}/Results/{4}/{1}-exact 0.99 "
                           "> {0}/Eval/{4}/{1}-{2:0.2f}-{3}.csv".format(remote_dir, dataset, th, neigh, algorithm))
        print('')
        neigh += 1
    th += step_threshold

print("sed -i 's/\./,/g' /home/rrosa/Statistics/{0}/*.csv".format(dataset))
