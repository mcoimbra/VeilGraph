# GraphBolt - Streaming Graph Approximations on Big Data.

## ABOUT

GraphBolt presents a novel execution model for graph processing engines that enables approximate computations on general graph applications.
Its model features an abstraction that flexibly allow the expression of custom vertex impact estimators.
With this abstraction, we build a representative graph summarization that solely comprises the subset of vertexes estimated as yielding high impact.
This way, GraphBolt is capable of delivering lower latencies in a resource-efficient manner, while maintaining query result accuracy within acceptable limits.

## REPRODUCING OUR EXPERIMENTS

The graph datasets we used in our experiments were mostly obtained from the Laboratory for Web Algorithmics.
As they come in the compressed format of WebGraph, we used it to convert the graphs into simple tab-separated vertices.
Note: the uncompressed versions of the graphs consume considerably more space.

We assume GraphBolt-master is located on the same directory as WebGraph-master on the user $HOME.

# Prepare GraphBolt.
cd $HOME/GraphBolt-master
mvn clean install -D=skipTests
mkdir -p testing/Temp
mkdir cache

# Obtain WebGraph.
cd $HOME
wget https://github.com/lhelwerd/WebGraph/archive/master.zip
unzip master.zip
cd WebGraph-master
mvn clean install

Below are sequences of commands to convert the graph files to .tsv files and to execute GraphBolt.
With these commands, for each dataset, the last 20000 edges are removed and shuffled into a "-stream.tsv" file.
The stream is divided into 50 chunks (400 edge additions per chunk)
A deletions stream is also generated, consisting of 50 edge removal chunks.
Each chunk with edge removals has the size of 20% of the "-stream.tsv" file chunk: 0.2 * 400 = 80 edge removals.
An edge removal chunk only removes edges that already existed in the original graph or were added in a previous stream chunk.

For 1 <= i <= 50:
	A_i is the ith edge addition chunk
	D_i is the ith edge removal chunk
	D_i only contains edge removals pertaining the original graph or any edge in A_j with j < i.

The values provided for parameters 'r', 'n' and '\Delta' in these example calls are based on two profiles:

- Accuracy-oriented: 0.05 2 0.50
- Performance-oriented: 0.20 1 1.0

This is configured to run with a parallelism of 2 in Apache Flink (either a single TaskManager with two parallel pipelines, or two TaskManager instances with a single pipeline each).

# cnr-2000: http://law.di.unimi.it/webdata/cnr-2000/

cd $HOME
mkdir cnr-2000-original
cd cnr-2000-original
wget http://data.law.di.unimi.it/webdata/cnr-2000/cnr-2000.graph
wget http://data.law.di.unimi.it/webdata/cnr-2000/cnr-2000.properties
cd ../WebGraph-master

mvn exec:java -Dexec.mainClass="it.unimi.dsi.webgraph.ArcListASCIIGraph" -Dexec.args="-g BVGraph '/home/$USER/cnr-2000-original/cnr-2000' '/home/$USER/cnr-2000-original/cnr-2000.tsv'"

cd ../GraphBolt-master/python
python -m graphbolt.dataset.sample_edges -i "~/cnr-2000-original/cnr-2000.tsv" -q 50 -c 20000 -deletion-ratio 0.2 -r

python -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i cnr-2000-20000-random -chunks 50 -out-dir "~/GraphBolt-master/testing" -data-dir "~" -cache "~/GraphBolt-master/cache" -p 2 -size 5000 -periodic-full-dump -temp "~/GraphBolt-master/testing/Temp" -flink-address 127.0.0.1 -flink-port 8081 -l 0.05 2 0.50 0.20 1 1.0

python -m graphbolt.figure.randomwalk.pagerank.make_figures -png -dataset-name cnr-2000-20000-random -iterations 30 -chunks 50 -size 5000 -out-dir "~/GraphBolt-master/testing" -data-dir "~/" -skip-single-figures -l 0.05 2 0.50 0.20 1 1.0


# eu-2005: http://law.di.unimi.it/webdata/eu-2005/

cd $HOME
mkdir eu-2005-original
cd eu-2005-original
wget http://data.law.di.unimi.it/webdata/eu-2005/eu-2005.graph
wget http://data.law.di.unimi.it/webdata/eu-2005/eu-2005.properties
cd ../WebGraph-master

mvn exec:java -Dexec.mainClass="it.unimi.dsi.webgraph.ArcListASCIIGraph" -Dexec.args="-g BVGraph '/home/$USER/eu-2005-original/eu-2005' '/home/$USER/eu-2005-original/eu-2005.tsv'"

cd ../GraphBolt-master/python
python -m graphbolt.dataset.sample_edges -i "~/eu-2005-original/eu-2005.tsv" -q 50 -c 20000 -deletion-ratio 0.2 -r

python -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i eu-2005-20000-random -chunks 50 -out-dir "~/GraphBolt-master/testing" -data-dir "~" -cache "~/GraphBolt-master/cache" -p 2 -size 5000 -periodic-full-dump -temp "~/GraphBolt-master/testing/Temp" -flink-address 127.0.0.1 -flink-port 8081 -l 0.05 2 0.50 0.20 1 1.0

python -m graphbolt.figure.randomwalk.pagerank.make_figures -png -dataset-name eu-2005-20000-random -iterations 30 -chunks 50 -size 5000 -out-dir "~/GraphBolt-master/testing" -data-dir "~/" -skip-single-figures -l 0.05 2 0.50 0.20 1 1.0

# dblp-2010: http://law.di.unimi.it/webdata/dblp-2010/

cd $HOME
mkdir dblp-2010-original
cd dblp-2010-original
wget http://data.law.di.unimi.it/webdata/dblp-2010/dblp-2010.graph
wget http://data.law.di.unimi.it/webdata/dblp-2010/dblp-2010.properties
cd ../WebGraph-master

mvn exec:java -Dexec.mainClass="it.unimi.dsi.webgraph.ArcListASCIIGraph" -Dexec.args="-g BVGraph '/home/$USER/dblp-2010-original/dblp-2010' '/home/$USER/dblp-2010-original/dblp-2010.tsv'"

cd ../GraphBolt-master/python
python -m graphbolt.dataset.sample_edges -i "~/dblp-2010-original/dblp-2010.tsv" -q 50 -c 20000 -deletion-ratio 0.2 -r

python -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i dblp-2010-20000-random -chunks 50 -out-dir "~/GraphBolt-master/testing" -data-dir "~" -cache "~/GraphBolt-master/cache" -p 2 -size 5000 -periodic-full-dump -temp "~/GraphBolt-master/testing/Temp" -flink-address 127.0.0.1 -flink-port 8081 -l 0.05 2 0.50 0.20 1 1.0

python -m graphbolt.figure.randomwalk.pagerank.make_figures -png -dataset-name dblp-2010-20000-random -iterations 30 -chunks 50 -size 5000 -out-dir "~/GraphBolt-master/testing" -data-dir "~/" -skip-single-figures -l 0.05 2 0.50 0.20 1 1.0


# amazon-2008: http://law.di.unimi.it/webdata/amazon-2008/

cd $HOME
mkdir amazon-2008-original
cd amazon-2008-original
wget http://data.law.di.unimi.it/webdata/amazon-2008/amazon-2008.graph
wget http://data.law.di.unimi.it/webdata/amazon-2008/amazon-2008.properties
cd ../WebGraph-master

mvn exec:java -Dexec.mainClass="it.unimi.dsi.webgraph.ArcListASCIIGraph" -Dexec.args="-g BVGraph '/home/$USER/amazon-2008-original/amazon-2008' '/home/$USER/amazon-2008-original/amazon-2008.tsv'"

cd ../GraphBolt-master/python
python -m graphbolt.dataset.sample_edges -i "~/amazon-2008-original/amazon-2008.tsv" -q 50 -c 20000 -deletion-ratio 0.2 -r

python -m graphbolt.algorithm.randomwalk.pagerank.run -delete-edges -i amazon-2008-20000-random -chunks 50 -out-dir "~/GraphBolt-master/testing" -data-dir "~" -cache "~/GraphBolt-master/cache" -p 2 -size 5000 -periodic-full-dump -temp "~/GraphBolt-master/testing/Temp" -flink-address 127.0.0.1 -flink-port 8081 -l 0.05 2 0.50 0.20 1 1.0

python -m graphbolt.figure.randomwalk.pagerank.make_figures -png -dataset-name amazon-2008-20000-random -iterations 30 -chunks 50 -size 5000 -out-dir "~/GraphBolt-master/testing" -data-dir "~/" -skip-single-figures -l 0.05 2 0.50 0.20 1 1.0


##### On the code:



GraphStreamHandler.java: the module encapsulating GraphBolt's engine logic.

PageRankStreamHandler.java: a class extending GraphStreamHandler, implementing the appropriate UDFs as mentioned in the article.

PageRankMain.java: the entry point for the PageRank algorithm as a use-case of GraphBolt.
