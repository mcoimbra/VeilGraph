#!/bin/bash
# Reference - Graphs over Time: Densification Laws, Shrinking Diameters and Possible Explanations
# http://doi.acm.org/10.1145/1081870.1081893
# http://snap.stanford.edu/data/cit-HepPh.html

FILE_NAME="Cit-HepPh.txt"
DATASET_NAME="Cit-HepPh"

# set first K lines:
P30=126474
P40=168632
P50=210791
P60=252949
P70=295107
P80=337265

# line count (N): 
N=$(wc -l < $FILE_NAME)

# lengths of the bottom files:
L30=$(( $N - $P30 ))
L40=$(( $N - $P40 ))
L50=$(( $N - $P50 ))
L60=$(( $N - $P60 ))
L70=$(( $N - $P70 ))
L80=$(( $N - $P80 ))

# create the initial graph files: 
head -n $P30 $FILE_NAME > $DATASET_NAME-init-30.txt
head -n $P40 $FILE_NAME > $DATASET_NAME-init-40.txt
head -n $P50 $FILE_NAME > $DATASET_NAME-init-50.txt
head -n $P60 $FILE_NAME > $DATASET_NAME-init-60.txt
head -n $P70 $FILE_NAME > $DATASET_NAME-init-70.txt
head -n $P80 $FILE_NAME > $DATASET_NAME-init-80.txt

# create the update stream file: 
tail -n $L30 $FILE_NAME > $DATASET_NAME-cont-30.txt
tail -n $L40 $FILE_NAME > $DATASET_NAME-cont-40.txt
tail -n $L50 $FILE_NAME > $DATASET_NAME-cont-50.txt
tail -n $L60 $FILE_NAME > $DATASET_NAME-cont-60.txt
tail -n $L70 $FILE_NAME > $DATASET_NAME-cont-70.txt
tail -n $L80 $FILE_NAME > $DATASET_NAME-cont-80.txt