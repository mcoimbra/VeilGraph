#!/usr/bin/gnuplot

set terminal pdfcairo mono font "sans, 16"
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-scalability-summarized-executions-vs-speedup.pdf'
set key top left
set grid
set xlabel 'execution'
set ylabel 'Scalability: summarized speedups'
set format '%g'



set style data linespoints

set xrange [0:51]
set title 'XXXXX'
set key autotitle columnhead

plot \
    'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data.tsv' using 1:($8/$16) pt 4 title 'P2', \
    'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data.tsv' using 1:($8/$24) pt 6 title 'P4', \
    'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data.tsv' using 1:($8/$32) pt 16 title 'P8', \
    'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data.tsv' using 1:($4/$40) pt 12 title 'P16'

set terminal png crop
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-scalability-summarized-executions-vs-speedup.png'
replot


quit

