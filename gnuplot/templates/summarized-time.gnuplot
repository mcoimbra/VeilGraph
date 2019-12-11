#!/usr/bin/gnuplot

set terminal pdfcairo mono font "sans, 16"
set output 'XXXXX_5000_0.85_model_0.05_2_0.50_D-summarized-time.pdf'

set key top left
set grid
set xlabel '# Executions'
set ylabel 'Total running time (s)'
set format '%g'


set style data linespoints

set xrange [0:51]
set title 'XXXXX'
set key autotitle columnhead

plot \
    'XXXXX_ITERATIONS_RBO_P1_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_columns.tsv' using 1:($8/1000) pt 10 title 'P1', \
    'XXXXX_ITERATIONS_RBO_P2_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_columns.tsv' using 1:($8/1000) pt 4 title 'P2', \
    'XXXXX_ITERATIONS_RBO_P4_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_columns.tsv' using 1:($8/1000) pt 6 title 'P4', \
    'XXXXX_ITERATIONS_RBO_P8_DAMP_model_RPARAM_NPARAM_DELTAPARAMD_columns.tsv' using 1:($8/1000) pt 16 title 'P8', \
    'XXXXX_ITERATIONS_RBO_P16_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_columns.tsv' using 1:($8/1000) pt 12 title 'P16', \

quit

