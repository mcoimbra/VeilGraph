#!/usr/bin/gnuplot

set terminal pdfcairo mono font "sans, 16" color 
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-scalability-summarized-speedup-candle-plot.pdf'

set key top left
set grid

# Setting the boxwidth to desirable value.
# http://gnuplot.sourceforge.net/docs_4.2/node163.html
set boxwidth 
set style fill solid 1.00 border 

set title "Summarized version speedup (scalability)" 

set ylabel "Speedup"
set xlabel "#Workers"

set xrange [1:32]
set logscale x 2

plot 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data_summarized_stats.tsv' using 1:($10-$11):($10-$11):($10+$11):($10+$11) linecolor rgb '#3399ff' with candlesticks 

set terminal png crop
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-scalability-summarized-speedup-candle-plot.png'
replot


quit