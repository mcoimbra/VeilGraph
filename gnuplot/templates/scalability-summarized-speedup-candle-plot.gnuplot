# set terminal pngcairo  transparent enhanced font "arial,10" fontscale 1.0 size 600, 400 
# set output 'candlesticks.3.png'
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
#set yrange [1:6]
set logscale x 2

plot 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data_summarized_stats.tsv' using 1:($6-$7):($6-$7):($6+$7):($6+$7) linecolor rgb '#3399ff' with candlesticks 
