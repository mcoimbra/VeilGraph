#!/usr/bin/gnuplot

set terminal pdfcairo mono font "sans, 16" color 
set output 'XXXXX-complete-update-and-computation-time-bar-group-plot.pdf'

set key top left
set grid

 

set ylabel "Time (ms)"
set xlabel "#Workers"


graph_update_color = "#99ffff"; total_update_color = "#4671d5"; computation_time_color = "#ff0000";
set auto x
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set boxwidth 0.75

set title "Complete version isolated times"
plot 'XXXXX_ITERATIONS_data_time_stats.tsv' using 2:xtic(1) ti "I/O" fc rgb graph_update_color, \
 '' u 4 ti "Integration" fc rgb total_update_color, \
 '' u 6 ti "Computation" fc rgb computation_time_color;


set title "Summarized version isolated times" 
set output 'XXXXX-summarized-update-and-computation-time-bar-group-plot.pdf'
plot 'XXXXX_ITERATIONS_data_time_stats.tsv' using 8:xtic(1) ti "I/O" fc rgb graph_update_color, \
'' u 10 ti "Integration" fc rgb total_update_color, \
'' u 12 ti "Computation" fc rgb computation_time_color;


set output 'XXXXX-update-and-computation-overlapped-time-bar-group-plot.pdf'

set style histogram columnstacked clustered  gap 1;
#set style histogram clustered  gap 1;
set key autotitle columnheader
set key outside below center horizontal


num_of_ksptypes=3
set boxwidth 0.5/num_of_ksptypes
dx=0.5/num_of_ksptypes
offset=-0.82

#plot 'amazon-2008-40000-random-start_30_data_time_stats.tsv' using 12:xtic(1) with boxes  ti "Computation" fc rgb computation_time_color, '' u 10:xtic(1) with boxes ti "Integration" fc rgb total_update_color, '' u 8:xtic(1) with boxes ti "I/O" fc rgb graph_update_color 

# See: https://stackoverflow.com/questions/18332161/gnuplot-histogram-cluster-bar-chart-with-one-line-per-category
#plot newhistogram "1", 'amazon-2008-40000-random-start_30_data_time_stats.tsv' using 6:xtic(1) with boxes  ti "Computation" fc rgb computation_time_color, '' u 4:xtic(1) with boxes ti "Integration" fc rgb total_update_color, '' u 2:xtic(1) with boxes ti "I/O" fc rgb graph_update_color, newhistogram "1" at 200, 'amazon-2008-40000-random-start_30_data_time_stats.tsv'  using 12 with boxes ti "Computation" fc rgb graph_update_color, '' u 10 with boxes ti "Integration" fc rgb total_update_color, '' u 8 with boxes  ti "I/O" fc rgb computation_time_color 

plot newhistogram "1", 'XXXXX_ITERATIONS_data_time_stats.tsv' using 6:xtic(1) with boxes ti "Computation" fc rgb computation_time_color, \
'' u 4:xtic(1) with boxes ti "Integration" fc rgb total_update_color, \
'' u 2:xtic(1) with boxes ti "I/O" fc rgb graph_update_color, \
newhistogram "2", 'XXXXX_ITERATIONS_data_time_stats.tsv'  using 12:xtic(1) ti "Computation" fc rgb graph_update_color, \
'' u 10 ti "Integration" fc rgb total_update_color, \
'' u 8 ti "I/O" fc rgb computation_time_color;

#ORIGINAL:
#plot 'amazon-2008-40000-random-start_30_data_time_stats.tsv' using 12:xtic(1) with boxes ti "Computation" fc rgb computation_time_color, \
'' u 10:xtic(1) with boxes ti "Integration" fc rgb total_update_color, \
'' u 8:xtic(1) with boxes ti "I/O" fc rgb graph_update_color ;



#set output 'amazon-2008-update-and-computation-overlapped-time-bar-group-plot.pdf'


#plot 'amazon-2008-40000-random-start_30_data_time_stats.tsv' using 12:xtic(1) with boxes  ti "Computation" fc rgb computation_time_color, '' u 10:xtic(1) with boxes ti "Integration" fc rgb total_update_color, '' u 8:xtic(1) with boxes ti "I/O" fc rgb graph_update_color 