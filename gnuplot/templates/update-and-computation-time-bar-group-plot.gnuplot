#!/usr/bin/gnuplot

set terminal pdfcairo mono font "sans, 16" color 
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-complete-update-and-computation-time-bar-group-plot.pdf'

set key top right
set grid

 

set ylabel "Time (ms)"  font ",10" offset 4,0,0
set xlabel "#Workers"  font ",10" offset 0,0,0


graph_update_color = "#99ffff"; total_update_color = "#4671d5"; computation_time_color = "#ff0000";
set auto x
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set xtics  norangelimit  font ",8"
set ytics  norangelimit autofreq  font ",8"
set key font ",8"
set boxwidth 0.75

# Isolated graph update, total update and processing times for the complete version.
set title "Complete version isolated times"
plot 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data_time_stats.tsv' using 2:xtic(1) ti "I/O" fc rgb graph_update_color, \
 '' u 4 ti "Integration" fc rgb total_update_color, \
 '' u 6 ti "Computation" fc rgb computation_time_color;


set terminal png crop
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-complete-update-and-computation-time-bar-group-plot.png'
replot
set terminal pdfcairo mono font "sans, 16" color 

# Isolated graph update, total update and processing times for the summarized version.
set title "Summarized version isolated times" 
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-summarized-update-and-computation-time-bar-group-plot.pdf'
plot 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data_time_stats.tsv' using 8:xtic(1) ti "I/O" fc rgb graph_update_color, \
'' u 10 ti "Integration" fc rgb total_update_color, \
'' u 12 ti "Computation" fc rgb computation_time_color;

set terminal png crop
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-summarized-update-and-computation-time-bar-group-plot.png'
replot
set terminal pdfcairo mono font "sans, 16" color 


# Isolated graph update, total update and processing times for both versions.
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-update-and-computation-overlapped-time-bar-group-plot.pdf'
set border 3 front lt black linewidth 1.000 dashtype solid
set boxwidth 0.8 absolute
#set style fill   solid 1.00 noborder
set style fill   pattern 1 

set grid nopolar
set grid noxtics nomxtics ytics nomytics noztics nomztics nortics nomrtics \
 nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics
set grid layerdefault   lt 0 linecolor 0 linewidth 0.500,  lt 0 linecolor 0 linewidth 0.500
#set key bmargin center horizontal Left reverse noenhanced autotitle columnhead nobox
set style increment default
set style histogram rowstacked title textcolor lt -1 offset character 2, 0.25
set datafile missing '-'
set style data histograms
set xtics border in scale 0,0 nomirror rotate by -45  autojustify
set xtics  norangelimit  font ",8"
set xtics   ()
set ytics border in scale 0,0 mirror norotate  autojustify
set ytics  norangelimit autofreq  font ",8"
set ztics border in scale 0,0 nomirror norotate  autojustify
set cbtics border in scale 0,0 mirror norotate  autojustify
set rtics axis in scale 0,0 nomirror norotate  autojustify
set title "All isolated times" 
#set xlabel  offset character 0, -2, 0 font "" textcolor lt -1 norotate
set xrange [ * : * ] noreverse writeback
set x2range [ * : * ] noreverse writeback
set yrange [ 0.00000 : 150000. ] noreverse writeback
set y2range [ * : * ] noreverse writeback
set zrange [ * : * ] noreverse writeback
set cbrange [ * : * ] noreverse writeback
set rrange [ * : * ] noreverse writeback

# https://stackoverflow.com/questions/41049785/gnuplot-custom-legend-with-two-different-specs



plot newhistogram "Complete" font ",12" lt 2 fs pattern 1 , 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D_data_time_stats.tsv' using "complete_graph_update_time_avg":xtic(1) title "I/O", \
'' u (column("complete_total_update_time_avg")-column("complete_graph_update_time_avg")) title "Integration", \
'' u (column("complete_computation_time_avg")-column("complete_total_update_time_avg")) title "Computing", \
newhistogram "Summarized" font ",12" lt 2 fs pattern 1, \
'' u "summarized_graph_update_time_avg":xtic(1) title "", \
'' u (column("summarized_total_update_time_avg")-column("summarized_graph_update_time_avg")) title "", \
'' u (column("summarized_computation_time_avg")-column("summarized_total_update_time_avg")) title ""


set terminal png crop
set output 'XXXXX_ITERATIONS_RBO_DAMP_model_RPARAM_NPARAM_DELTAPARAM_D-update-and-computation-overlapped-time-bar-group-plot.png'
replot

quit