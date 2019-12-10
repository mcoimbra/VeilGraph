# set terminal pngcairo  transparent enhanced font "arial,10" fontscale 1.0 size 600, 400 
# set output 'candlesticks.3.png'
set boxwidth 0.2 absolute
set style fill   solid 1.00 border
set style increment default
set title "candlesticks with style fill solid" 
set xrange [ 0.00000 : 11.0000 ] noreverse nowriteback
set x2range [ * : * ] noreverse writeback
set yrange [ 0.00000 : 10.0000 ] noreverse nowriteback
set y2range [ * : * ] noreverse writeback
set zrange [ * : * ] noreverse writeback
set cbrange [ * : * ] noreverse writeback
set rrange [ * : * ] noreverse writeback
## Last datafile plotted: "amazon-2008-40000-random-start_30_data_stats.tsv"
plot 'candlesticks.dat' using 1:3:2:6:5 with candlesticks
