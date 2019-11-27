set terminal pdfcairo mono font "sans, 16"
set output 'data-AC-time-dm.pdf'
#set multiplot
#unset key
set key top left
set grid
set xlabel 'log_k(n) log(m)'
set ylabel 't (μs)'
set format '%g'
#set logscale xy
#set logscale x

# 1  #V
# 2  #E
# 3  start_time
# 4  loop_time
# 5  exclusive_save_time
# 6  time_per_add_op
# 7  time_per_rem_op
# 8  time_per_list_op
# 9  add_op_count
# 10 add_op_exclusive_time
# 11  rem_op_count
# 12 rem_op_exclusive_time
# 13 list_op_count
# 14 list_op_exclusive_time

set style data linespoints

#f(x) = a*x*x + b*x + c
#fit f(x) 'rtest_k.csv' using ($1/1000):(($12-$11+$20-$19+$28-$27+$36-$35+$44-$43+$52-$51+$60-$59+$68-$67)/8) every ::37::40 via a,b,c

set yrange [0:9]
set xrange [150:]
#set size 0.5,1.0
#set origin 0.0,0.0
set title  'CHECK (DM)'
plot \
    'data-dyn-inesc-AC-time-dm.csv' using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) pt 10 title 'sdk2tree', \
    'data-dyn-udc-AC-time-dm.csv'   using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) pt 4 title 'dk2treee', \
	'data-static-AC-time-dm.csv'  using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) dt '_' pt 6 title 'k2tree', \
    'data-dyntrie1-AC-time-dm.csv' using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) dt '_' pt 16 title 'dyntrie1', \
    'data-dyntrie2-AC-time-dm.csv' using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) dt '_' pt 12 title 'dyntrie2'

#plot 'data.csv' using ($2*log($1)/log(2)*sqrt(sqrt(log($2)))/0.25):($10) title 'add time'
#  f(x) title 'Naive'
#  'rtest_k.csv' using ($1/1000):(($11-$8+$13-$12 + $19-$16+$21-$20 + $27-$24+$29-$28 + $35-$32+$37-$36 + $43-$40+$45-$44 + $51-$48+$53-$52 + $59-$56+$61-$60 + $67-$64+$69-$68)/8) \
#    every ::37::40 title 'k=350', \

quit

