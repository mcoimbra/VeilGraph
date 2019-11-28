set terminal pdfcairo mono font "sans, 16"
set output 'amazon-2008-40000-summarized-time.pdf'
#set multiplot
#unset key
set key top left
set grid
set xlabel 'execution'
set ylabel 'Total running time (s)'
set format '%g'
#set logscale xy
#set logscale x


set style data linespoints
#set style data points

#f(x) = a*x*x + b*x + c
#fit f(x) 'rtest_k.csv' using ($1/1000):(($12-$11+$20-$19+$28-$27+$36-$35+$44-$43+$52-$51+$60-$59+$68-$67)/8) every ::37::40 via a,b,c

#set yrange [0:300000]
set xrange [0:51]
#set size 0.5,1.0
#set origin 0.0,0.0
set title  'amazon-2008-40000-random'
set key autotitle columnhead

plot \
    'amazon-2008-40000-random-start_30_5000_P1_0.85_model_0.05_2_0.50_D_columns.tsv' using 1:($8/1000) pt 10 title 'P1', \
    'amazon-2008-40000-random-start_30_5000_P2_0.85_model_0.05_2_0.50_D_columns.tsv' using 1:($8/1000) pt 4 title 'P2', \
    'amazon-2008-40000-random-start_30_5000_P4_0.85_model_0.05_2_0.50_D_columns.tsv' using 1:($8/1000) pt 6 title 'P4', \
    'amazon-2008-40000-random-start_30_5000_P8_0.85_model_0.05_2_0.50_D_columns.tsv' using 1:($8/1000) pt 16 title 'P8', \
    'amazon-2008-40000-random-start_30_5000_P16_0.85_model_0.05_2_0.50_D_columns.tsv' using 1:($8/1000) pt 12 title 'P16', \


  #  'data-dyn-inesc-AC-time-dm.csv' using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) pt 10 title 'P2', \
  #  'data-dyn-udc-AC-time-dm.csv'   using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) pt 4 title 'P4', \
#	'data-static-AC-time-dm.csv'  using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) dt '_' pt 6 title 'P8', \
 #   'data-dyntrie1-AC-time-dm.csv' using (log($1)/log(2)*log($2)):(($3/$2)*1000*1000) dt '_' pt 16 title 'P16'

#plot 'data.csv' using ($2*log($1)/log(2)*sqrt(sqrt(log($2)))/0.25):($10) title 'add time'
#  f(x) title 'Naive'
#  'rtest_k.csv' using ($1/1000):(($11-$8+$13-$12 + $19-$16+$21-$20 + $27-$24+$29-$28 + $35-$32+$37-$36 + $43-$40+$45-$44 + $51-$48+$53-$52 + $59-$56+$61-$60 + $67-$64+$69-$68)/8) \
#    every ::37::40 title 'k=350', \

quit

