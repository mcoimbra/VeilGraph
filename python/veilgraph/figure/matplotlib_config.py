import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib import rc
from matplotlib import rcParams
from matplotlib import style

# https://stackoverflow.com/questions/15814635/prettier-default-plot-colors-in-matplotlib
# http://seaborn.pydata.org/tutorial/color_palettes.html
#import seaborn as sns

# http://tonysyu.github.io/mpltools/auto_examples/style/plot_ggplot.html
#style.use('ggplot')

###########################################################################
########################### MATPLOTLIB CONFIGS ############################
###########################################################################

# http://sbillaudelle.de/2015/02/20/matplotlib-with-style.html
# http://sbillaudelle.de/2015/02/23/seamlessly-embedding-matplotlib-output-into-latex.html

# Activate latex text rendering
plt.rc('text', usetex=True)

# Controlling font size from matplotlib: https://stackoverflow.com/questions/3899980/how-to-change-the-font-size-on-a-matplotlib-plot
SMALL_SIZE = 8
MEDIUM_SIZE = 15
BIGGER_SIZE = 16

TICK_SIZE = 18

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=BIGGER_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=TICK_SIZE)   # fontsize of the tick labels
plt.rc('ytick', labelsize=TICK_SIZE)   # fontsize of the tick labels
#plt.rc('legend', fontsize=MEDIUM_SIZE)   # legend fontsize

plt.rc('legend', fontsize=18)   # legend fontsize

plt.rc('legend', loc="best")
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

# Disable automatic layout resize. This way the plot rectangle will always be the same size.
plt.rcParams['figure.autolayout'] = False
plt.rcParams['legend.loc'] = "best"

plt.rcParams['lines.markersize'] = 7
plt.rcParams['lines.linestyle'] = "--"
plt.rcParams['lines.linewidth'] = 0.65
plt.rcParams['lines.marker'] = "^"
plt.rcParams['lines.color'] = "black"

plt.rcParams['figure.figsize'] = (8,5)

PLOT_ALPHA = 0.45

# List of matplotlib markers: https://matplotlib.org/api/markers_api.html
#colors = ('b', 'g', 'r', 'c', 'm', 'y', 'k')

# Convert the cycler of rcParams to a list of colors strings.
# https://matplotlib.org/examples/color/color_cycle_demo.html
# https://matplotlib.org/gallery/color/color_cycler.html
colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

# Set plot styles for group plots.
styles = ["o","+","*","x","D", "<"]

# Set linestyles.
linestyles = ['_', '-', '--', ':']
markers = []
# List of matplotlib markers: https://matplotlib.org/api/markers_api.html
for m in Line2D.markers:
    try:
        if len(m) == 1 and m != ' ':
            markers.append(m)
    except TypeError:
        pass