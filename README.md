# spark-local-extrema
Algorithm to that find local extremas using spark

# Simple cartesian points
## Assumption
* all points are differentiable (e.g. it has not duplicate points in the same x axis)

## Data
Test data is available in csv format in
`src/test/resources/data/cartesian_points.csv`

This data looks random and is not sorted.

The scatter plot for this data is in
`images/cartesian_points_scatter_plot.png`
![Cartesian points scatter plot](images/cartesian_points_scatter_plot.png)

The data has to be sorted to plot the line chart.
You can see the line chart in
`images/cartesian_points_line_chart.png`
![Cartesian points line chart](images/cartesian_points_line_chart.png)

You can see there are local extremas in the following points
(1.3, 1.7) maxima
(2, 1.6) minima
(2.6, 3) maxima
(2.7, 2.5) minima
