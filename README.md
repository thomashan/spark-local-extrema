# spark-local-extrema
Algorithm to that find local extremas using spark

# Simple cartesian points
## Assumption
* all points are differentiable (e.g. it has not duplicate points in the same x axis)

## Scenarios
The easiest calculation is when the differentiation changes signs.
* +ve -> -ve
* -ve -> +ve

The other cases are
* +ve -> any number of 0's -> -ve (scenario 1)
* -ve -> any number of 0's -> +ve (scenario 2)
* +ve -> any number of 0's -> +ve (scenario 3)
* -ve -> any number of 0's -> -ve (scenario 4)

### Simple cases

## Data
Test data is available in csv format in
`src/test/resources/data/cartesian_points.csv`

| x   | y    |
|-----|------|
| 1.5 | 1    |
| 5.2 | -0.5 |
| 0   | 0    |
| 1   | 1    |
| 2.5 | 0    |
| 5.1 | -0.5 |
| 4   | 0.5  |
| 0.5 | 0.5  |
| 3   | 0    |
| 5.5 | -1   |
| 5   | -0.5 |
| 3.5 | 0    |
| 2   | 0.5  |


This data looks random and is not sorted.

The scatter plot for this data is in
`images/cartesian_points_scatter_plot.png`
![Cartesian points scatter plot](images/cartesian_points_scatter_plot.png)

The data has to be sorted to plot the line chart.
You can see the line chart in
`images/cartesian_points_line_chart.png`
![Cartesian points line chart](images/cartesian_points_line_chart.png)

You can see there are local extremas in the following points
* (1, 1), (1.5, 1) maxima
* (2.5, 0), (3, 0), (3.5, 0) minima
* (4, 0.5) maxima

** FIXME: what happened to the data?**



### Extrema calculation



# Time series quotes
## FIXME: put quotes in here 
