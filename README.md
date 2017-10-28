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

## Test dataset
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

ExtremaSetTaskSpec has tests that uses this dataset.

## Example dataset
An example dataset is available in
`examples/random.csv`
To read more about how we got this data please read [examples/README.md](examples/README.md)

You can run the extrema against this dataset by running
```bash
> sbt assembly
> docker run --rm -it -p 4040:4040 \
-v $(pwd)/examples:/data \
-v $(pwd)/target/scala-2.11/spark-local-extrema-assembly-0.1-SNAPSHOT.jar:/job.jar \
gettyimages/spark bin/spark-submit \
--master local[*] \
--driver-memory 2g \
--class com.github.thomashan.spark.cartesian.extrema.CompleteDatasetJob /job.jar \
/data/random.csv true x y /data/random_extremas
```

This will create directory named `examples/random_extremas` which contains the csv file of the extremas.

The plot of the first 200 points can be seen in 
`images/first_200_points.png`
![First 200 points](images/first_200_points.png)

# Complex dataset (high low time series)
In this section we find the local extremas for more complex dataset like candlestick.

## Rules
Simple rules like looking at the diff transitions from +ve -> -ve or -ve -> +ve does not apply to high low time series data.
The low point of the maxima has to be greater than all surrounding highs to be to considered maxima and conversely 
the highest point of the minima has to be less than all the surrounding lows to be considered minima. 

* a point is considered a maxima if the low point is greater than the surrounding highs
* a point is considered a minima if the high point is less than the surrounding lows

## Scenarios
The easiest calculation is when the differentiation changes signs.
* +ve -> -ve and low is greater than surrounding lows
* +ve -> -ve and low is greater than surrounding highs

# Time series quotes
## FIXME: put quotes in here 
