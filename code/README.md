For the RDD scripts:

In order to calculate running time for our scripts, we wrote the results of the workers to a directory in the hdfs.

In order to print and save results to txt files, we used the ".collect()" method at the end,
printed the results and redirected them from the terminal to an output txt file.

For the SparkSQL scripts:

For calculating the running time, we used the same procedure as above.

For printing and saving the results, we also used the same procedure as above, just without the use of ".collect()",
which is used only for RDD API

