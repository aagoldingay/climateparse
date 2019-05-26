# climateparse

Go executable to mass import climate csv's to a Mongo database

## To use:

1. Install [MongoDB 4.0+](https://www.mongodb.com/what-is-mongodb), [Go 1.11.2+](https://golang.org/dl/), [Compass UI](https://www.mongodb.com/products/compass) (optional)
2. Clone [this](https://github.com/aagoldingay/climateparse) repository
3. Download dataset from [NCDC](https://www.ncdc.noaa.gov/orders/qclcd/)
4. Convert dataset files to CSV 

   WARNING: likely too large for Excel, you will probably need Microsoft Access - also did not work with pipe (“|”) delimited text files (stations data, but this should be small enough for Excel)

5. Edit the server details in main.go as required
6. Run the script
	* Build Go project > set as environment variable as ```climateparse filenameYYYYMM```
	* Alternatively, add the dataset folder to Go project folder then ```go run main.go csvprocessor.go filenameYYYYMM```
7.  Wait for data to import
8. Run aggregation pipelines
	* Load an aggregation file found [here](https://github.com/aagoldingay/climateparse/tree/master/pipelines) into Compass by clicking on the database
	* Select a collection
	* Choose the ‘Aggregations’ tab along the top 
	* Once, here, click the ‘. . .’ to reveal a dropdown which allows you to create a ‘New Pipeline From Text’. This will allow you to copy over the pipeline from the text files.
9. The values found in each step of the aggregation can be edited to change the outcome of each query.
	
	Alternatively, the query should be runnable directly in the command line when connecting to the mongo database by running the contents of the aggregation pipeline text file, and editing the values as preferred. It is advised to manually type these pipelines or at least remove line breaks before copy/pasting into a terminal.
