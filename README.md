# ETL project

This project has been designed as starting point to understand and learn  about ETL process.
ETL stands for Extract, Transform and Load, and the main idea of this project is to use open data available about the
New York cities taxis to create an ETL, so you will extract the data from "parquet" files, load the data in memory and
transform the read data into a new one.

## Data

Data dictionaries and datasets can be found in the NYC page: [NYC Portal](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Scenarios

Considered scenarios in the project:

### Case 1: Travel time according location

Using the columns:

* `tpep_pickup_datetime`
* `tpep_dropoff_datetime`
* `Trip_distance`
* `PULocationID`
* `DOLocationID`

### Case 2: Payment by distance

* Using the columns:
* `PULocationID`
* `DOLocationID`
* `Trip_distance`
* `Tolls_amount`
* `Total_amount`

### Case 3: Payment by time

* Using the columns:
* `tpep_pickup_datetime`
* `tpep_dropoff_datetime`
* `Passenger_count`
* `Payment_type`
* `Total_amount`

## Install

To be able to run this project you should have a properly configured environment. The instructions below contains the required
steps you should follow to be able to run the entire project.

### Automated install Linux, Mac, WSL

Install [sdkman](https://sdkman.io/) and activate the environment

```bash
curl -s "https://get.sdkman.io" | bash
# Inside the project folder install java, spark and hadoop (required once)
sdk env install
# Then you can simply enable the environment with
sdk env
```

### Manual install on windows

#### 1. Install Java

* Install the java development kit (JDK 1.8.0)

#### 2. Create environment variable for Java

* Go to the windows menu or to the control panel and search for "Environment variables" and select the "edit environment variables" option
* In the new window, select the "Environment Variables"
* Now you will see a new window with two main boxes. Use the upper one (User variables) to create the required variable
* Select the "New..." button and put the following info:
  * Variable name: `JAVA_HOME`
  * Variable value: Path to the JDK installation folder (default folder is `C:\Program Files\Java\jdk1.8.0_202`)
* Select the "Path" environment variable and push the "Edit..." button
* Add the path to the "bin" folder inside your JDK installation folder (E.g. `C:\Program Files\Java\jdk1.8.0_202\bin`)
* Select the "OK" button

#### 3. Install Spark

* Configure the application /TO COMPLETE/
* Create a new environment variable following the steps in the "Create environment variables for Java" section
  * Variable name: `SPARK_HOME`
  * Variable value: Path to the Spark installation folder (E.g. `C:\Spark\spark-3.2.3-bin-hadoop3.2`)
* Select the "Path" environment variable and push the "Edit..." button
* Add the path to the "bin" folder inside your Spark installation folder (E.g. `C:\Spark\spark-3.2.3-bin-hadoop3.2\bin`)
* Select the "OK" button

#### 4. Install Hadoop

Hadoop is used to process "parquet" files.

* Download the "`Hadoop.dll`" and "`winutils.exe`" files from the corresponding version to your spark installation from "https://github.com/kontext-tech/winutils"
* Create a new folder called "Hadoop" in C:/, and a new folder inside called "bin"
* Create a new environment variable following the steps in the "Create environment variables for Java" section
  * Variable name: `HADOOP_HOME`
  * Variable value: `C:\Hadoop`
* Select the "Path" environment variable and push the "Edit..." button
* Add the path to the "bin" folder inside your Hadoop folder (E.g. `C:\Hadoop\bin`)
* Select the "OK" button

### 5. IDE

You can use any IDE that supports Scala and Java languages.
We recommend to use IntelliJ Community Edition, which support Scala installing the Scala plugin.

* To install the Scala plugin push "ctr + alt + s", a new window will appear
* In the left panel Select "Plugins"
* Select the "Marketplace" tab that is in the upper middle
* Search for "Scala" in the search box and install it

## Running the project

Setup the following environment variables

TAXIS_ETL_FILES, TAXIS_ETL_DOWNLOAD_FOLDER

for example using bash

```bash
export TAXIS_ETL_FILES=yellow_tripdata_2020-01.parquet,yellow_tripdata_2021-01.parquet,yellow_tripdata_2022-01.parquet
export TAXIS_ETL_DOWNLOAD_FOLDER=downloads
export TAXIS_ETL_BASE_URL=https://d37ci6vzurychx.cloudfront.net/trip-data
export TAXIS_ETL_DOWNLOAD_FILES=true
```

Scala is used to run the compiled Java file that contains all the instructions. In this example we are going to run specific classes defined in the code

* Compile the project using Gradle.
  * If you are in IntelliJ, go to the right side on your window and select the Gradle tab
  * Double-click on "`clean`", then "`assemble`" to compile the project
* Open a new CMD window and execute this command: `spark-shell`. This will start Spark on your machine.
* A new task will be run using the clas definition in the code
  * Open a new CMD window and go to the generated .jar: `cd base_path\ETL\taxisEtl\build\libs`, where base_path should be replaced according to your project location
  * Run the following command to execute the class:
    * `spark-submit --class com.taxis.etl.travel_time.TravelTime --master local[8] taxisEtl.jar 100`
    * `spark-submit --class com.taxis.etl.payments_distance.PaymentsDistance --master local[8] taxisEtl.jar 100`
  * Wait until the execution finishes and verify the results are in the folder `cd base_path\ETL\taxisEtl\build\libs\results`

### By

* Santiago Ruiz
* Diego Peña
