# SETL examples

Lessons and exercises to get familiar with the wonderful [SETL](https://github.com/SETL-Developers/setl) framework!

# Chapters

## 1. Entry Point and configurations

<details> <summary><strong>Lesson</strong></summary>

<h3>1.1. Entry point with basic configurations</h3>

<details> <summary></summary>

The entry point is the first thing you need to learn to code with SETL. It is the starting point to run your ETL project.

```
val setl0: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()
```

This is the minimum code needed to create a `Setl` object. It is the entry point of every SETL app. This will create a SparkSession, which is the entry point of any Spark job. Additionally, the `withDefaultConfigLoader()` method is used. This means that `Setl` will read the default ConfigLoader located in `resources/application.conf`, where `setl.environment` must be set. The ConfigLoader will then read the corresponding configuration file `<app_env>.conf` in the `resources` folder, where `<app_env>` is the value set for `setl.environment`.

> `resources/application.conf`:
> ```
> setl.environment = <app.env>
> ```

> `<app.env>.conf`:
> ```
> setl.config.spark {
>    some.config.option = "some-value"
>  }
> ```

The configuration file is where you can specify your `SparkSession` options, like when you create one in a basic `Spark` process. You must specify your `SparkSession` options under `setl.config.spark`.

</details>

<h3>1.2. Entry point with specific configurations</h3>

<details> <summary></summary>

You can specify the configuration file that the default `ConfigLoader` should read. In the code below, instead of reading `<app_env>.conf` where `<app_env>` is defined in `application.conf`, it will read `own_config_file.conf`.
> ```
> val setl1: Setl = Setl.builder()
>     .withDefaultConfigLoader("own_config_file.conf")
>     .getOrCreate()
> ```
> 
> `resources/own_config_file.conf`:
> ```
> setl.config.spark {
>    some.config.option = "some-other-value"
>  }
> ```

You can also set your own `ConfigLoader`. In the code below, `Setl` will load `local.conf` from the `setAppEnv()` method. If no `<app_env>` is set, it will fetch the environment from the default `ConfigLoader`, located in `resources/application.conf`.
> ```
> val configLoader: ConfigLoader = ConfigLoader.builder()
>     .setAppEnv("local")
>     .setAppName("Setl2_AppName")
>     .setProperty("setl.config.spark.master", "local[*]")
>     .setProperty("setl.config.spark.custom-key", "custom-value")
>     .getOrCreate()
> val setl2: Setl = Setl.builder()
>     .setConfigLoader(configLoader)
>     .getOrCreate()
> ```
 
You can also set your own `SparkSession` which will be used by `Setl`, with the `setSparkSession()` method. Please refer to the documentation or the source code of [SETL](https://github.com/SETL-Developers/setl).

</details>

<h3>1.3 Utilities</h3>

<details> <summary></summary>

<h5>Helper methods</h5>

<details> <summary></summary>

There are some quick methods that can be used to set your `SparkSession` configurations.
> ```
> val setl3: Setl = Setl.builder()
>     .withDefaultConfigLoader()
>     .setSparkMaster("local[*]") // set your master URL
>     .setShufflePartitions(200) // spark setShufflePartitions
>     .getOrCreate()
> ```
 
* `setSparkMaster()` method set the `spark.master` property of the `SparkSession` in your `Setl` entry point
* `setShufflePartitions()` method set the `spark.sql.shuffle.partitions` property of the `SparkSession` in your `Setl` entry point

</details>

<h5>SparkSession options</h5>

<details> <summary></summary>

As mentioned earlier, the options you want to define in your `SparkSession` must be specified under `setl.config.spark` in your configuration file. However, you can change this path by using the `setlSetlConfigPath()` method:
> ```
> val setl4: Setl = Setl.builder()
>     .withDefaultConfigLoader("own_config_file.conf")
>     .setSetlConfigPath("myApp")
>     .getOrCreate()
> ```
> 
> `resources/own_config_file.conf`:
> ```
> myApp.spark {
>     some.config.option = "my-app-some-other-value"
> }
> ```

</details>

</details>

</details>

##

<details> <summary><strong>Exercises</strong></summary>

Nothing too crazy: try to build your own `Setl` object! Run your code and examine the logs to check about the options you specified. Make sure it loads the correct configuration file.

</details>

## 2. Extract

<details> <summary><strong>Lesson</strong></summary>

SETL supports two types of data accessors: Connector and SparkRepository.
* A Connector is a non-typed abstraction of data access layer (DAL). For simplicity, you can understand it to as a Spark DataFrame.
* A SparkRepository is a typed abstraction data access layer (DAL). For simplicity, you can understand it as a Spark Dataset.
For more information, please refer to the [official documentation](https://setl-developers.github.io/setl/).

`SETL` supports multiple data format, such as CSV, JSON, Parquet, Excel, Cassandra, DynamoDB, JDBC or Delta.

To ingest data in the `Setl` object entry point, you first must register the data, using the `setConnector()` or the `setSparkRepository[T]` methods.

### 2.1 Registration with `Connector`

<details> <summary></summary>

```
val setl: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl
    .setConnector("testObjectRepository", deliveryId = "id")
```

The first argument provided is a `String` that refers to an item in the specified configuration file. The second argument, `deliveryId`, must be specified for data ingestion. We will see in section **2.3** why it is necessary. Just think of it as an ID, and the only way for `SETL` to ingest a `Connector` is with its ID.

Note that `deliveryId` is not necessary for the registration but it is for the ingestion. However there is no much use if we only register the data. If you are a beginner in `SETL`, you should think as setting a `Connector` must always come with a `deliveryId`.

`local.conf`:
```
setl.config.spark {
  some.config.option = "some-value"
}

testObjectRepository {
  storage = "CSV"
  path = "src/main/resources/test_objects.csv"
  inferSchema = "true"
  delimiter = ","
  header = "true"
  saveMode = "Overwrite"
}
```

As you can see, `testObjectRepository` defines a configuration for data of type `CSV`. This data is in a file, located in `src/main/resources/test_objects.csv`. Other classic read or write options are configured.

In summary, to register a `Connector`, you need to:
1. Specify an item in your configuration file. This item must have a `storage` key, which represents the type of the data. Other keys might be mandatory depending on this type.
2. Register the data in your `Setl` object, using `setConnector("<item>", deliveryId = "<id>")`.

</details>

### 2.2 Registration with `SparkRepository`

<details> <summary></summary>

```
val setl: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl
    .setSparkRepository[TestObject]("testObjectRepository")
```

Like `setConnector()`, the argument provided is a `String` that refers to an item in the specified configuration file.

`local.conf`:
```
setl.config.spark {
  some.config.option = "some-value"
}

testObjectRepository {
  storage = "CSV"
  path = "src/main/resources/test_objects.csv"
  inferSchema = "true"
  delimiter = ","
  header = "true"
  saveMode = "Overwrite"
}
```

Notice that the above `SparkRepository` is set with the `TestObject` type. In this example, the data we want to register is a CSV file containing two columns: `value1` of type `String` and `value2` of type `Int`. That is why the `TestObject` class should be:
```
case class TestObject(value1: String,
                      value2: Int)
```

In summary, to register a `SparkRepository`, you need to:
1. Specify an item in your configuration file. This item must have a `storage` key, which represents the type of the data. Other keys might be mandatory depending on this type.
2. Create a class or a case class representing the object type of your data.
3. Register the data in your `Setl` object, using `setSparkRepository[T]("<item>")`.

</details>

<details> <summary></summary>
    
1. `Connector` or `SparkRepository`?

    Sometimes, the data your are ingesting contain irrelevant information that you do not want to keep. For example, let's say that the CSV file you want to ingest contain 10 columns: `value1`, `value2`, `value3` and 7 other columns you are not interested in.
    
    It is possible to ingest these 3 columns only with a `SparkRepository` if you specify the correct object type of your data:
    ```
    case class A(value1: T1,
                 value2: T2,
                 value3: T3)
    
    setl
        .setSparkRepository[A]("itemInConfFile")
    ```

    This is not possible with a `Connector`. If you register this CSV file with a `Connector`, all 10 columns will appear.

2. Annotations

* `@ColumnName`

    `@ColumnName` is an annotation used in a case class. When you want to rename some columns in your code for integrity but also keep the original name when writing the data, you can use this annotation.
    
    ```
    case class A(@ColumnName("value_one") valueOne: T1,
                 @ColumnName("value_two") valueTwo: T2)
    ```
  
  As you probably know, Scala does not use `snake_case` but `camelCase`. If you register a `SparkRepository` of type `[A]` in your `Setl` object, and if you read it, the columns will be named as `valueOne` and `valueTwo`. The file you read will still keep their name, i.e `value_one` and `value_two`.

* `@CompoundKey`

    TODO

* `@Compress`

    TODO

</details>

### 2.3 Registration of multiple data sources

Most of the time, you will need to register multiple data sources.

<details> <summary></summary>

#### 2.3.1 Multiple `Connector`

<details> <summary></summary>

Let's start with `Connector`. Note that it is perfectly possible to register multiple `Connector`, as said previously. However, there will be an issue during the ingestion. `Setl` has no way to differentiate one `Connector` from another. You will need to set what is called a `deliveryId`.

```
val setl1: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()
 
// /!\ This will work for data registration here but not for data ingestion later /!\
setl1
    .setConnector("testObjectRepository")
    .setConnector("pokeGradesRepository")
 
// Please get used to set a `deliveryId` when you register one or multiple `Connector`
setl1
    .setConnector("testObjectRepository", deliveryId = "testObject")
    .setConnector("pokeGradesRepository", deliveryId = "grades")
```

</details>

#### 2.3.2 Multiple `SparkRepository`

<details> <summary></summary>

Let's now look at how we can register multiple `SparRepository`. If the `SparkRepository` you register all have different type, there will be no issue during the ingestion. Indeed, `Setl` is capable of differentiating the upcoming data by inferring the object type.

```
val setl2: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl2
    .setSparkRepository[TestObject]("testObjectRepository")
    .setSparkRepository[Grade]("pokeGradesRepository")
```

However, if there are multiple `SparkRepository` with the same type, you **must** use a `deliveryId` for each of them. Otherwise, there will be an error during the data ingestion. This is the same reasoning as multiple `Connector`: there is no way to differentiate two `SparkRepository` of the same type.

```
val setl3: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

// /!\ This will work for data registration here but not for data ingestion later /!\
setl3
    .setSparkRepository[Grade]("pokeGradesRepository")
    .setSparkRepository[Grade]("digiGradesRepository")

// Please get used to set a `deliveryId` when you register multiple `SparkRepository` of same type
setl3
    .setSparkRepository[Grade]("pokeGradesRepository", deliveryId = "pokeGrades")
    .setSparkRepository[Grade]("digiGradesRepository", deliveryId = "digiGrades")
```

</details>

</details>

### 2.4 Data Ingestion

<details> <summary></summary>

Before deep diving into data ingestion, we first must learn about how `SETL` organizes an ETL process. `SETL` uses `Pipeline` and `Stage` to organize workflows. A `Pipeline` is where the whole ETL process will be done. The registered data are ingested inside a `Pipeline`, and all transformations and restitution will be done inside it. A `Pipeline` is composed of multiple `Stage`. A `Stage` allows you to modularize your project. It can be constituted of multiple `Factory`. You can understand a `Factory` as a module of your ETL process. So in order to "see" the data ingestion, we have to create a `Pipeline` and add a `Stage` to it. As it may be a little bit theoretical, let's look at some examples.

`App.scala`:
```
val setl4: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl4
    .setConnector("testObjectRepository", deliveryId = "testObjectConnector")
    .setSparkRepository[TestObject]("testObjectRepository", deliveryId = "testObjectRepository")

setl4
    .newPipeline() // Creation of a `Pipeline`.
    .addStage[IngestionFactory]() // Add a `Stage` composed of one `Factory`: `IngestionFactory`.
    .run()
```

Before running the code, let's take a look at `IngestionFactory`.

```
class IngestionFactory extends Factory[DataFrame] with HasSparkSession {

    import spark.implicits._

    override def read(): IngestionFactory.this.type = this

    override def process(): IngestionFactory.this.type = this

    override def write(): IngestionFactory.this.type = this

    override def get(): DataFrame = spark.emptyDataFrame
}
```

This is a skeleton of a `SETL Factory`. A `SETL Factory` contains 4 main functions: `read()`, `process()`, `write()` and `get()`. These functions will be executed in this order. These 4 functions are the core of your ETL process. This is where you will write your classic `Spark` code of data transformation.

You can see that `IngestionFactory` is a child class of `Factory[DataFrame]`. This simply means that the output of this data transformation must be a `DataFrame`. `IngestionFactory` also has the trait `HasSparkSession`. It allows you to access the `SparkSession` easily. Usually, we use it simply to import `spark.implicits`.

Where is the ingestion? 

```
class IngestionFactory extends Factory[DataFrame] with HasSparkSession {

    import spark.implicits._

    @Delivery(id = "testObjectConnector")
    val testObjectConnector: Connector = Connector.empty
    @Delivery(id = "testObjectRepository")
    val testObjectRepository: SparkRepository[TestObject] = SparkRepository[TestObject]
    
    var testObjectOne: DataFrame = spark.emptyDataFrame
    var testObjectTwo: Dataset[TestObject] = spark.emptyDataset[TestObject]

    override def read(): IngestionFactory.this.type = this

    override def process(): IngestionFactory.this.type = this

    override def write(): IngestionFactory.this.type = this

    override def get(): DataFrame = spark.emptyDataFrame
}
```

The structure of a `SETL Factory` starts with the `@Delivery` annotation. This annotation is the way `SETL` ingest the corresponding registered data. If you look at `App.scala` where this `IngestionFactory` is called, the associated `Setl` object has registered a `Connector` with id `testObjectConnector` and a `SparkRepository` with id `testObjectRepository`.

> Note that it is not mandatory to use a `deliveryId` in this case, because there is only one `Factory` with `TestObject` as object type. You can try to remove the `deliveryId` when registering the `SparkRepository` and the `id` in the `@Delivery` annotation. The code will still run. Same can be said for the `Connector`.

With the `@Delivery` annotation, we retrieved a `Connector` and `SparkRepository`. The data has been correctly ingested, but these are data access layers. To process the data, we have to retrieve the `DataFrame` of the `Connector` and the `Dataset` of the `SparkRepository`. This is why we defined two `var`, one of type `DataFrame` and one of type `Dataset[TestObject]`. We will assign values to them during the `read()` function. These `var` are accessible from all the 4 core functions, and you will use them for your ETL process.

To retrieve the `DataFrame` of the `Connector` and the `Dataset` of the `SparkRepository`, we can use the `read()` function.

```
override def read(): IngestionFactory.this.type = {
    testObjectOne = testObjectConnector.read()
    testObjectTwo = testObjectRepository.findAll()

    this
}
```

The `read()` function is typically where you will do your data preprocessing. Usually, we will simply assign values to our variables. Occasionally, this is typically where you would want to do some filtering on your data.

* To retrieve the `DataFrame` of a `Connector`, use the `read()` method.
* To retrieve the `Dataset` of a `SparkRepository`, you can use the `findAll()` method, or the `findBy()` method. The latter allows you to do filtering based on `Condition`. More info [here](https://setl-developers.github.io/setl/Condition).

The registered data is then correctly ingested. It is now ready to be used during the `process()` function.

</details>

### 2.5 Additional resources

<details> <summary></summary>

#### 2.5.1 `AutoLoad`

<details> <summary></summary>

In the previous `IngestionFactory`, we would set a `val` of type `SparkRepository` but also a `var` in which we assign the corresponding `Dataset` in the `read()` function. With `autoLoad = true`, we can skip the first step and directly declare a `Dataset`. The `Dataset` of the `SparkRepository` will be automatically assigned in it.

`App.scala`:
```
val setl5: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl5
    .setSparkRepository[TestObject]("testObjectRepository", deliveryId = "testObjectRepository")

setl5
    .newPipeline()
    .addStage[AutoLoadIngestionFactory]()
    .run()
```

`AutoLoadIngestionFactory`
```
class AutoLoadIngestionFactory extends Factory[DataFrame] with HasSparkSession {

  import spark.implicits._

  @Delivery(id = "testObjectRepository", autoLoad = true)
  val testObject: Dataset[TestObject] = spark.emptyDataset[TestObject]

  override def read(): AutoLoadIngestionFactory.this.type = {
    testObject.show(false)

    this
  }

  override def process(): AutoLoadIngestionFactory.this.type = this

  override def write(): AutoLoadIngestionFactory.this.type = this

  override def get(): DataFrame = spark.emptyDataFrame
}
```

Note that there is no way to use the `findBy()` method to filter the data, compared to the previous `Factory`. Also, `autoLoad` is available for `SparkRepository` only, and not for `Connector`.

</details>

#### 2.5.2 Adding parameters to the `Pipeline`

<details> <summary></summary>

If you want to set some primary type parameters, you can use the `setInput[T]()` method. Those *inputs* are directly set in the `Pipeline`, and there are no registrations like for `Connector` or `SparkRepository`.

`App.scala`:
```
val setl5: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl5
    .newPipeline()
    .setInput[Int](42)
    .setInput[String]("SETL", deliveryId = "ordered")
    .setInput[String]("LTES", deliveryId = "reversed")
    .setInput[Array[String]](Array("S", "E", "T", "L"))
    .addStage[AutoLoadIngestionFactory]()
    .run()
```

*Inputs* are retrieved in the same way `Connector` or `SparkRepository` are retrieved: the `@Delivery` annotation, and the `deliveryId` if necessary.

`AutoLoadIngestionFactory.scala`:
```
class AutoLoadIngestionFactory extends Factory[DataFrame] with HasSparkSession {

    import spark.implicits._

    @Delivery
    val integer: Int = 0
    @Delivery(id = "ordered")
    val firstString: String = ""
    @Delivery(id = "reversed")
    val secondString: String = ""
    @Delivery
    val stringArray: Array[String] = Array()

    override def read(): AutoLoadIngestionFactory.this.type = {
      // Showing that inputs work correctly
      println("integer: " + integer) // integer: 42
      println("ordered: " + firstString) // ordered: SETL
      println("reversed: " + secondString) // reversed: LTES
      println("array: " + stringArray.mkString(".")) // array: S.E.T.L

      this
    }

    override def process(): AutoLoadIngestionFactory.this.type = this

    override def write(): AutoLoadIngestionFactory.this.type = this

    override def get(): DataFrame = spark.emptyDataFrame
}
```

</details>

</details>

### 2.6 Summary

<details> <summary></summary>

In summary, the *extraction* part of an ETL process translates to the following in a `SETL` project:
1. Create a configuration item representing the data you want to ingest in your configuration file.
2. Register the data in your `Setl` object by using the `setConnector()` or the `setSparkRepository[]()` method. Reminder: the mandatory parameter is the name of your object in your configuration file, and you might want to add a `deliveryId`.
3. Create a new `Pipeline` in your `Setl` object, then add a `Stage` with a `Factory` in which you want to process your data.
4. Create a `SETL Factory`, containing the 4 core functions: `read()`, `process()`, `write()` and `get()`.
5. Retrieve your data using the `@Delivery` annotation.
6. Your data is ready to be processed. 

</details>

### 2.7 Data format configuration cheat sheet

Cheat sheet can be found [here](https://setl-developers.github.io/setl/data_access_layer/configuration_example).

</details>

##

<details> <summary><strong>Exercises</strong></summary>

In these exercises, we are going to practice registering and ingesting different types of storage: a CSV file, a JSON file, a Parquet file, an Excel file, a table from DynamoDB, a table from Cassandra, a table from PostgreSQL and a table from Delta.

An `App.scala` is already prepared. We created a `SETL` entry point and use a configuration file located at `src/main/resources/exercise/extract/extract.conf`. In this file, a configuration object has been created for each storage type, but they are incomplete.

The goal here is to complete the configuration objects with the help of the [documentation](https://setl-developers.github.io/setl/data_access_layer/configuration_example), register the data as `Connector` in a `Pipeline` and print them in a `Factory` after ingestion.

To verify registration and ingestion, we prepared `CheckExtractFactory`. To test your code, complete the `???` parts and uncomment the corresponding lines. 

<details> <summary>a) Ingesting a CSV file</summary>

The goal of this first exercise is to register and ingest a CSV file.

We are looking to read the CSV file located at `src/main/resources/exercise/extract/paris-wi-fi-service.csv`.
1. Complete the configuration object `csvFile` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register a `Connector` with this data.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `csvFileConnector`. It will read it into `csvFile` in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of CSV file registration and ingestion.

</details>

<details> <summary>b) Ingesting a JSON file</summary>

The goal of this second exercise is to register and ingest a JSON file.

We are looking to read the JSON file located at `src/main/resources/exercise/extract/paris-notable-trees.json`.
1. Complete the configuration object `jsonFile` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register a `Connector` with this data.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how to do that yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `jsonFileConnector`. It will read it into `parquetFile` in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of JSON file registration and ingestion.

</details>

<details> <summary>c) Ingesting a Parquet file</summary>

The goal of this third exercise is to register and ingest a Parquet file.

We are looking to read the Parquet file located at `src/main/resources/exercise/extract/paris-public-toilet.parquet`.
1. Complete the configuration object `parquetFile` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register a `Connector` with this data.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how to do that yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `parquetFileConnector`. It will read it into `parquetFile` in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of Parquet file registration and ingestion.

</details>

<details> <summary>d) Ingesting an Excel file</summary>

The goal of this fourth exercise is to register and ingest an Excel file.

We are looking to read the Excel file located at `src/main/resources/exercise/extract/paris-textile-containers.xlsx`.
1. Complete the configuration object `excelFile` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register a `Connector` with this data.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how to do that yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `excelFileConnector`. It will read it into `excelFile` in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of Excel file registration and ingestion.

</details>

<details> <summary>e) Ingesting data from DynamoDB</summary>

The goal of this fifth exercise is to register and ingest data from a table in DynamoDB.

To work on this exercise, we need to host a local [DynamoDB](https://aws.amazon.com/fr/dynamodb/) server. To do that, we prepared a `docker-compose.yml` in the `exercise-environment/` folder. Make sure you have [Docker](https://www.docker.com/) installed. In a terminal, change your directory to `exercise-environment/` and execute `docker-compose up`. It will create a local DynamoDB server at `http://localhost:8000`. It will also create a table `orders_table` in the `us-east-1` region, and populate it with some data.

Make sure you launch the Docker containers before starting this exercise.

We are looking to read the `orders_table` table from DynamoDB, located at the `us-east-1` region.
1. Complete the configuration object `dynamoDBData` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register a `Connector` with this data. We already set the endpoint to be `http://localhost:8000` so that the requests are pointing to your local DynamoDB instance.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how to do that yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `dynamoDBDataConnector`. It will read it into `dynamoDBData` in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of DynamoDB data registration and ingestion.

</details>

<details> <summary>f) Ingesting data from Cassandra</summary>

The goal of this sixth exercise is to register and ingest data from a table in Cassandra.

To work on this exercise, we need to host a local [Cassandra](https://cassandra.apache.org/) server. To do that, we prepared a `docker-compose.yml` in the `exercise-environment/` folder. Make sure you have [Docker](https://www.docker.com/) installed. In a terminal, change your directory to `exercise-environment/` and execute `docker-compose up`. It will create a local Cassandra server at `http://localhost:9042`. It will also create a keyspace `mykeyspace` and a table `profiles`, and populate it with some data.

Make sure you launch the Docker containers before starting this exercise.

We are looking to read the `profiles` table from Cassandra, located at the `mykeyspace` keyspace.
1. Complete the configuration object `cassandraDBData` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register a `Connector` with this data. We already set the endpoint to be `http://localhost:9042` so that the requests are pointing to your local Cassandra instance.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how to do that yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `cassandraDataConnector`. It will read it into `cassandraData` in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of Cassandra data registration and ingestion.

</details>

<details> <summary>g) Ingesting data from PostgreSQL</summary>

The goal of this seventh exercise is to register and ingest data from a table in PostgreSQL.

To work on this exercise, we need to host a local [PostgreSQL](https://www.postgresql.org/) server. To do that, we prepared a `docker-compose.yml` in the `exercise-environment/` folder. Make sure you have [Docker](https://www.docker.com/) installed. In a terminal, change your directory to `exercise-environment/` and execute `docker-compose up`. It will create a local PostgreSQL server at `http://localhost:5432`. It will also create a database `postgres` and a table `products`, and populate it with some data.

You will also need the PostgreSQL JDBC driver. As specified in the documentation, you must provide a JDBC driver when using JDBC storage type. o provide the PostgreSQL JDBC driver, head to https://jdbc.postgresql.org/download.html, download the driver, and make the JDBC library jar available to the project. If you are using IntelliJ IDEA, right click on the jar and click on `Add as Library`.

Make sure you launch the Docker containers before starting this exercise.

We are looking to read the `products` table from PostgreSQL, located at the `postgres` database.
1. Complete the configuration object `jdbcDBData` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register a `Connector` with this data. Remember that the endpoint should be `http://localhost:5432`.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how to do that yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `jdbcDataConnector`. It will read it into `jdbcData` in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of PostgreSQL data registration and ingestion.

</details>

<details> <summary>h) Ingesting a local Delta table</summary>

The goal of this eighth exercise is to register and ingest a local Delta table.

We are looking to read the Delta table located at `src/main/resources/exercise/extract/delta-table`. This table contains two versions, and we will read those two versions.
1. Complete the two configuration objects `deltaDataVersionZero` and `deltaDataVersionOne` in `src/main/resources/exercise/extract/extract.conf`.
2. In `App.scala`, register one `Connector` with version zero data and one with version one data.
3. You may create your own `Factory` and implement the `read()` function to verify if you can ingest the data. If you are not sure how to do that yet, we already added a `Factory` to a `Stage`, which is added to the `Pipeline`. This `CheckExtractFactory` will ingest a `Connector` named `deltaDataVersionZeroConnector` and a `Connector` named `deltaDataVersionOneConnector`. It will read it into `deltaDataVersionZero` and `deltaDataVersionOne` respectively in the `read()` method, and verify the number of lines in this data. Uncomment the corresponding lines and complete the part on the `@Delivery` annotation before running your code to test your implementation of local Delta table registration and ingestion.

</details>

To challenge yourself, try to replace the different `Connector` with `SparkRepository`. Make use of what you have learned in the *lesson*!

</details>

## 3. Transform

<details> <summary><strong>Lesson</strong></summary>

Transformations in `SETL` are the easiest part to learn. There is nothing new if you are used to write ETL jobs with `Spark`. This is where you will transfer the code you write with `Spark` into `SETL`.

### 3.1 `Factory`

<details> <summary></summary>

After seeing what the `read()` function in a `Factory` looks like, let's have a look at the `process()` function that is executed right after.
```
class ProcessFactory extends Factory[DataFrame] with HasSparkSession {

    @Delivery(id = "testObject")
    val testObjectConnector: Connector = Connector.empty

    var testObject: DataFrame = spark.emptyDataFrame

    var result: DataFrame = spark.emptyDataFrame

    override def read(): ProcessFactory.this.type = {
      testObject = testObjectConnector.read()

      this
    }

    override def process(): ProcessFactory.this.type = {
      val testObjectDate = testObject.withColumn("date", lit("2020-11-20"))

      result = testObjectDate
        .withColumnRenamed("value1", "name")
        .withColumnRenamed("value2", "grade")

      this
    }

    override def write(): ProcessFactory.this.type = this

    override def get(): DataFrame = spark.emptyDataFrame
}
```

You should understand the first part of the code with the ingestion thanks to the `@Delivery` and the `read()` function. Here is declared a `var result` in which will be stored the result of the data transformations. It is declared globally so that it can be accessed later in the `write()` and `get()` functions. The data transformations are what is inside the `process()` function, and you must surely know what they do.

As it is previously said, there is nothing new to learn here: you just write your `Spark` functions to transform your data, and this is unrelated to `SETL`. 

</details>

### 3.2 `Transformer`

<details> <summary></summary>

You might not learn anything new for `SETL` for data transformations in itself, but `SETL` helps you to structure them. We will now take a look about `SETL Transformer`. You already know about `Factory`. A `Factory` can contain multiple `Transformer`. A `Transformer` is a piece of highly reusable code that represents one data transformation. Let's look at how it works.

```
class ProcessFactoryWithTransformer extends Factory[DataFrame] with HasSparkSession {

    @Delivery(id = "testObject")
    val testObjectConnector: Connector = Connector.empty

    var testObject: DataFrame = spark.emptyDataFrame

    var result: DataFrame = spark.emptyDataFrame

    override def read(): ProcessFactoryWithTransformer.this.type = {
        testObject = testObjectConnector.read()
  
        this
    }

    override def process(): ProcessFactoryWithTransformer.this.type = {
        val testObjectDate = new DateTransformer(testObject).transform().transformed
        result = new RenameTransformer(testObjectDate).transform().transformed
  
        this
    }

    override def write(): ProcessFactoryWithTransformer.this.type = this

    override def get(): DataFrame = spark.emptyDataFrame
}
```

If you compare this `Factory` with the previous `ProcessFactory` in the last section, it does the same job. However, the workflow is more structured. You can see that in the `process()` function, there is no `Spark` functions for data transformations. Instead, we used `Transformer`. The data transformation will be done in `Transformer`. This allows to make to code highly reusable and add a lot more structure to it. In the previous `ProcessFactory`, we can divide the job by two: the first process is adding a new column, and the second process is renaming the column.

First, we are calling the first `Transformer` by passing our input `DataFrame`. The `transform()` method is then called, and the result is retrieved with the `transformed` getter. The second data transformation is done with `RenameTransformer`, and the result is assigned to our `result` variable. Let's have a look at each `Transformer`.

A `Transformer` has two core methods:
* `transform()` which is where the data transformation should happen.
* `transformed` which is a getter to retrieve the result.

Typically, we will also declare a variable in which we will assign the result of the transformation. In this case, `transformedData`. The `transformed` getter returns this variable. This is why in `ProcessingFactoryWithTransformer`, the `transform()` method is called, before calling the `transformed` getter.

`DateTransformer.scala`:
```
class DateTransformer(testObject: DataFrame) extends Transformer[DataFrame] with HasSparkSession {
    private[this] var transformedData: DataFrame = spark.emptyDataFrame

    override def transformed: DataFrame = transformedData

    override def transform(): DateTransformer.this.type = {
      transformedData = testObject
          .withColumn("date", lit("2020-11-20"))

      this
    }
}
```

`DateTransformer` represents the first data transformation that is done in the `ProcessFactory` in the previous section: adding a new column.

`RenameTransformer`:
```
class RenameTransformer(testObjectDate: DataFrame) extends Transformer[DataFrame] with HasSparkSession {
    private[this] var transformedData: DataFrame = spark.emptyDataFrame

    override def transformed: DataFrame = transformedData

    override def transform(): RenameTransformer.this.type = {
      transformedData = testObjectDate
        .withColumnRenamed("value1", "name")
        .withColumnRenamed("value2", "grade")

      this
    }
}
```

`RenameTransformer` represents the second data transformation that is done in the `ProcessFactory` in the previous section: renaming the columns.

</details>

### 3.3 Summary

<details> <summary></summary>

The classic data transformations happen in the `process()` function of your `Factory`. This is how you write your data transformations in `SETL`, given that you already did what is needed in the Extract part. You have two solutions:
1. Write all the data transformations with `Spark` functions in the `process()` function of your `Factory`. Remember to set a global variable to store the result so that it can be used in the next functions of the `Factory`.
2. Organize your workflow with `Transformer`. This is best for code reusability, readability, understanding and structuring. To use a `Transformer`, remember that you need to pass parameters, usually the `DataFrame` or the `Dataset` you want to transform, eventually some parameters. You need to add the `transform()` function which is where the core `Spark` functions should be called, and the `transformed` getter to retrieve the result. 

</details>

</details>

##

<details> <summary><strong>Exercises</strong></summary>

In this exercise, we are going to practice about how to structure a `SETL` project for transformation processes.

An App.scala is already prepared. We created a SETL entry point and use a configuration file located at `src/main/resources/exercise/transform/transform.conf`. In this file, configuration objects are already created. We will be working with `pokeGrades.csv` and `digiGrades.csv`, both files located at `src/main/resources/`. We are looking to looking to compute the mean score of each "poke" and then of each "digi".

1. We are going to extract the data: `pokeGrades.csv` and `digiGrades.csv`, from `src/main/resources/`. In `App.scala`, register these two as `SparkRepository`.
2. Next step is to complete the `Factory`. Head over to `MeanGradeFactory` to complete the part about data ingestion.
3. You should know a `Factory` has 4 core mandatory functions. Leave the `write()` and `get()` functions as they are. Use the `read()` function if necessary. Keep in mind about what `autoLoad` is.
4. In the `process()` function, we are going to compute the mean grade for `pokeGrades` and `digiGrades` data. To do that, we are going to create a `Transformer`, named `MeanGradeTransformer`. This `Transformer` takes a parameter of type `Dataset[Grade]` and outputs an object of type `DataFrame`. There should be two columns: one column `name` and one column `grade` for the mean grade.
5. In the `process()` function, we can now call the `Transformer` on each data, apply transformations and store the result in variables.
6. Lastly, we can merge the two results and verify the final `DataFrame` by printing it.

Follow the instructions in the code to achieve this exercise. If you'd like to challenge yourself, try to write a complete `Pipeline` by yourself, without the help of the prepared code files. For example, you can try to find the top-3 scores of each "poke" and each "digi".

</details>

## 4. Load

<details> <summary><strong>Lesson</strong></summary>

The Load processes with SETL correspond to two key ideas: writing the output, or passing the output. Passing the output allows to pass the result of a `Factory` to another `Factory`, for example. The second `Factory` is then using the result of a previous `Factory` as an input.

### 4.1 Writing an output

<details> <summary></summary>

In order to write data, you need to register a `Connector` or a `SparkRepository`. As you probably already know, if you want to write a `DataFrame`, register a `Connector`. If you want to write a `Dataset`, register a `SparkRepository`. Do not forget that you must create a configuration item in the configuration file. There, you can specify the path of your output.

`App.scala`:
```
val setl0: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl0
    .setConnector("testObjectRepository", deliveryId = "testObject")
    .setConnector("testObjectWriteRepository", deliveryId = "testObjectWrite")

setl0
    .newPipeline()
    .setInput[String]("2020-11-23", deliveryId = "date")
    .addStage[WriteFactory]()
```

`local.conf`:
```
testObjectRepository {
  storage = "CSV"
  path = "src/main/resources/test_objects.csv"
  inferSchema = "true"
  delimiter = ","
  header = "true"
  saveMode = "Overwrite"
}

testObjectWriteRepository {
  storage = "EXCEL"
  path = "src/main/resources/test_objects_write.xlsx"
  useHeader = "true"
  saveMode = "Overwrite"
}
```

`WriteFactory.scala`:
```
class WriteFactory extends Factory[DataFrame] with HasSparkSession {

    @Delivery(id = "date")
    val date: String = ""
    @Delivery(id = "testObject")
    val testObjectConnector: Connector = Connector.empty
    @Delivery(id = "testObjectWrite")
    val testObjectWriteConnector: Connector = Connector.empty

    var testObject: DataFrame = spark.emptyDataFrame

    var result: DataFrame = spark.emptyDataFrame

    override def read(): WriteFactory.this.type = {
        testObject = testObjectConnector.read()

        this
    }

    override def process(): WriteFactory.this.type = {
        result = testObject
            .withColumn("date", lit(date))

        this
    }
  
    override def write(): WriteFactory.this.type = {
        testObjectWriteConnector.write(result.coalesce(1))

        this
    }

    override def get(): DataFrame = spark.emptyDataFrame
}
```

Note that in the `Deliveries`, there is one with the ID `testObjectWrite`. It has been previously registered in the `Pipeline`. We are retrieving it, but using it as a way to write our output.

The `write()` function is the third executed function in a `Factory`, after `read()` and `process()`. The idea is to call the `write()` method of a `Connector` or a `SparkRepository`, and pass the result `DataFrame` or `Dataset` as argument. `SETL` will automatically read the configuration item; storage type, path and options, and write the result there.

The advantage of using `SETL` for the Load process is that it makes it easier for you because you can change everything you need in your configuration item. If you ever want to change the data storage, you only need to modify the value of the corresponding key. Same for the path, or other options.

**In summary**, to write an output in `SETL`, you need to:
1. Create a configuration item in your configuration file
2. Register the corresponding `Connector` or `SparkRepository`
3. Ingest it in your `Factory` with the `@Delivery` annotation
4. Use it in the `write()` function to write your output


</details>

### 4.2 Getting an output

<details> <summary></summary>

As SETL is organized with `Factory`, it is possible to pass the result of a `Factory` to another. The result of a `Factory` can be of any type, it generally is a `DataFrame` or a `Dataset`. 

#### 4.2.1 Getting a `DataFrame`

<details> <summary></summary>

We are now going to ingest data and make some transformations in `FirstFactory`, then use the result in `SecondFactory`. You can see in the `Pipeline` that `FirstFactory` is before `SecondFactory`.

`App.scala`:
```
val setl1: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl1.setConnector("testObjectRepository", deliveryId = "testObject")

setl1
    .newPipeline()
    .setInput[String]("2020-12-18", deliveryId = "date")
    .addStage[FirstFactory]()
    .addStage[SecondFactory]()
    .run()
```

`FirstFactory.scala`:
```
class FirstFactory extends Factory[DataFrame] with HasSparkSession {

    @Delivery(id = "date")
    val date: String = ""
    @Delivery(id = "testObject")
    val testObjectConnector: Connector = Connector.empty

    var testObject: DataFrame = spark.emptyDataFrame

    var result: DataFrame = spark.emptyDataFrame

    override def read(): FirstFactory.this.type = {
        testObject = testObjectConnector.read()

        this
    }

  override def process(): FirstFactory.this.type = {
    result = testObject
      .withColumn("date", lit(date))

    this
  }

  override def write(): FirstFactory.this.type = this

  override def get(): DataFrame = result
}
```

This `FirstFactory` is similar to the previous `WriteFactory`. Instead of writing the result, we are going to pass it in the `get()` function. The `get()` function is the fourth executed function in a `Factory`, after `read()`, `process()` and `write()`. In the above example, the output is simply returned.

Remember that the type of the output is defined at the start of the `Factory`, when specifying the parent class. In this case, the output is a `DataFrame`. This output is then injected in the `Pipeline` as a `Deliverable`. The other `Factory` can then ingest it.

`SecondFactory.scala`:
```
class SecondFactory extends Factory[DataFrame] with HasSparkSession {

    import spark.implicits._

    @Delivery(producer = classOf[FirstFactory])
    val firstFactoryResult: DataFrame = spark.emptyDataFrame

    var secondResult: DataFrame = spark.emptyDataFrame

    override def read(): SecondFactory.this.type = this

    override def process(): SecondFactory.this.type = {
        secondResult = firstFactoryResult
            .withColumn("secondDate", $"date")

        secondResult.show(false)

        this
    }

    override def write(): SecondFactory.this.type = this

    override def get(): DataFrame = secondResult
}
```

In this `SecondFactory`, we want to retrieve the output produced by `FirstFactory`. Noticed that we used the `producer` argument in the `@Delivery` annotation. This is how `SETL Pipeline` retrieves the output of a `Factory`: the result of a `Factory` is injected into the `Pipeline` as a `Deliverable`, which can be ingested with the `@Delivery` annotation. 

</details>

#### 4.2.2 Getting a `Dataset`

<details> <summary></summary>

In the previous `Pipeline`, we retrieved the result of `FirstFactory` to use it in `SecondFactory`. The result of `FirstFactory` was a `DataFrame`, and we needed to retrieve it in `SecondFactory` by using the `producer` argument in the `@Delivery` annotation. In the following `Pipeline`, we are going to produce a `Dataset` from `FirstFactoryBis` and use it in `SecondFactoryBis`.

`App.scala`:
```
val setl2: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl2
    .setConnector("testObjectRepository", deliveryId = "testObject")

setl2
    .newPipeline()
    .setInput[String]("2020-12-18", deliveryId = "date")
    .addStage[FirstFactoryBis]()
    .addStage[SecondFactoryBis]()
    .run()
```

`FirstFactoryBis.scala`:
```
class FirstFactoryBis extends Factory[Dataset[TestObject]] with HasSparkSession {

    import spark.implicits._

    @Delivery(id = "testObject")
    val testObjectConnector: Connector = Connector.empty

    var testObject: DataFrame = spark.emptyDataFrame

    var result: Dataset[TestObject] = spark.emptyDataset[TestObject]

    override def read(): FirstFactoryBis.this.type = {
      testObject = testObjectConnector.read()

      this
  }

    override def process(): FirstFactoryBis.this.type = {
        result = testObject
            .withColumn("value1", concat($"value1", lit("42")))
            .as[TestObject]

        this
    }

    override def write(): FirstFactoryBis.this.type = this

    override def get(): Dataset[TestObject] = result
}
```

Noticed that the `FirstFactoryBis` is a child class of `Factory[Dataset[TestObject]]`, meaning that the output of it must be a `Dataset[TestObject]`. `result` is a variable of type `Dataset[TestObject]`, and the `get()` function returns it. This `Dataset` is injected into the `Pipeline`.

`SecondFactoryBis.scala`:
```
class SecondFactoryBis extends Factory[DataFrame] with HasSparkSession {

    import spark.implicits._

    @Delivery(id = "date")
    val date: String = ""
    @Delivery
    val firstFactoryBisResult: Dataset[TestObject] = spark.emptyDataset

    var secondResult: DataFrame = spark.emptyDataFrame

    override def read(): SecondFactoryBis.this.type = this

    override def process(): SecondFactoryBis.this.type = {
        secondResult = firstFactoryBisResult
            .withColumn("secondDate", lit("date"))

        secondResult.show(false)

        this
    }

    override def write(): SecondFactoryBis.this.type = this

    override def get(): DataFrame = secondResult
}
```

The result of `FirstFactoryBis` is a `Dataset[TestObject]`. We used the `@Delivery` annotation to retrieve it. Compared to `SecondFactory`, we did not need to use the `producer` in the `@Delivery` annotation. This is because the `Pipeline` can infer on the data, and the only `Dataset[TestObject]` that it found is produced by `FirstFactoryBis`. So there is no need to specify it. This is the same mechanism that explains why a `Connector` needs a `deliveryId` to be retrieved, and not a `SparkRepository[T]` if there is only one of type T that is registered.

</details>

#### 4.2.3 Summary

<details> <summary></summary>

In summary, to use the output of a `Factory` in another one:
1. Check the type of the output.
2. Make sure that the `Stage` of the first `Factory` is before the `Stage` of the second `Factory`.
3. The second `Factory` must be a child class of `Factory[T]` where `T` is the type of the output of the first `Factory`.
4. Retrieve the output of the first `Factory` by using the `@Delivery` annotation. If it is a `DataFrame`, also use the `producer` argument.

Note: Although it is possible to retrieve the output of a `Factory` in another one, most of the time, we would prefer to save the output of the first `Factory` in a `Connector` or `SparkRepository`, and re-use the same `Connector` or `SparkRepository` in the second `Factory` to retrieve the output.

</details>

</details>

</details>

##

<details> <summary><strong>Exercises</strong></summary>

In this exercise, we are going to practice about how to the Load processes with SETL, that is, how to pass the result of a `Factory` to another `Factory`, and how to write the result of a `Factory`.

An App.scala is already prepared. We created a SETL entry point and use a configuration file located at `src/main/resources/exercise/load/load.conf`. In this file, configuration objects are already created. We will be working with `pokeGrades.csv` and `digiGrades.csv`, both files located at `src/main/resources/`. We are going to find out how many exams there are per year. To do that, a first `Factory` will "compute" all the dates from the data, and pass this result to the second `Factory`. This second `Factory` ingest the result, extract the year of each date and count the number of exams per year.

1. We are going to extract the data: `pokeGrades.csv` and `digiGrades.csv`, from `src/main/resources/`. In `App.scala`, register these two as `SparkRepository`. Also register a `Connector` where to write your output. Remind that a configuration file have been provided.
2. Next step is to complete the two `Factory`. Head over to `GetExamsDateFactory` first.
3. Complete the part on the data ingestion by setting the `Delivery`. In `GetExamsDateFactory`, the goal is to get the different exam dates of `pokeGrades.csv` and `digiGrades.csv`. Use the `read()` function if necessary. In the `process()` function, concatenate both the "poke" and the "digi" data. Then, only keep the `date` column, as it is the only relevant column in this exercise. Leave the `write()` function as is, and complete the `get()` function by returning the result of your process.
4. Now, head over to `ExamStatsFactory`. This `Factory` will ingest the result of `GetExamsDateFactory`. As usual, the first step is to add the `Delivery`. Remember that to write an output, you also have to add a `Connector` or `SparkRepository` for the output, as it can define the storage type and the path. Also remember about `producer`. Go over to the lesson if you forgot about it.
5. Use the `read()` function if necessary. In the `process()` function, we are looking to compute the number of exams per year. Our input data is a `DataFrame` of 1 single column `date`. Replace the `date` column by extracting the year only: it is the first 4 characters of the `date` column. Then, count the number of `date`. Use the `groupBy()`, `agg()` and `count()` functions. In our data, each year is duplicated 10 times. Indeed, for each exam, there are always 10 "poke" or 10 "digi". As a consequence, we need to divide the count by 10.
6. Use the output Delivery you declared to save the result output in the `write()` function. Complete the `get()` function to return the result, even though it is not used. If you run the code, you should have the number of exams per year, located in `src/main/resources/examsStats/`.

Follow the instructions in the code to achieve this exercise. If you'd like to challenge yourself, try to write a complete `Pipeline` by yourself, without the help of the prepared code files. For example, you can try to save in a file the list of "poke" and "digi".

Note that these exercises are simply used to practise `SETL` and their structure may very well not be optimized for your production workflow. These are just simple illustrations of what you can usually do with the framework. 

</details>

## 5. From local to production environment

<details> <summary><strong>Lesson</strong></summary>

The difference between multiple development environment consists in the location of files/data we want to read and the location of files/data we want to write.

### 5.1 Changing the `path`

<details> <summary></summary>

In order to see how `SETL` handles between local and production environment, we are going to set two `Connector`: one for `local` and one for `prod`.

`App.scala`:
```
val setl: Setl = Setl.builder()
    .withDefaultConfigLoader("storage.conf")
    .setSparkMaster("local[*]")
    .getOrCreate()

setl
    .setConnector("pokeGradesRepository", deliveryId = "pokeGradesRepository")
    .setConnector("pokeGradesRepositoryProd", deliveryId = "pokeGradesRepositoryProd")

setl
    .newPipeline()
    .addStage[ProductionFactory]()
    .run()
```

`storage.conf`:
```
setl.config.spark {
  spark.hadoop.fs.s3a.access.key = "dummyaccess" // Used to connect to AWS S3 prod environment
  spark.hadoop.fs.s3a.secret.key = "dummysecret" // Used to connect to AWS S3 prod environment
  spark.driver.bindAddress = "127.0.0.1"
}

pokeGradesRepository {
  storage = "CSV"
  path = "src/main/resources/pokeGrades.csv"
  inferSchema = "true"
  delimiter = ","
  header = "true"
  saveMode = "Overwrite"
}

pokeGradesRepositoryProd {
  storage = "CSV"
  path = "s3a://setl-examples/pokeGrades.csv"
  inferSchema = "true"
  delimiter = ","
  header = "true"
  saveMode = "Overwrite"
}
```

The difference between these two repositories is the path. The first object uses a local path, and the second uses a AWS S3 path, considered as a production environment. They are exactly the same file. When ingesting these files into a `Factory`, we can retrieve the same `DataFrame`. Thus, in `SETL`, it is possible to switch your development environment **without looking** at the code. You just need to make adjustments to the `path` of your configuration objects.

</details>

### 5.2 Generalize your configuration

<details> <summary></summary>

Most of the time, you will have a lot of configuration objects for both input and output. Changing the path for all of these objects may not be efficient. Instead of having two configuration objects (`pokeGradesRepository` and `pokeGradesRepositoryProd`) like in the last section, you can simply declare one configuration object, and make it reusable.

`local.conf`:
```
setl.config.spark {
  some.config.option = "some-value"
}

root {
  path = "src/main/resources"
}

include "smartConf.conf" // /!\ important
``` 

`prod.conf`
```
setl.config.spark {
  spark.hadoop.fs.s3a.endpoint = "http://localhost:9090"
  spark.hadoop.fs.s3a.access.key = "dummyaccess"
  spark.hadoop.fs.s3a.secret.key = "dummysecret"
  spark.hadoop.fs.s3a.path.style.access = "true"
  spark.driver.bindAddress = "127.0.0.1"
}

root {
  path = "s3a://setl-examples"
}

include "smartConf.conf" // /!\ important
```

`smartConf.conf`:
```
smartPokeGradesRepository {
  storage = "CSV"
  path = ${root.path}"/pokeGrades.csv"
  inferSchema = "true"
  delimiter = ","
  header = "true"
  saveMode = "Overwrite"
}
```

If you look at `smartConf.conf`, notice the `path` key: it uses the `root.path` key. `smartConf.conf` is included in both in `local.conf` and `prod.conf`, which are the configuration files to be loaded. In `local.conf`, `root.path` is set to a value corresponding to a local path, and in `prod.conf`, it is set to a value corresponding to a prod path, which is a S3 path in this example. Let's now see how to switch development environment.

Note that in the `Setl` object below, we used the `withDefaultConfigLoader()` method. This means that `application.conf` will be loaded, and it retrieves the `app.environment`. `app.environment` is a VM option. By default, it is set to `local` in the `pom.xml` file. Depending on the `app.environment`, it will load the corresponding configuration file, i.e `<app.environment>.conf`.

`App.scala`:
```
val smartSetl: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .setSparkMaster("local[*]")
    .getOrCreate()

smartSetl.setConnector("smartPokeGradesRepository", deliveryId = "smartPokeGradesRepository")
println(smartSetl.getConnector[Connector]("smartPokeGradesRepository").asInstanceOf[FileConnector].options.getPath)
```

Now, to see how easy it is to switch development environment with `SETL`, change the VM option `-Dapp.environment` by setting it to `local` or `prod`. If you run `App.scala`, you will see that the path will change according to the environment:
* `src/main/resources/pokeGrades.csv` if `-Dapp.environment=local`
* `s3a://setl-examples/pokeGrades.csv` if `-Dapp.environment=prod`

</details>

### 5.3 Summary

<details> <summary></summary>

In summary, you can change your development environment by changing to path of your configuration objects. However, this can be obnoxious especially if you have a lot of input/output storage object. By writing a general configuration file, you simply need adjust the VM option to switch your development environment, and get the corresponding paths of your data.

Remind that `SETL` aims at simplifying the Extract and Load processes so that a Data Scientist can focus on his core job: data transformations. On top of that, it gives structure and allows more modularization of your code!

</details>


</details>