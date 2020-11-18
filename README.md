# SETL examples

Lessons and exercises to get familiar with the wonderful [SETL](https://github.com/SETL-Developers/setl) framework!

# Chapters

## 1. Entry Point and configurations

<details> <summary>Lesson</summary>

<h3>1.1. Entry point with basic configurations</h3>

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

<h3>1.2. Entry point with specific configurations</h3>

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

<h3>1.3 Utilities</h3>

<h5>Helper methods</h5>

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

<h5>SparkSession options</h5>

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

<details> <summary>Exercises</summary>

Nothing too crazy: try to build your own `Setl` object! Run your code and examine the logs to check about the options you specified. Make sure it loads the correct configuration file.

</details>

## 2. Extract

<details> <summary>Lesson</summary>

SETL supports two types of data accessors: Connector and SparkRepository.
* A Connector is a non-typed abstraction of data access layer (DAL). For simplicity, you can understand it to as a Spark DataFrame.
* A SparkRepository is a typed abstraction data access layer (DAL). For simplicity, you can understand it as a Spark Dataset.
For more information, please refer to the [official documentation](https://setl-developers.github.io/setl/).

`SETL` supports multiple data format, such as CSV, JSON, Parquet, Excel, Cassandra, DynamoDB, JDBC or Delta.

To ingest data in the `Setl` object entry point, you first must register the data, using the `setConnector()` or the `setSparkRepository[T]` methods.

### 2.1 Registration with `Connector`

```
val setl: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

setl
    .setConnector("testObjectRepository")
```

The argument provided is a `String` that refers to an item in the specified configuration file.

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
2. Register the data in your `Setl` object, using `setConnector(<item>)`.

### 2.2 Registration with `SparkRepository`

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
3. Register the data in your `Setl` object, using `setSparkRepository[T](<item>)`.

<details> <summary>Advanced</summary>
    
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

#### 2.3.1 Multiple `Connector`

Let's start with `Connector`. Note that it is perfectly possible to register multiple `Connector`. However, and we will go through that later on, during the ingestion, there will be an issue if there are multiple `Connector`. `Setl` has no way to differentiate one `Connector` from another. You will need to set what is called a `deliveryId`.

```
val setl1: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()
 
// /!\ This will work for data registration here but not for data ingestion later /!\
setl1
    .setConnector("testObjectRepository")
    .setConnector("pokeGradesRepository")
 
// Please get used to set a `deliveryId` when you register multiple `Connector`
setl1
    .setConnector("testObjectRepository", deliveryId = "testObject")
    .setConnector("pokeGradesRepository", deliveryId = "grades")
```

#### 2.3.2 Multiple `SparkRepository`

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

### 2.4 Data Ingestion



### 2.5 Data format configuration cheat sheet

Cheat sheet can be found [here](https://setl-developers.github.io/setl/data_access_layer/configuration_example).

</details>

<details> <summary>Exercises</summary>



</details>

## 3. Transform

<details> <summary>Lesson</summary>



</details>

<details> <summary>Exercises</summary>



</details>

## 4. Load

<details> <summary>Lesson</summary>



</details>

<details> <summary>Exercises</summary>



</details>

## 5. From local to production environment

<details> <summary>Lesson</summary>



</details>

<details> <summary>Exercises</summary>



</details>