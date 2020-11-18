# SETL exercises

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
