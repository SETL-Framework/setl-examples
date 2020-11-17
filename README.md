# SETL exercises

Exercises to get familiar with the wonderful [SETL](https://github.com/SETL-Developers/setl) framework!

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

> `resources/application.conf:`
> ```
> setl.environment = <app.env>
> ```

> `<app.env>.conf`
> ```
> setl.config.spark {
>    some.config.option = "some-value"
>  }
> ```

The configuration file is where you can specify your `SparkSession` options, like when you create one in a basic `Spark` process.

<h3>1.2. Entry point with specific configurations</h3>

You can also specify the configuration file that the `ConfigLoader` should read. In the code below, instead of reading `<app_env>.conf` where `<app_env>` is defined in `application.conf`, it will read `own_config_file.conf`.
> ```
> val setl1: Setl = Setl.builder()
>        .withDefaultConfigLoader("own_config_file.conf")
>        .getOrCreate()
> ```
> 
> `resources/own_config_file.conf` 
> ```
> setl.config.spark {
>    some.config.option = "some-other-value"
>  }
> ```

<h3>1.3 Utilities</h3>

</details>

<details> <summary>Exercises</summary>



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
