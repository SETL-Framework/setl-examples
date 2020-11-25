package com.github.joristruong.production.lesson

import java.io.File

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.github.joristruong.production.lesson.factory.ProductionFactory
import com.jcdecaux.setl.Setl
import com.jcdecaux.setl.storage.connector.{Connector, FileConnector}
import io.findify.s3mock.S3Mock

/**
 * Learn about how `SETL` works from local to production environment
 */
object App {
  def main(args: Array[String]): Unit = {
    /**
     * This section has nothing to do with `SETL`.
     * To experiment with a production environment, we are going to simulate an AWS S3 production environment.
     * In this simulated S3, there is only one bucket named `setl-examples`, containing only one object with key `pokeGrades.csv`.
     * This object is a CSV file which is equal to the `pokeGrades.csv` you can find on `src/main/resources/pokeGrades.csv`.
     */
    val s3Mock: S3Mock = S3Mock(port = 9090)
    s3Mock.start

    val endpoint = new EndpointConfiguration("http://localhost:9090", "us-west-2")
    val client = AmazonS3ClientBuilder
      .standard
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
      .build

    client.createBucket("setl-examples")
    client.putObject("setl-examples", "pokeGrades.csv", new File("src/main/resources/pokeGrades.csv"))
    assert(client.doesObjectExist("setl-examples", "pokeGrades.csv"))
    /**
     * End of S3 mocking code
     */

    /**
     * In order to see the difference between local and production environment,
     * we are going to set two `Connector`: one for local and one for prod.
     *
     * Before running the code, please use the VM option `-Dapp.environment=prod`.
     * If you remember correctly, this will make the `ConfigLoader` load `prod.conf`.
     */
    val setl: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .setSparkMaster("local[*]")
      .getOrCreate()

    /**
     * Head over to `prod.conf` and look at the differences between the two objects.
     * They are the same file, but one is located in a local environment, the other is located in an AWS S3 prod environment.
     * The only thing that changes is the path.
     */
    setl
      .setConnector("pokeGradesRepository", deliveryId = "pokeGradesRepository") // in `storage.conf`
    if (setl.configLoader.getAppEnvFromJvmProperties.contains("prod")) {
      setl
        .setConnector("pokeGradesRepositoryProd", deliveryId = "pokeGradesRepositoryProd") // in `storage.conf`

      setl
        .newPipeline()
        .addStage[ProductionFactory]()
        // Before running the code, I invite you to go over `ProductionFactory` for more details. Feel free to remove the line comment below afterwards.
        //.run()
    }
    /**
     * You can see that one can easily switch from local to production environment: the only thing that needs to be changed is the input/output files path.
     * Remind that `SETL` aims at simplifying the Extract and Load processes so that a Data Scientist can focus on his core job: data transformations.
     * On top of that, it gives structure and allows more modularization of your code!
     */


    /**
     * Instead of having two configuration objects (`pokeGradesRepository` and `pokeGradesRepositoryProd`), you can simply declare one configuration object.
     * Head over to `smartConf.conf` and look at the `path` key. It uses the `root.path` key.
     * Then, `smartConf.conf` is included in both in `local.conf` and `prod.conf`.
     * In `local.conf`, `root.path` is set to a value corresponding to a local path, and in `prod.conf`, it is set to a value corresponding to a prod path, which is a S3 path in this example.
     * Note that in the `Setl` object below, we used the `withDefaultConfigLoader()` method. This means that `application.conf` will be loaded, and it retrieves the `app.environment`.
     * `app.environment` is a VM option. By default, it is set to `local` in the `pom.xml` file.
     * Depending on the `app.environment`, it will load the corresponding configuration file, i.e `<app.environment>.conf`.
     */
    val smartSetl: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .setSparkMaster("local[*]")
      .getOrCreate()

    /**
     * Now, to see how easy it is to switch development environment with `SETL`, change the VM option `-Dapp.environment` by setting it to `local` or `prod`.
     * Uncomment the lines below, and you will see that the path will change according to the environment.
     */
    //smartSetl.setConnector("smartPokeGradesRepository", deliveryId = "smartPokeGradesRepository")
    //println(smartSetl.getConnector[Connector]("smartPokeGradesRepository").asInstanceOf[FileConnector].options.getPath)

    /**
     * In summary, you can change your development environment by changing to path of your configuration objects.
     * However, this can be obnoxious especially if you have a lot of input/output storage object.
     *
     * By writing a general configuration file, you simply need adjust the VM option to switch your development environment, and get the corresponding paths of your data.
     */

    s3Mock.shutdown
  }
}
