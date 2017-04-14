[![Build Status](https://travis-ci.org/fdm1/coursera_spark_scala.svg?branch=master)](https://travis-ci.org/fdm1/coursera_spark_scala)

Coursera - Big Data Analysis with Scala and Spark
=================================================

This is my work on the [Coursera Spark/Scala course](https://www.coursera.org/learn/scala-spark-big-data)

### My workflow
---------------

I have just been working in a tmux with the source code on one side, and the workflow below on the other


 1. If the assignment requires data, I'm writing scripts to download it

    ```
    bash coursera/week_1/download_wikipedia_data.sh
    ```

 1. Spin up spark in a docker container:

    ```
    docker-compose up -d master     # so far just running everything in local mode, so no need for workers
    docker-compose exec master bash
    ```

 1. Once in the container:

    ```
    cd /coursera/week_{n}/{assignment}  # e.g. cd /coursera/week_1/wikipedia
    sbt
    ```

 1. Once in sbt

    ```
    test                                 # run tests

    console                                # enter scala console

    # in scala console, you can import the scala modules
    import wikipedia.WikipediaRanking._    # import current state of src/main/wikipedia/WikipediaRanking.scala

    :q     # quits out of scala console, while keeping you in sbt (ctrl-C backs you out to bash prompt)
    ```

--------------------
#### protips:

- If in the scala console and you've spun up a `SparkContext`, be sure to run `sc.stop` before `:q` to stop it.
  (It just results in annoying persistent warnings/errors if you don't until you exit to `bash`)


Testing
-------

1. Automated via `travis`

    This is configured to use travis to automatically run tests.
    Simply add the filepath to the root of each sbt project dir as a `TEST_SUITE` `env` in `.travis.yml`.

    ```
    env:
      - TEST_SUITE=coursera/week_1/example
      - TEST_SUITE=coursera/week_2/stackoverflow
    ```

1. Test interactively in the container with `test` inside the `sbt` console or `sbt test` in the container's `bash` prompt.




