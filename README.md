Coursera - Big Data Analysis with Scala and Spark
=================================================

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
- If you run `docker-compose down`, it will remove the container, and the next time you start working, `sbt` will be slow.
  Don't run this unless you mean to.
