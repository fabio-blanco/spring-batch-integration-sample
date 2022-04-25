# Spring Batch Integration Sample

This project is a simple sample of using [Spring Batch](https://spring.io/projects/spring-batch) 
with [Spring Integration](https://spring.io/projects/spring-integration).
The [Spring Batch Integration documentation](https://docs.spring.io/spring-batch/docs/current/reference/html/spring-batch-integration.html)
has a general overview of how to use Spring Integration for polling files from a directory to process with
Spring Batch. But there are some details that the documentation doesn't clarify. This sample's intention
is to clarify those details.

## Running the app

Run the app by executing the following command from inside the project folder:

```shell
./gradlew bootRun
```

The app will start to poll the `tesst_files/input` directory. Place any `.csv` file in this
directory and it will be processed generating a file in the `tesst_files/output` directory.
The original file will be moved to the `tesst_files/processed` directory.

A sample input file can be found on `test_files/sample`. The csv input file is expected to have
comma separated lines with two fields representing the first and last names of a person.

The output file is a csv where each line represents one person extracted from the
input file but with its first and last names upper-cased.

# Copyright Notice

Copyright (c) 2022 Global Byte - Fábio M. Blanco

## License

Released under [MIT Lisense](https://github.com/fabio-blanco/spring-batch-integration-sample/blob/main/LICENSE).
