## MapReduce Example
This is a hadoop MapReduce example program. It reads two CSV files and combines them to JSON.

#### Requirements (recommended and tested)
    - Oracle JDK 11.0.1 (LTS)
    - Hadoop 2.9.2
    - Gradle 4.10.2
    - Fedora 29 (other unix systems should also be good)

#### How to build
To build the application you will need JDK-11. To build for local run execute following command from the root directory:

    ./gradlew clean build

To build minimal jar:

    ./gradlew clean jar

To build shadow/large jar to run in hadoop cluster

    ./gradlew shadowJar

#### How to open project with IntelliJ Idea
    ./gradlew cleanIdea
    ./gradlew idea

#### Program Arguments
Make separate folders for companies CSV files and accounts CSV files. ***Beware, the output folder will be deleted if already exists.***

    - arg0 : Folder path of the companies CSV files
    - arg1 : Folder path of the accounts CSV files
    - arg2 : Folder path of the output