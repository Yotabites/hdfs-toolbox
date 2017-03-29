# README #

Hadoop distributed file system (HDFS) is designed to store very large data sets reliably, and to stream those data sets at high bandwidth to user applications. Files are stored in a redundant fashion across multiple machines to ensure their durability to failure and high availability to very parallel applications.

We face a lot of problems when we try to do some file operations in HDFS. For example, an external application like Hive or Impala produces data in compressed format and other applications are not able to access the compressed data. How do we usually overcome this? One way is to run a map-reduce to compress and decompress. Otherwise, transfer the files to local file system and decompress the files and again copy into HDFS. Isn’t it too much burden?

This toolbox contains the solutions for the problems which we faced in our everyday programming life.

An overview of the toolbox

* Copy into HDFS with overwrite option
* Merging multiple files into one
* Compress/Decompress a file/directory - BZip2, GZip and Snappy

### How do I get set up? ###

This is an SBT project. These commands would be useful to set it up.

**sbt clean	** Removes all generated files from the target directory.

**sbt compile** Compiles source code files that are in src/main/scala, src/main/java, and the root directory of the project.

**sbt assembly** Creates a fat JAR of your project with all of its dependencies.

**sbt package** Creates a JAR file (or WAR file for web projects) containing the files in src/main/scala, src/main/java, and resources in src/main/resources.

### Copy into HDFS ###

It copies nested directories from local file system to HDFS and skips if the files are already existing. 
```
#!script

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar WriteToHDFS /local/file/dir /hdfs/file/dir
```

To overwrite the old files, use -f option.


```
#!script

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar WriteToHDFS -f /local/file/dir /hdfs/file/dir
```

If the local files to be deleted immediately after copying into HDFS, use -d option.

```
#!script

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar WriteToHDFS -f -d /local/file/dir /hdfs/file/dir
```

For the custom replication factor, use -repl option.

```
#!script

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar WriteToHDFS -repl=1 -d /local/file/dir /hdfs/file/dir
```

### File Concatenation ###

It appends the content of all files in a directory into a single file. 


```
#!script

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar LinkFiles /hdfs/input/dir
```

If the output needs to be written in a different directory,


```
#!script

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar LinkFiles /hdfs/input/dir /hdfs/output/dir
```

### Compress Decompress ###

Compresses or decompresses a file or a directory. It accepts compress/decompress, source location, target location and optional compression type. The default compression type is BZip2.


```
#!script

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar HDFSComp -c /src/hdfs/path /target/hdfs/path/

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar HDFSComp -d /compressed/hdfs/dir /target/hdfs/dir/
```

For other compression types like GZip and Snappy, specify the type.


```
#!script
hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar HDFSComp -c /src/hdfs/path /target/hdfs/path/ snap

hadoop jar target/scala-2.11/hdfs-toolbox-assembly-1.0.jar HDFSComp -d /compressed/hdfs/dir /target/hdfs/dir/ gzip

```

### Contribution guidelines ###

Please feel free to give your suggestions and ideas to develop this toolbox.
