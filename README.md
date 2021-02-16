# hadoop-in-memory-fs

Hadoop in-memory file system for usage in unit and integration tests.

It resembles semantics of cloud-based file systems and can be used as a replacement in tests.

The
file system uses the components from the URI, e.g. scheme, authority, host, and port,
that is passed when the file system is initialised making it easy to test S3, ABFS, or WASB paths.

```scala
val conf = new Configuration()
conf.set("fs.s3a.impl", classOf[InMemoryFileSystem].getName)

val p = new Path("s3a://bucket/path")
val fs = path.getFileSystem(conf)

// All paths will be resolved against "s3a://bucket"
// For example, the code below would return "s3a://bucket/a/b/file"
fs.getFileStatus(new Path("/a/b/file")).getPath
```

You can just copy [InMemoryFileSystem.java](./src/main/java/com/github/sadikovi/InMemoryFileSystem.java),
it only depends on `hadoop-common`.

TODO: set up a maven dependency for download.

## Build
If you want to compile the code, run `sbt build`.

## Test
If you want to run unit tests, run `sbt test`.
