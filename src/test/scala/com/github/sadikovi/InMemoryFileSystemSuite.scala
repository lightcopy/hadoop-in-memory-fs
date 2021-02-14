package com.github.sadikovi

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

class InMemoryFileSystemSuite extends UnitTestSuite {
  private def getFileSystem(): FileSystem = {
    val conf = new Configuration()
    conf.set("fs.dbfs.impl", classOf[InMemoryFileSystem].getName)
    conf.set("fs.dbfs.impl.disable.cache", "true")
    val path = new Path("dbfs://bucket/path/to/dir")
    path.getFileSystem(conf)
  }

  test("init") {
    val fs = getFileSystem()
    assert(fs.getScheme === "dbfs")
    assert(fs.getWorkingDirectory === new Path("dbfs://bucket/home"))
  }

  test("working directory") {
    val fs = getFileSystem()
    fs.setWorkingDirectory(new Path("/my/working/dir"))
    assert(fs.getWorkingDirectory === new Path("dbfs://bucket/my/working/dir"))
  }

  test("get status") {
    val fs = getFileSystem()
    fs.mkdirs(new Path("/a/b/c"))
    fs.create(new Path("/a/b/file")).close()

    var status = fs.getFileStatus(new Path("/a/b/c"))
    assert(status.isDirectory)
    assert(status.getPath === new Path("dbfs://bucket/a/b/c"))

    status = fs.getFileStatus(new Path("/a/b/file"))
    assert(!status.isDirectory)
    assert(status.getPath === new Path("dbfs://bucket/a/b/file"))
  }

  test("list a directory") {
    val fs = getFileSystem()
    fs.mkdirs(new Path("/a/1"))
    fs.mkdirs(new Path("/a/2"))
    fs.mkdirs(new Path("/a/3"))

    val res = fs.listStatus(new Path("/a"))
    for (i <- 0 until res.length) {
      assert(res(i).isDirectory)
      assert(res(i).getPath === new Path("dbfs://bucket/a/" + (i + 1)))
    }
  }

  test("list a file") {
    val fs = getFileSystem()
    fs.createNewFile(new Path("/a/b/file"))
    val res1 = fs.listStatus(new Path("/a/b"))
    assert(res1.length === 1)
    assert(!res1.head.isDirectory)
    assert(res1.head.getPath === new Path("dbfs://bucket/a/b/file"))

    val res2 = fs.listStatus(new Path("/a/b/file"))
    assert(res2.length === 1)
    assert(!res2.head.isDirectory)
    assert(res2.head.getPath === new Path("dbfs://bucket/a/b/file"))
  }

  test("write and read a file") {
    val fs = getFileSystem()
    val out = fs.create(new Path("/a/b/file"))
    out.writeUTF("Hello, world!")
    out.close

    val in = fs.open(new Path("/a/b/file"))
    val res = in.readUTF()
    assert(res === "Hello, world!")
  }

  test("create with overwrite") {
    val fs = getFileSystem()
    val out1 = fs.create(new Path("/file"))
    out1.writeUTF("data1");

    val out2 = fs.create(new Path("/file"))
    out2.writeUTF("data2");

    out2.close
    out1.close

    val in = fs.open(new Path("/file"))
    val res = in.readUTF()
    assert(res === "data1") // the last close() wrote data
  }

  test("create file on close") {
    val fs = getFileSystem()
    val out = fs.create(new Path("/file"))
    assert(!fs.exists(new Path("/file")))
    out.close
    assert(fs.exists(new Path("/file")))
  }

  test("create empty file") {
    val fs = getFileSystem()
    fs.createNewFile(new Path("/file"))
    assert(fs.getFileStatus(new Path("/file")).getLen() === 0)
  }

  test("read a file") {
    val fs = getFileSystem()
    val out = fs.create(new Path("/file"))
    out.writeUTF("Hello, world!")
    out.close

    val in = fs.open(new Path("/file"))
    val res = in.readUTF()
    assert(res === "Hello, world!")
  }

  test("rename") {
    val fs = getFileSystem()
    fs.mkdirs(new Path("/a/1"))
    fs.mkdirs(new Path("/a/2"))
    fs.createNewFile(new Path("/a/file"))
    fs.mkdirs(new Path("/a/b/1"))
    fs.mkdirs(new Path("/a/b/2"))
    fs.createNewFile(new Path("/a/b/file"))

    assert(fs.rename(new Path("/a"), new Path("/a_new")))

    assert(!fs.exists(new Path("/a/1")))
    assert(!fs.exists(new Path("/a/2")))
    assert(!fs.exists(new Path("/a/file")))
    assert(!fs.exists(new Path("/a/b/1")))
    assert(!fs.exists(new Path("/a/b/2")))
    assert(!fs.exists(new Path("/a/b/file")))

    assert(fs.exists(new Path("/a_new/1")))
    assert(fs.exists(new Path("/a_new/2")))
    assert(fs.exists(new Path("/a_new/file")))
    assert(fs.exists(new Path("/a_new/b/1")))
    assert(fs.exists(new Path("/a_new/b/2")))
    assert(fs.exists(new Path("/a_new/b/file")))
  }

  test("mkdirs") {
    val fs = getFileSystem()
    assert(fs.mkdirs(new Path("/")))

    assert(fs.mkdirs(new Path("/a/b")))
    assert(fs.getFileStatus(new Path("/a")).isDirectory)
    assert(fs.getFileStatus(new Path("/a/b")).isDirectory)

    // Repeated creation should succeed
    assert(fs.mkdirs(new Path("/a/b")))
    assert(fs.getFileStatus(new Path("/a")).isDirectory)
    assert(fs.getFileStatus(new Path("/a/b")).isDirectory)

    fs.createNewFile(new Path("/a/b/file"))
    assert(!fs.mkdirs(new Path("/a/b/file/c")))
  }
}
