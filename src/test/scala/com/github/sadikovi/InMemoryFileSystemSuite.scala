package com.github.sadikovi

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

class InMemoryFileSystemSuite extends UnitTestSuite {
  // test("init") {
  //   // val conf = new Configuration()
  //   // conf.set("fs.dbfs.impl", classOf[InMemoryFileSystem].getName)
  //   // val path = new Path("dbfs://bucket/path/to/dir")
  //   // val fs = path.getFileSystem(conf)
  //   //
  //   // assert(fs.getScheme === "dbfs")
  //   // assert(fs.getWorkingDirectory === new Path("dbfs://bucket/home"))
  //   //
  //   // // fs.mkdirs(new Path("blah"));
  //   // println(new Path("/blah").getParent)
  //   // println(new Path("/").getParent)
  //
  //   println(INode.tokenize(new Path("/a/b/c/d")).toList)
  //   println(INode.tokenize(new Path("/a")).toList)
  //   println(INode.tokenize(new Path("/")).toList)
  //   println(INode.tokenize(null))
  //   println(INode.tokenize(new Path("a/b/c")).toList)
  //
  //   val node = new INode(true)
  //   println(node.create(new Path("/a/b/c/d"), true, false))
  //   println(node)
  //   println(node.create(new Path("/a/b/c/d"), false, false))
  //   println(node)
  //   println(node.create(new Path("/a/b/c/e"), false, false))
  //   println(node)
  //   println(node.create(new Path("/a/b/c"), true, false))
  //   println(node)
  //   println(node.create(new Path("/a/b/c"), false, true))
  //   println(node)
  //   println(node.create(new Path("/a/b/c/e"), false, true))
  //   println(node)
  //   println(node.get(new Path("/a/b/c/e/f")));
  // }
}
