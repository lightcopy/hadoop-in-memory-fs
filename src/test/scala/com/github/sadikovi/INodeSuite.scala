package com.github.sadikovi

import org.apache.hadoop.fs._
import com.github.sadikovi.InMemoryFileSystem.INode

class INodeSuite extends UnitTestSuite {
  // Root directory to use in the tests
  private var root: INode = null

  before {
    root = INode.root()
  }

  test("tokenize null path") {
    intercept[AssertionError] {
      INode.tokenize(null)
    }
  }

  test("tokenize relative path") {
    intercept[AssertionError] {
      INode.tokenize(new Path("a/b"))
    }
  }

  test("tokenize root") {
    assert(INode.tokenize(new Path("/")) === Seq())
    assert(INode.tokenize(new Path("/a")) === Seq("a"))
    assert(INode.tokenize(new Path("/a/")) === Seq("a"))
    assert(INode.tokenize(new Path("/a/b")) === Seq("a", "b"))
    assert(INode.tokenize(new Path("/a/b/")) === Seq("a", "b"))
    assert(INode.tokenize(new Path("/a/b/c")) === Seq("a", "b", "c"))
    assert(INode.tokenize(new Path("/a/b/c/")) === Seq("a", "b", "c"))
  }

  test("root is directory") {
    assert(INode.root().isDir)
  }

  test("create a directory") {
    val dir = root.create(new Path("/a/b/c/d"))

    assert(dir != null)
    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(root.get(new Path("/a/b/c")).isDir)
    assert(root.get(new Path("/a/b/c/d")).isDir)

    assert(root.create(new Path("/a/b/c/d")) === dir)
    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(root.get(new Path("/a/b/c")).isDir)
    assert(root.get(new Path("/a/b/c/d")).isDir)

    assert(root.get(new Path("/b")) == null)
    assert(root.get(new Path("/c")) == null)
    assert(root.get(new Path("/d")) == null)
  }

  test("create a root directory") {
    assert(root.create(new Path("/")) == root)
    assert(root.toString === "DIR {}")
  }

  test("create a leaf directory") {
    assert(root.create(new Path("/a/b/c")) != null)
    assert(root.create(new Path("/a/b/c/d")) != null)
    assert(root.get(new Path("/a/b/c/d")).isDir)

    assert(root.create(new Path("/a/b/c2")) != null)
    assert(root.create(new Path("/a/b/c2/d2")) != null)
    assert(root.get(new Path("/a/b/c2/d2")).isDir)
  }

  test("create a file") {
    assert(root.create(new Path("/a/b/c")) != null)
    assert(root.createFile(new Path("/a/b/c/file"), null, false) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file for the root directory") {
    assert(root.createFile(new Path("/"), null, false) == null)
    assert(root.createFile(new Path("/"), null, true) == null)
  }

  test("create a file that already exists") {
    assert(root.create(new Path("/a/b/c")) != null)

    assert(root.createFile(new Path("/a/b/c/file"), null, false) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)

    assert(root.createFile(new Path("/a/b/c/file"), null, false) == null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file with overwrite") {
    assert(root.create(new Path("/a/b/c")) != null)

    assert(root.createFile(new Path("/a/b/c/file"), null, false) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)

    assert(root.createFile(new Path("/a/b/c/file"), null, true) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file for the directory path") {
    root.create(new Path("/a/b/c"))
    assert(root.createFile(new Path("/a/b/c"), null, false) == null)
    assert(root.createFile(new Path("/a/b/c"), null, true) == null)

    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(root.get(new Path("/a/b/c")).isDir)
  }

  test("create a directory for the file path") {
    root.createFile(new Path("/a/b/file"), null, false)
    assert(root.create(new Path("/a/b/file")) == null)
    assert(root.create(new Path("/a/b/file/c")) == null)

    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(!root.get(new Path("/a/b/file")).isDir)
  }

  test("get a node") {
    assert(root.get(new Path("/a")) == null)
  }

  test("get a root node") {
    assert(root.get(new Path("/")) === root)
  }

  test("get a file") {
    root.createFile(new Path("/a/b/c/file"), null, true)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("get beyond path") {
    root.createFile(new Path("/a/b"), null, true)
    assert(root.get(new Path("/a/b")) != null)
    assert(root.get(new Path("/a/b/c")) == null)
    assert(root.get(new Path("/a/b/c/file")) == null)
  }

  test("remove a root node") {
    assert(!root.remove(new Path("/"), false))
    assert(!root.remove(new Path("/"), true)) // Check removal of a root directory recursively
    assert(root.toString === "DIR {}")
  }

  test("remove a node") {
    root.create(new Path("/a/b/c/x"))
    root.create(new Path("/a/b/c/y"))
    root.createFile(new Path("/a/b/file"), null, false)

    assert(root.remove(new Path("/a/b/c/x"), false))
    assert(root.get(new Path("/a/b/c")).isDir)
    assert(root.get(new Path("/a/b/c/x")) == null)

    assert(root.remove(new Path("/a/b/c"), true))
    assert(root.get(new Path("/a/b")).isDir)
    assert(root.get(new Path("/a/b/c")) == null)
    assert(root.get(new Path("/a/b/c/y")) == null)

    assert(root.remove(new Path("/a/b/file"), false))

    assert(!root.remove(new Path("/a"), false))
    assert(root.get(new Path("/a")).isDir)

    assert(root.remove(new Path("/a"), true))
    assert(root.get(new Path("/a")) == null)
  }

  test("remove non-existent path") {
    root.createFile(new Path("/a/b/file"), null, false)

    assert(!root.remove(new Path("/a/b/file/c"), true))
    assert(!root.remove(new Path("/a/b/file/c"), false))

    assert(root.get(new Path("/a/b/file")) != null)
  }

  test("list") {
    root.create(new Path("/a/b/x"))
    root.create(new Path("/a/b/y"))
    root.create(new Path("/a/b/z"))
    root.createFile(new Path("/a/b/_file"), null, false)

    assert(root.list(new Path("/a")).map(_.name) === Seq("b"))
    assert(root.list(new Path("/a/b")).map(_.name) === Seq("_file", "x", "y", "z"))
  }

  test("list result sorting order") {
    val items = (0 until 128) // more than initial hash map capacity
    for (i <- items) {
      root.create(new Path("/a/" + i))
    }
    assert(root.list(new Path("/a")).map(_.name) === items.map(_.toString).sorted)
  }

  test("list non-existent path") {
    root.create(new Path("/a/b"))
    assert(root.list(new Path("/a/b/c/d")) == null)
  }

  test("list empty directory") {
    root.create(new Path("/a/b"))
    assert(root.list(new Path("/a/b")).map(_.name) === Seq())
  }

  test("list a file") {
    root.createFile(new Path("/a/b/file"), null, false)
    assert(root.list(new Path("/a/b")).map(_.name) === Seq("file"))
    assert(root.list(new Path("/a/b/file")).map(_.name) === Seq("file"))
  }

  test("content of an empty file") {
    val node = root.createFile(new Path("/file"), null, false)
    assert(node.getContent() === Seq())
    assert(node.getContentLength() === 0)
  }

  test("content of a directory") {
    val node = root.create(new Path("/dir"))
    assert(node.getContent() == null)
    assert(node.getContentLength() === 0)
  }

  test("content of a non-empty file") {
    val node = root.createFile(new Path("/file"), Array[Byte](1, 2, 3, 4, 5), false)

    val res = root.get(new Path("/file"))
    assert(res === node)
    assert(res.getContent() === Array[Byte](1, 2, 3, 4, 5))
    assert(res.getContentLength() === 5)
  }

  test("rename") {
    assert(!root.rename(new Path("/"), new Path("/file")))
    assert(root.get(new Path("/file")) == null)

    assert(!root.rename(new Path("/"), new Path("/a")))
    assert(root.get(new Path("/a")) == null)

    root.create(new Path("/a"))
    assert(!root.rename(new Path("/a"), new Path("/")))
  }

  test("rename a file") {
    root.createFile(new Path("/a/file"), Array[Byte](1, 2, 3), false)
    assert(root.rename(new Path("/a/file"), new Path("/b/file")))

    assert(root.get(new Path("/a/file")) == null)
    val node = root.get(new Path("/b/file"))
    assert(node != null)
    assert(!node.isDir)
    assert(node.getContent() === Array[Byte](1, 2, 3))
  }

  test("rename a directory") {
    root.create(new Path("/a/b/c"))
    assert(root.rename(new Path("/a"), new Path("/a_new")))

    assert(root.get(new Path("/a/b/c")) == null)
    assert(root.get(new Path("/a_new")).isDir)
    assert(root.get(new Path("/a_new/b")).isDir)
    assert(root.get(new Path("/a_new/b/c")).isDir)
  }

  test("rename to an existing directory") {
    root.create(new Path("/a/b/c"))
    root.create(new Path("/a_new"))
    assert(!root.rename(new Path("/a"), new Path("/a_new")))
    assert(root.list(new Path("/a_new")).isEmpty)
  }

  test("rename to an existing file") {
    root.createFile(new Path("/a/b/file"), null, false)
    root.createFile(new Path("/file"), null, false)

    assert(!root.rename(new Path("/a/b/file"), new Path("/file")))
    assert(!root.get(new Path("/file")).isDir)
    assert(!root.get(new Path("/a/b/file")).isDir)
  }
}
