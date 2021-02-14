package com.github.sadikovi

import org.apache.hadoop.fs._

class INodeSuite extends UnitTestSuite {
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
    assert(INode.root.isDir)
  }

  test("create a directory") {
    val root = INode.root()
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
    val root = INode.root
    assert(root.create(new Path("/")) == root)
    assert(root.toString === "DIR {}")
  }

  test("create a leaf directory") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c")) != null)
    assert(root.create(new Path("/a/b/c/d")) != null)
    assert(root.get(new Path("/a/b/c/d")).isDir)

    assert(root.create(new Path("/a/b/c2")) != null)
    assert(root.create(new Path("/a/b/c2/d2")) != null)
    assert(root.get(new Path("/a/b/c2/d2")).isDir)
  }

  test("create a file") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c")) != null)
    assert(root.createFile(new Path("/a/b/c/file"), null, false) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file for the root directory") {
    val root = INode.root
    assert(root.createFile(new Path("/"), null, false) == null)
    assert(root.createFile(new Path("/"), null, true) == null)
  }

  test("create a file that already exists") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c")) != null)

    assert(root.createFile(new Path("/a/b/c/file"), null, false) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)

    assert(root.createFile(new Path("/a/b/c/file"), null, false) == null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file with overwrite") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c")) != null)

    assert(root.createFile(new Path("/a/b/c/file"), null, false) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)

    assert(root.createFile(new Path("/a/b/c/file"), null, true) != null)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file for the directory path") {
    val root = INode.root
    root.create(new Path("/a/b/c"))
    assert(root.createFile(new Path("/a/b/c"), null, false) == null)
    assert(root.createFile(new Path("/a/b/c"), null, true) == null)

    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(root.get(new Path("/a/b/c")).isDir)
  }

  test("create a directory for the file path") {
    val root = INode.root
    root.createFile(new Path("/a/b/file"), null, false)
    assert(root.create(new Path("/a/b/file")) == null)
    assert(root.create(new Path("/a/b/file/c")) == null)

    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(!root.get(new Path("/a/b/file")).isDir)
  }

  test("get a node") {
    val root = INode.root
    assert(root.get(new Path("/a")) == null)
  }

  test("get a root node") {
    val root = INode.root
    assert(root.get(new Path("/")) === root)
  }

  test("get a file") {
    val root = INode.root
    root.createFile(new Path("/a/b/c/file"), null, true)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("get beyond path") {
    val root = INode.root
    root.createFile(new Path("/a/b"), null, true)
    assert(root.get(new Path("/a/b")) != null)
    assert(root.get(new Path("/a/b/c")) == null)
    assert(root.get(new Path("/a/b/c/file")) == null)
  }

  test("remove a root node") {
    val root = INode.root
    assert(!root.remove(new Path("/"), false))
    assert(!root.remove(new Path("/"), true)) // Check removal of a root directory recursively
    assert(root.toString === "DIR {}")
  }

  test("remove a node") {
    val root = INode.root
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
    val root = INode.root
    root.createFile(new Path("/a/b/file"), null, false)

    assert(!root.remove(new Path("/a/b/file/c"), true))
    assert(!root.remove(new Path("/a/b/file/c"), false))

    assert(root.get(new Path("/a/b/file")) != null)
  }

  test("list") {
    val root = INode.root
    root.create(new Path("/a/b/x"))
    root.create(new Path("/a/b/y"))
    root.create(new Path("/a/b/z"))
    root.createFile(new Path("/a/b/_file"), null, false)

    assert(root.list(new Path("/a")).map(_.getName) === Seq("b"))
    assert(root.list(new Path("/a/b")).map(_.getName) === Seq("_file", "x", "y", "z"))
  }

  test("list result sorting order") {
    val root = INode.root
    val items = (0 until 128) // more than initial hash map capacity
    for (i <- items) {
      root.create(new Path("/a/" + i))
    }
    assert(root.list(new Path("/a")).map(_.getName) === items.map(_.toString).sorted)
  }

  test("list non-existent path") {
    val root = INode.root
    root.create(new Path("/a/b"))
    assert(root.list(new Path("/a/b/c/d")) == null)
  }

  test("list empty directory") {
    val root = INode.root
    root.create(new Path("/a/b"))
    assert(root.list(new Path("/a/b")).map(_.getName) === Seq())
  }

  test("list a file") {
    val root = INode.root
    root.createFile(new Path("/a/b/file"), null, false)
    assert(root.list(new Path("/a/b")).map(_.getName) === Seq("file"))
    assert(root.list(new Path("/a/b/file")).map(_.getName) === Seq("file"))
  }

  test("content of an empty file") {
    val root = INode.root
    val node = root.createFile(new Path("/file"), null, false)
    assert(node.getContent() === Seq())
    assert(node.getContentLength() === 0)
  }

  test("content of a directory") {
    val root = INode.root
    val node = root.create(new Path("/dir"))
    assert(node.getContent() == null)
    assert(node.getContentLength() === 0)
  }

  test("content of a non-empty file") {
    val root = INode.root
    val node = root.createFile(new Path("/file"), Array[Byte](1, 2, 3, 4, 5), false)

    val res = root.get(new Path("/file"))
    assert(res === node)
    assert(res.getContent() === Array[Byte](1, 2, 3, 4, 5))
    assert(res.getContentLength() === 5)
  }
}
