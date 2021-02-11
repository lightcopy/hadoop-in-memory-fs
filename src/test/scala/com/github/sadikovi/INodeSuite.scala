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

  test("is directory") {
    assert((new INode("dir", true)).isDir)
    assert(!(new INode("dir", false)).isDir)
  }

  test("create a directory") {
    val root = INode.root()
    assert(root.create(new Path("/a/b/c/d"), true, false))
    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(root.get(new Path("/a/b/c")).isDir)
    assert(root.get(new Path("/a/b/c/d")).isDir)

    assert(!root.create(new Path("/a/b/c/d"), true, false))
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
    assert(!root.create(new Path("/"), true, false))
    assert(!root.create(new Path("/"), false, false))
    assert(!root.create(new Path("/"), false, true))
    assert(root.toString === "DIR {}")
  }

  test("create a leaf directory") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c"), true, false))
    assert(root.create(new Path("/a/b/c/d"), true, false))
    assert(root.get(new Path("/a/b/c/d")).isDir)

    // overwriteFile does not matter
    assert(root.create(new Path("/a/b/c2"), true, true))
    assert(root.create(new Path("/a/b/c2/d2"), true, true))
    assert(root.get(new Path("/a/b/c2/d2")).isDir)
  }

  test("create a file") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c"), true, false))
    assert(root.create(new Path("/a/b/c/file"), false, false))
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file that already exists") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c"), true, false))

    assert(root.create(new Path("/a/b/c/file"), false, false))
    assert(!root.get(new Path("/a/b/c/file")).isDir)

    assert(!root.create(new Path("/a/b/c/file"), false, false))
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file with overwriteFile") {
    val root = INode.root
    assert(root.create(new Path("/a/b/c"), true, false))

    assert(root.create(new Path("/a/b/c/file"), false, false))
    assert(!root.get(new Path("/a/b/c/file")).isDir)

    assert(root.create(new Path("/a/b/c/file"), false, true))
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("create a file for the directory path") {
    val root = INode.root
    root.create(new Path("/a/b/c"), true, false)
    assert(!root.create(new Path("/a/b/c"), false, false))
    assert(!root.create(new Path("/a/b/c"), false, true))

    assert(root.get(new Path("/a")).isDir)
    assert(root.get(new Path("/a/b")).isDir)
    assert(root.get(new Path("/a/b/c")).isDir)
  }

  test("create a directory for the file path") {
    val root = INode.root
    root.create(new Path("/a/b/file"), false, false)
    assert(!root.create(new Path("/a/b/file"), true, false))

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
    root.create(new Path("/a/b/c/file"), false, true)
    assert(!root.get(new Path("/a/b/c/file")).isDir)
  }

  test("get beyond path") {
    val root = INode.root
    root.create(new Path("/a/b"), false, true)
    assert(root.get(new Path("/a/b")) != null)
    assert(root.get(new Path("/a/b/c")) == null)
    assert(root.get(new Path("/a/b/c/file")) == null)
  }

  test("remove a root node") {
    val root = INode.root
    assert(!root.remove(new Path("/"), false))
    assert(root.remove(new Path("/"), true)) // Check removal of a root directory recursively
  }

  test("remove a node") {
    val root = INode.root
    root.create(new Path("/a/b/c/x"), true, false)
    root.create(new Path("/a/b/c/y"), true, false)
    root.create(new Path("/a/b/file"), false, false)

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
    root.create(new Path("/a/b/file"), false, false)

    assert(!root.remove(new Path("/a/b/file/c"), true))
    assert(!root.remove(new Path("/a/b/file/c"), false))

    assert(root.get(new Path("/a/b/file")) != null)
  }

  test("list") {
    val root = INode.root
    root.create(new Path("/a/b/x"), true, false)
    root.create(new Path("/a/b/y"), true, false)
    root.create(new Path("/a/b/z"), true, false)
    root.create(new Path("/a/b/_file"), false, false)

    assert(root.list(new Path("/a")).map(_.getName) === Seq("b"))
    assert(root.list(new Path("/a/b")).map(_.getName) === Seq("_file", "x", "y", "z"))
  }

  test("list result sorting order") {
    val root = INode.root
    val items = (0 until 32) // more than initial hash map capacity
    for (i <- items) {
      root.create(new Path("/a/" + i), true, false)
    }
    assert(root.list(new Path("/a")).map(_.getName) === items.map(_.toString).sorted)
  }

  test("list non-existent path") {
    val root = INode.root
    root.create(new Path("/a/b"), true, false)
    assert(root.list(new Path("/a/b/c/d")) == null)
  }

  test("list empty directory") {
    val root = INode.root
    root.create(new Path("/a/b"), true, false)
    assert(root.list(new Path("/a/b")).map(_.getName) === Seq())
  }

  test("list a file") {
    val root = INode.root
    root.create(new Path("/a/b/file"), false, false)
    assert(root.list(new Path("/a/b")).map(_.getName) === Seq("file"))
    assert(root.list(new Path("/a/b/file")).map(_.getName) === Seq("file"))
  }
}
