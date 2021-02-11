package com.github.sadikovi;

import java.util.HashMap;

import org.apache.hadoop.fs.Path;

public class INode {
  // If this is null, it is a file, if it is empty it is a dir
  private final HashMap<String, INode> children;

  // Private constructor
  INode(boolean isDir) {
    this.children = isDir ? new HashMap<String, INode>() : null;
  }

  /** Returns a root node */
  public static INode root() {
    return new INode(true);
  }

  /** Returns true if the node is a directory */
  public boolean isDir() {
    return children != null;
  }

  /** Returns a valid node for path or null if no such node exists */
  public INode get(Path p) {
    assertValidPath(p);
    INode tmp = this;
    String[] tokens = tokenize(p);
    for (int i = 0; i < tokens.length; i++) {
      if (tmp == null || !tmp.isDir()) return null;
      tmp = tmp.children.get(tokens[i]);
    }
    return tmp;
  }

  /** Create path, directory or a file */
  public boolean create(Path p, boolean isDir, boolean overwriteFile) {
    assertValidPath(p);
    INode tmp = this;
    String[] tokens = tokenize(p);
    for (int i = 0; i < tokens.length - 1; i++) {
      INode node = tmp.children.get(tokens[i]);
      if (node == null) {
        node = new INode(true);
        tmp.children.put(tokens[i], node);
      }
      if (!node.isDir()) return false;
      tmp = node;
    }
    // Handle the special case of the root directory
    if (tokens.length == 0) return false;
    // Handle general case
    String token = tokens[tokens.length - 1];
    INode node = tmp.children.get(token);
    if (node == null || !node.isDir() && !isDir && overwriteFile) {
      node = new INode(isDir);
      tmp.children.put(token, node);
      return true;
    }
    return false;
  }

  /** Remove the path */
  public boolean remove(Path p, boolean recursive) {
    assertValidPath(p);
    String[] tokens = tokenize(p);
    // Handle the special case of removing a root directory
    if (tokens.length == 0) {
      if (!recursive) return false; // do nothing even if it is empty
      this.children.clear(); // if recursive, remove all of the children
      return true;
    }
    // Find the parent
    INode parent = get(p.getParent());
    // Remove the target from the parent
    if (parent == null || !parent.isDir()) return false;
    INode target = parent.children.get(p.getName());
    if (recursive || !target.isDir() || target.children.isEmpty()) {
      parent.children.remove(p.getName());
      return true;
    }
    return false;
  }

  /** Converts path /a/b/c into tokens ["a", "b", "c"] */
  static String[] tokenize(Path p) {
    assertValidPath(p);
    String[] tokens = new String[p.depth()];
    for (int i = tokens.length - 1; i >= 0; i--) {
      tokens[i] = p.getName();
      p = p.getParent();
    }
    return tokens;
  }

  /** Asserts if the path is valid */
  private static void assertValidPath(Path p) {
    if (p == null) throw new AssertionError("Path is null");
    if (!p.isAbsolute()) throw new AssertionError("Path " + p + " is not absolute");
  }

  @Override
  public String toString() {
    return isDir() ? ("DIR " + children) : "FILE";
  }
}
