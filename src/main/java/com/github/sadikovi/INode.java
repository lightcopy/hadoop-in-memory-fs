package com.github.sadikovi;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;

public class INode {
  // Name of the INode, represents the directory or file name
  private final String name;
  // If this is null, it is a file; if it is empty, it is a directory
  private final HashMap<String, INode> children;
  // Content for a file, set to null for a directory
  private byte[] content;

  // Default comparator to sort inodes in lexicographical order
  private static final Comparator<INode> DEFAULT_CMP = new Comparator<INode>() {
    @Override
    public int compare(INode left, INode right) {
      return left.getName().compareTo(right.getName());
    }
  };

  // Private constructor
  INode(String name, boolean isDir) {
    this.name = name;
    this.children = isDir ? new HashMap<String, INode>() : null;
  }

  /** Returns a root node */
  public static INode root() {
    return new INode("root", true);
  }

  /** Returns node name (token) */
  public String getName() {
    return name;
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

  /** List a directory or a file */
  public INode[] list(Path p) {
    assertValidPath(p);
    INode node = get(p);
    if (node == null) return null;
    if (node.isDir()) {
      INode[] res = new INode[node.children.size()];
      int i = 0;
      for (INode child : node.children.values()) {
        res[i++] = child;
      }
      // Sort explicitly for now
      Arrays.sort(res, DEFAULT_CMP);
      return res;
    } else {
      // Listing of a file is the file itself
      return new INode[] { node };
    }
  }

  /** Create path, directory or a file */
  public boolean create(Path p, boolean isDir, boolean overwriteFile) {
    assertValidPath(p);
    INode tmp = this;
    String[] tokens = tokenize(p);
    for (int i = 0; i < tokens.length - 1; i++) {
      INode node = tmp.children.get(tokens[i]);
      if (node == null) {
        node = new INode(tokens[i], true);
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
      node = new INode(token, isDir);
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

  /** Open a file */
  public InputStream open(Path p) {
    assertValidPath(p);
    INode node = get(p);
    if (node == null || node.isDir()) return null;
    return new ByteArrayInputStream(node.content == null ? new byte[0] : node.content);
  }

  /** Set content for a file */
  public void setContent(Path p, byte[] content) {
    assertValidPath(p);
    INode node = get(p);
    if (node == null || node.isDir()) return;
    node.content = content;
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
