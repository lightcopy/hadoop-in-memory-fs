package com.github.sadikovi;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;

public class INode {
  // Name of the INode, represents the directory or file name
  final String name;
  // Parent pointer, can be null for the root directory
  private INode parent;
  // If this is null, it is a file; if it is empty, it is a directory
  private HashMap<String, INode> children;
  // Modification time for the node
  private long modificationTime;
  // Content for a file, set to null for a directory
  private byte[] content;

  // Default comparator to sort inodes in lexicographical order
  private static final Comparator<INode> DEFAULT_CMP = new Comparator<INode>() {
    @Override
    public int compare(INode left, INode right) {
      return left.name.compareTo(right.name);
    }
  };

  /** Creates the root of the tree */
  public static INode root() {
    return new INode("root", true, null);
  }

  // Private constructor
  private INode(String name, boolean isDir, byte[] content) {
    if (name == null || name.isEmpty()) throw new AssertionError("Name is invalid: " + name);
    this.name = name;
    this.parent = null;
    this.children = isDir ? new HashMap<String, INode>() : null;
    this.modificationTime = System.currentTimeMillis();
    this.content = isDir ? null : (content == null ? new byte[0] : content);
  }

  /** Returns true if the node is a directory */
  public boolean isDir() {
    return children != null;
  }

  /** Returns size for the content in bytes */
  public long getContentLength() {
    return (isDir() || content == null) ? 0 : content.length;
  }

  /** Returns content of the file */
  public byte[] getContent() {
    return isDir() ? null : content;
  }

  /** Returns modification time */
  public long getModificationTime() {
    return modificationTime;
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

  /** Creates a directory */
  public synchronized INode create(Path p) {
    assertValidPath(p);
    INode tmp = this;
    String[] tokens = tokenize(p);
    for (int i = 0; i < tokens.length; i++) {
      INode node = tmp.children.get(tokens[i]);
      if (node == null) {
        node = new INode(tokens[i], true, null);
        tmp.children.put(tokens[i], node);
        node.parent = tmp;
      }
      if (!node.isDir()) return null;
      tmp = node;
    }
    return tmp;
  }

  /** Creates a new file */
  public synchronized INode createFile(Path p, byte[] content, boolean overwrite) {
    assertValidPath(p);
    if (p.getParent() == null) return null; // handle root directory
    INode parent = create(p.getParent());
    if (parent == null) return null; // handle files on the path
    INode node = parent.children.get(p.getName());
    if (node == null || !node.isDir() && overwrite) {
      node = new INode(p.getName(), false, content);
      parent.children.put(p.getName(), node);
      node.parent = parent;
      return node;
    }
    return null;
  }

  /** Private method to remove a node */
  private boolean remove(INode target) {
    if (target == null || target.parent == null) return false;
    target.parent.children.remove(target.name);
    target.parent = null;
    return true;
  }

  /** Remove the path */
  public synchronized boolean remove(Path p, boolean recursive) {
    assertValidPath(p);
    INode target = get(p);
    if (target == null || target.parent == null) return false;
    if (!recursive && target.isDir() && !target.children.isEmpty()) return false;
    return remove(target);
  }

  /** Renames path "from" to "to" */
  public synchronized boolean rename(Path from, Path to) {
    assertValidPath(from);
    assertValidPath(to);
    INode src = get(from), dst = get(to);
    if (src == null || src.parent == null || dst != null) return false;
    // Try to create destination
    dst = src.isDir() ? create(to) : createFile(to, src.content, false);
    if (dst == null) return false;
    // Swap the properties
    dst.children = src.children;
    dst.content = src.content;
    remove(src);
    return true;
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
    return isDir() ? ("DIR " + children) : ("FILE (" + getContentLength() + ")");
  }
}
