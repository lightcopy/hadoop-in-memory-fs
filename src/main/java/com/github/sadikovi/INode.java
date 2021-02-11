package com.github.sadikovi;

import java.util.HashMap;

import org.apache.hadoop.fs.Path;

public class INode {
  HashMap<String, INode> children; // if this is null, it is a file, if it is empty it is a dir

  INode(boolean isDir) {
    this.children = isDir ? new HashMap<String, INode>() : null;
  }

  public boolean isDir() {
    return children != null;
  }

  public INode get(Path p) {
    INode tmp = this;
    String[] tokens = tokenize(p);
    for (int i = 0; tokens != null && i < tokens.length; i++) {
      if (!tmp.isDir()) return null;
      tmp = tmp.children.get(tokens[i]);
      if (tmp == null) return null;
    }
    return tokens == null ? null : tmp;
  }

  public boolean create(Path p, boolean isDir, boolean overwriteFile) {
    INode tmp = this;
    String[] tokens = tokenize(p);
    for (int i = 0; tokens != null && i < tokens.length - 1; i++) {
      INode node = tmp.children.get(tokens[i]);
      if (node == null) {
        node = new INode(true);
        tmp.children.put(tokens[i], node);
      }
      if (!node.isDir()) return false;
      tmp = node;
    }

    if (tokens == null || tokens.length == 0) return false;

    INode node = tmp.children.get(tokens[tokens.length - 1]);
    if (node == null || !node.isDir() && !isDir && overwriteFile) {
      node = new INode(isDir);
      tmp.children.put(tokens[tokens.length - 1], node);
      return true;
    }
    return false;
  }

  public boolean remove(Path p, boolean recursive) {
    return false;
  }

  public static String[] tokenize(Path p) {
    if (p == null) return null;
    String[] tokens = new String[p.depth()];
    for (int i = tokens.length - 1; i >= 0; i--) {
      tokens[i] = p.getName();
      p = p.getParent();
    }
    return tokens;
  }

  @Override
  public String toString() {
    return "node " + children;
  }
}
