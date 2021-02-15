package com.github.sadikovi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * In-memory test file system.
 * The file system reuses scheme, host, port, and authority making it possible to use this class
 * to mock S3AFileSystem, AzureBlobFileSystem, or any other distributed file system.
 *
 * For example, initialisaing the file system with "s3a://bucket/path", all subsequent paths are
 * resolved against the URI, including file status, listing, writes, and reads.
 */
public class InMemoryFileSystem extends FileSystem {
  private String scheme; // scheme that is assigned to this file system
  private URI uri; // full URI that the file system is initialised with
  private Path workingDir; // working directory
  private INode root; // working tree

  public InMemoryFileSystem() {
    super();
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.scheme = name.getScheme();
    this.uri = name;
    this.workingDir = new Path("/home").makeQualified(this.uri, null);
    this.root = INode.root();
  }

  @Override
  public String getScheme() {
    return scheme;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
    INode node = root.get(abs);
    if (node == null) throw new FileNotFoundException("" + abs);
    if (node.isDir()) throw new IOException("Cannot open a directory " + abs);
    return new FSDataInputStream(new INodeInputStream(node.getContent()));
  }

  @Override
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
    INode node = root.get(abs);

    if (node != null && (node.isDir() || !overwrite)) {
      throw new IOException("Cannot create path " + abs);
    }

    return new FSDataOutputStream(new ByteArrayOutputStream() {
      @Override
      public void close() throws IOException {
        super.close();
        byte[] out = new byte[count];
        System.arraycopy(buf, 0, out, 0, count);
        if (root.createFile(abs, out, overwrite) == null) {
          throw new IOException("Failed to write to file " + abs);
        }
      }
    }, null);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path from = src.makeQualified(this.uri, this.workingDir);
    Path to = dst.makeQualified(this.uri, this.workingDir);
    return root.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
    return root.remove(abs, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
    INode node = root.get(abs);
    // Path is for a file, return getFileStatus
    if (node != null && !node.isDir()) {
      return new FileStatus[] { getFileStatus(abs) };
    }
    // List the directory
    INode[] nodes = root.list(abs);
    if (nodes == null) return null;
    FileStatus[] res = new FileStatus[nodes.length];
    for (int i = 0; i < res.length; i++) {
      res[i] = new FileStatus(
        nodes[i].getContentLength(),
        nodes[i].isDir(),
        0,
        0,
        nodes[i].getModificationTime(),
        new Path(abs, nodes[i].name));
    }
    return res;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    Path abs = dir.makeQualified(this.uri, this.workingDir);
    this.workingDir = abs;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    // TODO: handle permissions
    Path abs = f.makeQualified(this.uri, this.workingDir);
    return root.create(abs) != null;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
    INode node = root.get(abs);
    if (node == null) throw new FileNotFoundException("" + abs);
    return new FileStatus(
      node.getContentLength(), node.isDir(), 0, 0, node.getModificationTime(), abs);
  }

  @Override
  public String toString() {
    return root.toString();
  }

  /** INode input stream for Hadoop */
  private static class INodeInputStream
      extends ByteArrayInputStream
      implements Seekable, PositionedReadable {

    public INodeInputStream(byte[] array) {
      super(array);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {
      // this method is not thread-safe
      long curr = getPos();
      seek(position);
      try {
        return read(buffer, offset, length);
      } finally {
        seek(curr);
      }
    }


    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      // this method is not thread-safe
      long curr = getPos();
      seek(position);
      try {
        int bytesRead = 0;
        while (bytesRead < length) {
          int bytes = read(buffer, offset + bytesRead, length - bytesRead);
          if (bytes < 0) throw new IOException("Reached EOF");
          bytesRead += bytes;
        }
      } finally {
        seek(curr);
      }
    }

    @Override
    public void seek(long pos) {
      skip(pos);
    }

    @Override
    public long getPos() {
      return this.pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }
  }

  /**
   * Class representing INode tree.
   *
   * This is used to perform direct operations on the tree including atomic creation, deletion,
   * and rename.
   */
  static class INode {
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
      INode tmp = this;
      String[] tokens = tokenize(p);
      for (int i = 0; i < tokens.length; i++) {
        if (tmp == null || !tmp.isDir()) return null;
        tmp = tmp.children.get(tokens[i]);
      }
      return tmp;
    }

    /** Creates a directory */
    public synchronized INode create(Path p) {
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
      INode parent = p.getParent() != null ? create(p.getParent()) : null; // handle root directory
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

    /** List a directory or a file */
    public INode[] list(Path p) {
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

    /** Private method to remove a node */
    private boolean remove(INode target) {
      if (target == null || target.parent == null) return false;
      target.parent.children.remove(target.name);
      target.parent = null;
      return true;
    }

    /** Remove the path */
    public synchronized boolean remove(Path p, boolean recursive) {
      INode target = get(p);
      if (target == null || target.parent == null) return false;
      if (!recursive && target.isDir() && !target.children.isEmpty()) return false;
      return remove(target);
    }

    /** Renames path "from" to "to" */
    public synchronized boolean rename(Path from, Path to) {
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
}
