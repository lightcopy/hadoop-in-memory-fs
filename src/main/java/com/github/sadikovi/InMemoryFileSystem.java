package com.github.sadikovi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;

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
        new Path(abs, nodes[i].getName()));
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
}
