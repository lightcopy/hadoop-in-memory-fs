package com.github.sadikovi;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
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

public class InMemoryFileSystem extends FileSystem {
  private String scheme; // scheme that is assigned to this file system
  private URI uri; // full URI that the file system is initialised with
  private Path workingDir; // working directory
  private INode root;

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
    InputStream in = root.open(abs);
    if (in == null) throw new IOException("Cannot open path " + abs);
    return new FSDataInputStream(in);
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
    if (root.create(abs, false, overwrite)) {
      INode node = root.get(abs);
      return new FSDataOutputStream(new ByteArrayOutputStream() {
        @Override
        public void close() throws IOException {
          super.close();
          root.setContent(abs, buf);
        }
      }, null);
    } else {
      throw new IOException("Cannot create path " + abs);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
    return root.remove(abs, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
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
    this.workingDir = dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    // TODO: handle permissions
    Path abs = f.makeQualified(this.uri, this.workingDir);
    return root.create(abs, true, false);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path abs = f.makeQualified(this.uri, this.workingDir);
    INode node = root.get(abs);
    return new FileStatus(
      node.getContentLength(), node.isDir(), 0, 0, node.getModificationTime(), abs);
  }
}
