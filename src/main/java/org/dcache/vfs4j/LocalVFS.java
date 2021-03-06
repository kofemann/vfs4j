package org.dcache.vfs4j;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import jdk.incubator.foreign.CLinker;
import jdk.incubator.foreign.FunctionDescriptor;
import jdk.incubator.foreign.LibraryLookup;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemorySegment;
import jnr.ffi.byref.LongLongByReference;
import jnr.ffi.provider.FFIProvider;
import jnr.constants.platform.Errno;
import jnr.ffi.Address;
import jnr.ffi.annotations.*;
import org.dcache.nfs.status.*;

import org.dcache.nfs.util.UnixSubjects;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

/** A file system which implementation with is backed-up with a local file system. */
public class LocalVFS implements VirtualFileSystem {

  private final Logger LOG = LoggerFactory.getLogger(LocalVFS.class);

  private static final String XATTR_PREFIX = "user.";

  // stolen from /usr/include/bits/fcntl-linux.h
  private static final int O_DIRECTORY = 0200000;
  private static final int O_RDONLY = 00;
  private static final int O_WRONLY = 01;
  private static final int O_RDWR = 02;

  private static final int O_PATH = 010000000;
  private static final int O_NOFOLLOW = 0400000;
  private static final int O_EXCL = 0200;
  private static final int O_CREAT = 0100;

  private static final int AT_SYMLINK_NOFOLLOW = 0x100;
  private static final int AT_REMOVEDIR = 0x200;
  private static final int AT_EMPTY_PATH = 0x1000;

  protected final SysVfs sysVfs;
  private final jnr.ffi.Runtime runtime;

  private final KernelFileHandle rootFh;
  protected final int rootFd;

  private final NfsIdMapping idMapper = new SimpleIdMap();

  /** Cache of opened files used by read/write operations. */
  private final LoadingCache<Inode, SystemFd> _openFilesCache;

  // handles to native functions;
  private final MethodHandle fStrerror;
  private final MethodHandle fOpen;
  private final MethodHandle fOpenAt;
  private final MethodHandle fClose;
  private final MethodHandle fNameToHandleAt;
  private final MethodHandle fOpenByHandleAt;
  private final MethodHandle fSync;
  private final MethodHandle fDataSync;

  private final VarHandle err

  public LocalVFS(File root) throws IOException {

    sysVfs = FFIProvider.getSystemProvider().createLibraryLoader(SysVfs.class).load("c");
    runtime = jnr.ffi.Runtime.getRuntime(sysVfs);

    fStrerror = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("strerror").get().address(),
                    MethodType.methodType(MemoryAddress.class, int.class),
                    FunctionDescriptor.of(CLinker.C_POINTER, CLinker.C_INT)
            );

    fOpen = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("open").get().address(),
                    MethodType.methodType(int.class, MemoryAddress.class, int.class, int.class),
                    FunctionDescriptor.of(CLinker.C_INT, CLinker.C_POINTER, CLinker.C_INT, CLinker.C_INT)
            );

    fOpenAt = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("openat").get().address(),
                    MethodType.methodType(int.class, int.class,  MemoryAddress.class, int.class, int.class),
                    FunctionDescriptor.of(CLinker.C_INT, CLinker.C_INT, CLinker.C_POINTER, CLinker.C_INT, CLinker.C_INT)
            );

    fClose = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("close").get().address(),
                    MethodType.methodType(int.class, int.class),
                    FunctionDescriptor.of(CLinker.C_INT, CLinker.C_INT)
            );

    fNameToHandleAt = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("name_to_handle_at").get().address(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, MemoryAddress.class, MemoryAddress.class, int.class),
                    FunctionDescriptor.of(CLinker.C_INT, CLinker.C_INT, CLinker.C_POINTER, CLinker.C_POINTER, CLinker.C_POINTER, CLinker.C_INT)
            );

    fOpenByHandleAt = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("open_by_handle_at").get().address(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class),
                    FunctionDescriptor.of(CLinker.C_INT, CLinker.C_INT, CLinker.C_POINTER, CLinker.C_INT)
            );

    fDataSync = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("fdatasync").get().address(),
                    MethodType.methodType(int.class, int.class),
                    FunctionDescriptor.of(CLinker.C_INT, CLinker.C_INT)
            );

    fSync = CLinker.getInstance()
            .downcallHandle(
                    LibraryLookup.ofDefault().lookup("fsync").get().address(),
                    MethodType.methodType(int.class, int.class),
                    FunctionDescriptor.of(CLinker.C_INT, CLinker.C_INT)
            );

    rootFd = open(root.getAbsolutePath(), O_DIRECTORY, O_RDONLY);
    checkError(rootFd >= 0);

    rootFh = path2fh(rootFd, "", AT_EMPTY_PATH);

    _openFilesCache =
        CacheBuilder.newBuilder()
            .maximumSize(1024)
            .removalListener(new FileCloser())
            .build(new FileOpenner());
  }

  @Override
  public int access(Inode inode, int mode) throws IOException {
    // pseudofs will do the checks
    return mode;
  }

  @Override
  public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode)
      throws IOException {
    int uid = (int) UnixSubjects.getUid(subject);
    int gid = (int) UnixSubjects.getPrimaryGid(subject);

    if (type == Stat.Type.DIRECTORY) {
      throw new NotSuppException("create of this type not supported");
    }

    try (SystemFd fd = inode2fd(parent, O_NOFOLLOW | O_DIRECTORY)) {

      int rc;
      if (type == Stat.Type.REGULAR) {
        int rfd = openat(fd.fd(), path, O_EXCL | O_CREAT | O_RDWR, mode);
        checkError(rfd >= 0);
        rc = sysVfs.fchownat(rfd, "", uid, gid, AT_EMPTY_PATH);
        checkError(rc == 0);

        Inode inode = path2fh(rfd, "", AT_EMPTY_PATH).toInode();
        _openFilesCache.put(inode, new SystemFd(rfd));
        return inode;

      } else {

        // FIXME: we should get major and minor numbers from CREATE arguments.
        // dev == (long)major << 32 | minor
        LongLongByReference dev = new LongLongByReference(type == Stat.Type.BLOCK || type == Stat.Type.CHAR ? 1 : 0);
        rc = sysVfs.__xmknodat(0, fd.fd(), path,  mode | type.toMode(), dev);
        checkError(rc >= 0);
        rc = sysVfs.fchownat(fd.fd(), path, uid, gid, 0);
        checkError(rc >= 0);
        return path2fh(fd.fd(), path, 0).toInode();
      }
    }
  }

  @Override
  public FsStat getFsStat() throws IOException {
    StatFs statFs = new StatFs(runtime);
    int rc = sysVfs.fstatfs(rootFd, statFs);
    checkError(rc == 0);

    return new FsStat(
        statFs.f_blocks.get() * statFs.f_bsize.get(),
        statFs.f_files.get(),
        (statFs.f_blocks.get() - statFs.f_bfree.get()) * statFs.f_bsize.get(),
        statFs.f_files.get() - statFs.f_ffree.get());
  }

  @Override
  public Inode getRootInode() throws IOException {
    return rootFh.toInode();
  }

  @Override
  public Inode lookup(Inode parent, String path) throws IOException {
    try (SystemFd fd = inode2fd(parent, O_DIRECTORY | O_PATH)) {
      return path2fh(fd.fd(), path, 0).toInode();
    }
  }

  @Override
  public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
    try (SystemFd dirFd = inode2fd(parent, O_NOFOLLOW | O_DIRECTORY)) {
      try (SystemFd inodeFd = inode2fd(link, O_NOFOLLOW)) {
        int rc = sysVfs.linkat(inodeFd.fd(), "", dirFd.fd(), path, AT_EMPTY_PATH);
        checkError(rc == 0);
        return lookup(parent, path);
      }
    }
  }

  @Override
  public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {

    TreeSet<DirectoryEntry> list = new TreeSet<>();
    try (SystemFd fd = inode2fd(inode, O_DIRECTORY)) {
      Address p = sysVfs.fdopendir(fd.fd());
      checkError(p != null);

      sysVfs.seekdir(p, cookie);

      while (true) {
        Dirent dirent = sysVfs.readdir(p);

        if (dirent == null) {
          break;
        }

        String name = dirent.d_name.get();
        Inode fInode = path2fh(fd.fd(), name, 0).toInode();
        Stat stat = getattr(fInode);
        list.add(new DirectoryEntry(name, fInode, stat, dirent.d_off.longValue()));
      }
    }
    return new DirectoryStream(DirectoryStream.ZERO_VERIFIER, list);
  }

  @Override
  public byte[] directoryVerifier(Inode inode) throws IOException {
    return DirectoryStream.ZERO_VERIFIER;
  }

  @Override
  public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
    int uid = (int) UnixSubjects.getUid(subject);
    int gid = (int) UnixSubjects.getPrimaryGid(subject);

    Inode inode;
    try (SystemFd fd = inode2fd(parent, O_PATH | O_NOFOLLOW | O_DIRECTORY)) {
      int rc = sysVfs.mkdirat(fd.fd(), path, mode);
      checkError(rc == 0);
      inode = lookup(parent, path);
      try (SystemFd fd1 = inode2fd(inode, O_NOFOLLOW | O_DIRECTORY)) {
        rc = sysVfs.fchownat(fd1.fd(), "", uid, gid, AT_EMPTY_PATH);
        checkError(rc == 0);
      }
      return inode;
    }
  }

  @Override
  public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
    try (SystemFd fd1 = inode2fd(src, O_NOFOLLOW | O_DIRECTORY)) {
      try (SystemFd fd2 = inode2fd(dest, O_NOFOLLOW | O_DIRECTORY)) {
        int rc = sysVfs.renameat(fd1.fd(), oldName, fd2.fd(), newName);
        checkError(rc == 0);
        return true;
      }
    }
  }

  @Override
  public Inode parentOf(Inode inode) throws IOException {
    // avoid lookup behind the exported root
    if (Arrays.equals(inode.getFileId(), rootFh.toBytes())) {
      throw new NoEntException();
    }
    return lookup(inode, "..");
  }

  @Override
  public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
    return read(inode, ByteBuffer.wrap(data, 0, count), offset);
  }

  @Override
  public int read(Inode inode, ByteBuffer data, long offset) throws IOException {
    SystemFd fd = getOfLoadRawFd(inode);
    int n = sysVfs.pread(fd.fd(), data, data.remaining(), offset);
    checkError(n >= 0);
    // JNI interface does not updates the position
    data.position(data.position() + n);
    return n;
  }

  @Override
  public String readlink(Inode inode) throws IOException {
    try (SystemFd fd = inode2fd(inode, O_PATH | O_NOFOLLOW)) {
      Stat stat = statByFd(fd);
      byte[] buf = new byte[(int) stat.getSize()];
      int rc = sysVfs.readlinkat(fd.fd(), "", buf, buf.length);
      checkError(rc >= 0);
      return new String(buf, UTF_8);
    }
  }

  @Override
  public void remove(Inode parent, String path) throws IOException {
    try (SystemFd fd = inode2fd(parent, O_PATH | O_DIRECTORY)) {
      Inode inode = lookup(parent, path);
      Stat stat = getattr(inode);
      int flags = stat.type() == Stat.Type.DIRECTORY ? AT_REMOVEDIR : 0;
      int rc = sysVfs.unlinkat(fd.fd(), path, flags);
      checkError(rc == 0);
    }
  }

  @Override
  public Inode symlink(Inode parent, String path, String link, Subject subject, int mode)
      throws IOException {
    int uid = (int) UnixSubjects.getUid(subject);
    int gid = (int) UnixSubjects.getPrimaryGid(subject);

    try (SystemFd fd = inode2fd(parent, O_DIRECTORY)) {
      int rc = sysVfs.symlinkat(link, fd.fd(), path);
      checkError(rc == 0);
      Inode inode = lookup(parent, path);
      Stat stat = new Stat();
      stat.setUid(uid);
      stat.setGid(gid);
      try (SystemFd fd1 = inode2fd(inode, O_PATH)) {
        rc = sysVfs.fchownat(fd1.fd(), "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
        checkError(rc == 0);
      }
      return inode;
    }
  }

  @Override
  public WriteResult write(
      Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel)
      throws IOException {
    return write(inode, ByteBuffer.wrap(data, 0, count), offset, stabilityLevel);
  }

  @Override
  public WriteResult write(Inode inode, ByteBuffer data, long offset, StabilityLevel stabilityLevel)
      throws IOException {
    SystemFd fd = getOfLoadRawFd(inode);
    int n = sysVfs.pwrite(fd.fd(), data, data.remaining(), offset);
    checkError(n >= 0);

    // JNI interface does not updates the position
    data.position(data.position() + n);

    int rc = 0;
    switch (stabilityLevel) {
      case UNSTABLE:
        // NOP
        break;
      case DATA_SYNC:
        try {
          rc = (int)fDataSync.invokeExact(fd.fd());
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
        break;
      case FILE_SYNC:
        try {
          rc = (int)fSync.invokeExact(fd.fd());
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }

        break;
      default:
        throw new RuntimeException("bad sync type");
    }
    checkError(rc == 0);
    return new WriteResult(stabilityLevel, n);
  }

  @Override
  public void commit(Inode inode, long offset, int count) throws IOException {
    // NOP
  }

  @Override
  public Stat getattr(Inode inode) throws IOException {
    try (SystemFd fd = inode2fd(inode, O_PATH)) {
      return statByFd(fd);
    }
  }

  @Override
  public void setattr(Inode inode, Stat stat) throws IOException {

    int openMode = O_RDONLY;

    Stat currentStat = getattr(inode);
    if (currentStat.type() == Stat.Type.SYMLINK) {
      if (stat.isDefined(Stat.StatAttribute.SIZE)) {
        throw new InvalException("Can't change size of a symlink");
      }
      openMode = O_PATH | O_RDWR | O_NOFOLLOW;
    }

    if (stat.isDefined(Stat.StatAttribute.SIZE)) {
      openMode |= O_RDWR;
    }

    try (SystemFd fd = inode2fd(inode, openMode)) {
      int uid = -1;
      int gid = -1;
      int rc;

      if (stat.isDefined(Stat.StatAttribute.OWNER)) {
        uid = stat.getUid();
      }

      if (stat.isDefined(Stat.StatAttribute.GROUP)) {
        gid = stat.getGid();
      }

      if (uid != -1 || gid != -1) {
        rc = sysVfs.fchownat(fd.fd(), "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
        checkError(rc == 0);
      }

      if (currentStat.type() != Stat.Type.SYMLINK) {
        if (stat.isDefined(Stat.StatAttribute.MODE)) {
          rc = sysVfs.fchmod(fd.fd(), stat.getMode());
          checkError(rc == 0);
        }
      }

      if (stat.isDefined(Stat.StatAttribute.SIZE)) {
        rc = sysVfs.ftruncate(fd.fd(), stat.getSize());
        checkError(rc == 0);
      }
    }
  }

  @Override
  public nfsace4[] getAcl(Inode inode) throws IOException {
    return new nfsace4[0];
  }

  @Override
  public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
    // NOP
  }

  @Override
  public boolean hasIOLayout(Inode inode) throws IOException {
    return false;
  }

  @Override
  public AclCheckable getAclCheckable() {
    return AclCheckable.UNDEFINED_ALL;
  }

  @Override
  public NfsIdMapping getIdMapper() {
    return idMapper;
  }

  @Override
  public boolean getCaseInsensitive() {
    return true;
  }

  @Override
  public boolean getCasePreserving() {
    return true;
  }

  private int findByte(byte[] buf, byte b, int offset) {

    for (int i = offset; i < buf.length; i++) {
      if (buf[i] == b) {
        return i;
      }
    }
    return -1;
  }

  /*
   * only user attribute are supported. Adjust the name is required.
   */
  private final String toXattrName(String s) {
    return s.startsWith(XATTR_PREFIX) ? s : XATTR_PREFIX + s;
  }

  @Override
  public String[] listXattrs(Inode inode) throws IOException {
    byte[] out = new byte[1024];
    List<String> list = new ArrayList<>();
    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW)) {

      int rc = sysVfs.flistxattr(fd.fd(), out, out.length);
      checkError(rc >= 0);

      for (int i = 0; i < rc; ) {
        int index = findByte(out, (byte) '\0', i);
        if (index < 0) {
          break;
        }
        list.add(new String(out, i, index - i, UTF_8));
        i = index + 1;
      }
      return list.toArray(String[]::new);
    }
  }

  @Override
  public byte[] getXattr(Inode inode, String name) throws IOException {
    byte[] out = new byte[64 * 1024];

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW)) {

      int rc = sysVfs.fgetxattr(fd.fd(), toXattrName(name), out, out.length);
      checkError(rc >= 0);
      return Arrays.copyOf(out, rc);
    }
  }

  @Override
  public void setXattr(Inode inode, String attr, byte[] value, SetXattrMode mode)
      throws IOException {

    int flag = switch (mode) {
      case EITHER -> 0;
      case CREATE -> 1;
      case REPLACE -> 2;
    };

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW)) {
      int rc = sysVfs.fsetxattr(fd.fd(), toXattrName(attr), value, value.length, flag);
      checkError(rc == 0);
    }
  }

  @Override
  public void removeXattr(Inode inode, String attr) throws IOException {
    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW)) {
      int rc = sysVfs.fremovexattr(fd.fd(), toXattrName(attr));
      checkError(rc == 0);
    }
  }

  @Override
  public long copyFileRange(Inode src, long srcPos, Inode dst, long dstPos, long len)
      throws IOException {

    SystemFd fdIn = getOfLoadRawFd(src);
    SystemFd fdDst = getOfLoadRawFd(dst);


    int rc = sysVfs.copy_file_range(fdIn.fd(), new LongLongByReference(srcPos),
            fdDst.fd(), new LongLongByReference(dstPos), len, 0);
    checkError(rc >= 0);
    return rc;
  }

  /**
   * Lookup file handle by path
   *
   * @param fd parent directory open file descriptor
   * @param path path within a directory
   * @param flags ...
   * @return file handle
   * @throws IOException
   */
  private KernelFileHandle path2fh(int fd, String path, int flags) throws IOException {

    try(MemorySegment str = CLinker.toCString(path);
        MemorySegment bytes = MemorySegment.allocateNative(KernelFileHandle.MAX_HANDLE_SZ);
        MemorySegment mntId = MemorySegment.allocateNative(Integer.BYTES)){

      bytes.asByteBuffer().order(ByteOrder.nativeOrder()).putInt(0, (int)bytes.byteSize());
      int rc;
      try {
        rc = (int)fNameToHandleAt.invokeExact(fd, str.address(), bytes.address(), mntId.address(), flags);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }

      checkError(rc == 0);
      KernelFileHandle fh = new KernelFileHandle(bytes.toByteArray());
      LOG.debug("path = [{}], handle = {}", path, fh);
      return fh;
    }
  }

  protected void checkError(boolean condition) throws IOException {

    if (condition) {
      return;
    }

    int errno = runtime.getLastError();
    Errno e = Errno.valueOf(errno);
    String msg = strerror(errno) + " " + e.name() + "(" + errno + ")";
    LOG.debug("Last error: {}", msg);

    // FIXME: currently we assume that only xattr related calls return ENODATA
    switch (e) {
      case ENOENT -> throw new NoEntException(msg);
      case ENOTDIR -> throw new NotDirException(msg);
      case EISDIR -> throw new IsDirException(msg);
      case EIO -> throw new NfsIoException(msg);
      case ENOTEMPTY -> throw new NotEmptyException(msg);
      case EEXIST -> throw new ExistException(msg);
      case ESTALE -> throw new StaleException(msg);
      case EINVAL -> throw new InvalException(msg);
      case ENOTSUP -> throw new NotSuppException(msg);
      case ENXIO -> throw new NXioException(msg);
      case ENODATA -> throw new NoXattrException(msg);
      default -> {
        IOException t = new ServerFaultException(msg);
        LOG.error("unhandled exception ", t);
        throw t;
      }
    }
  }

  private SystemFd inode2fd(Inode inode, int flags) throws IOException {
    KernelFileHandle fh = new KernelFileHandle(inode);
    int fd;
    byte[] fhBytes = fh.toBytes();
    try (MemorySegment rawHandle = MemorySegment.allocateNative(fhBytes.length)){
      rawHandle.asByteBuffer().put(fhBytes);
      fd = (int)fOpenByHandleAt.invokeExact(rootFd, rawHandle.address(), flags);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    checkError(fd >= 0);
    return new SystemFd(fd);
  }

  private Stat statByFd(SystemFd fd) throws IOException {
    FileStat stat = new FileStat(runtime);
    int rc = sysVfs.__fxstat64(0, fd.fd(), stat);
    checkError(rc == 0);
    return toVfsStat(stat);
  }

  private Stat toVfsStat(FileStat fileStat) {
    Stat vfsStat = new Stat();

    vfsStat.setATime(fileStat.st_atime.get() * 1000);
    vfsStat.setCTime(fileStat.st_ctime.get() * 1000);
    vfsStat.setMTime(fileStat.st_mtime.get() * 1000);

    vfsStat.setGid(fileStat.st_gid.get());
    vfsStat.setUid(fileStat.st_uid.get());
    vfsStat.setDev(fileStat.st_dev.intValue());
    vfsStat.setIno(fileStat.st_ino.intValue());
    vfsStat.setMode(fileStat.st_mode.get());
    vfsStat.setNlink(fileStat.st_nlink.intValue());
    vfsStat.setRdev(fileStat.st_rdev.intValue());
    vfsStat.setSize(fileStat.st_size.get());
    vfsStat.setFileid(fileStat.st_ino.get());
    vfsStat.setGeneration(Math.max(fileStat.st_ctime.get(), fileStat.st_mtime.get()));

    return vfsStat;
  }

  private class FileCloser implements RemovalListener<Inode, SystemFd> {

    @Override
    public void onRemoval(RemovalNotification<Inode, SystemFd> notification) {
      close(notification.getValue().fd());
    }
  }

  private class FileOpenner extends CacheLoader<Inode, SystemFd> {

    @Override
    public SystemFd load(@Nonnull Inode key) throws Exception {
      return inode2fd(key, O_NOFOLLOW | O_RDWR);
    }
  }

  private SystemFd getOfLoadRawFd(Inode inode) throws IOException {
    try {
      return _openFilesCache.get(inode);
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new IOException(e.getMessage(), t);
    }
  }

  @SuppressWarnings("PublicInnerClass")
  public interface SysVfs {

    int ioctl(int fd, int request, @Out @In byte[] fh);

    int __fxstat64(int version, int fd, @Transient @Out FileStat fileStat);

    Address fdopendir(int fd);

    void seekdir(@In Address dirp, long offset);

    Dirent readdir(@In @Out Address dirp);

    int readlinkat(int fd, CharSequence path, @Out byte[] buf, int len);

    int unlinkat(int fd, CharSequence path, int flags);

    int mkdirat(int fd, CharSequence path, int mode);

    int fchownat(int fd, CharSequence path, int uid, int gid, int flags);

    int fchmod(int fd, int mode);

    int ftruncate(int fildes, long length);

    int pread(int fd, @Out ByteBuffer buf, int nbyte, long offset);

    int pwrite(int fd, @In ByteBuffer buf, int nbyte, long offset);

    int fstatfs(int fd, @Out StatFs statfs);

    int renameat(int oldfd, CharSequence oldPath, int newfd, CharSequence newPath);

    int symlinkat(CharSequence target, int newdirfd, CharSequence linkpath);

    int linkat(int fd1, CharSequence path1, int fd2, CharSequence path2, int flag);

    int flistxattr(int fd, @Out byte[] buf, int size);

    int fgetxattr(int fd, CharSequence name, @Out byte[] buf, int size);

    int fsetxattr(int fd, CharSequence name, @In byte[] buf, int size, int flags);

    int fremovexattr(int fd, CharSequence name);

    int __xmknodat(int version, int fd, CharSequence name, int mode, @In @Out LongLongByReference dev);

    int copy_file_range(int fd_in, @In @Out LongLongByReference off_in,
                        int fd_out, @In @Out LongLongByReference off_out, long count, int flags);
  }


  // FFI helpers
  private String strerror(int errno) {
    try {
    MemoryAddress o = (MemoryAddress) fStrerror.invokeExact(errno);
    return CLinker.toJavaStringRestricted(o);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private int open(String name, int flags, int mode) {
    try(MemorySegment str = CLinker.toCString(name)){
      return (int)fOpen.invokeExact(str.address(), flags, mode);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private int openat(int fd, String name, int flags, int mode) {
    try(MemorySegment str = CLinker.toCString(name)){
      return (int)fOpenAt.invokeExact(fd, str.address(), flags, mode);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private int close(int fd) {
    try{
      return (int)fClose.invokeExact(fd);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /** {@link AutoCloseable} class which represents OS native file descriptor. */
  private class SystemFd implements Closeable {

    private final int fd;

    SystemFd(int fd) {
      this.fd = fd;
    }

    int fd() {
      return fd;
    }

    @Override
    public void close() throws IOException {
      int rc = LocalVFS.this.close(fd);
      checkError(rc == 0);
    }
  }
}
