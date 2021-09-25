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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import jdk.incubator.foreign.CLinker;
import jdk.incubator.foreign.FunctionDescriptor;
import jdk.incubator.foreign.GroupLayout;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.SymbolLookup;
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
import static jdk.incubator.foreign.CLinker.*;

/** A file system which implementation with is backed-up with a local file system. */
public class LocalVFS implements VirtualFileSystem {

  private final Logger LOG = LoggerFactory.getLogger(LocalVFS.class);

  // max buffer size to cache
  private final static int MAX_CACHE = 1024*1024;

  // max file name
  private final static int MAX_NAME_LEN = 256;

  // directIO ByteByffer cache
  private static final ThreadLocal<ByteBuffer> BUFFERS = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(MAX_CACHE));

  private static final String XATTR_PREFIX = "user.";

  // stolen from /usr/include/bits/fcntl-linux.h
  private static final int O_DIRECTORY = 0200000;
  private static final int O_RDONLY = 00;
  private static final int O_WRONLY = 01;
  private static final int O_RDWR = 02;
  private static final int O_NOACCESS = 00000003;

  private static final int O_PATH = 010000000;
  private static final int O_NOFOLLOW = 0400000;
  private static final int O_EXCL = 0200;
  private static final int O_CREAT = 0100;

  private static final int AT_SYMLINK_NOFOLLOW = 0x100;
  private static final int AT_REMOVEDIR = 0x200;
  private static final int AT_EMPTY_PATH = 0x1000;

  private final KernelFileHandle rootFh;
  protected final int rootFd;

  private final NfsIdMapping idMapper = new SimpleIdMap();

  /** Cache of opened files used by read/write operations. */
  private final LoadingCache<Inode, SystemFd> _openFilesCache;

  // struct stat layout
  public static final GroupLayout STAT_LAYOUT = MemoryLayout.structLayout(

          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_dev"), /* Device. */
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_ino"), /* File serial number.	*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_nlink"), /* Object link count.	*/
          MemoryLayout.valueLayout(32, ByteOrder.nativeOrder()).withName("st_mode"), /* File mode.	*/
          MemoryLayout.valueLayout(32, ByteOrder.nativeOrder()).withName("st_uid"), /* User ID of the file's owner. */
          MemoryLayout.valueLayout(32, ByteOrder.nativeOrder()).withName("st_gid"), /* Group ID of the file's owner.*/
          MemoryLayout.paddingLayout(32), /* unused */
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_rdev"), /* Device number, if device.*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_size"), /* File's size in bytes.*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_blksize"), /* Optimal block size for IO.*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_blocks"), /* Number of 512-byte blocks allocated/ */
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_atime"), /* Time of last access (time_t) .*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_atimensec"), /* Time of last access (nannoseconds).*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_mtime"), /* Last data modification time (time_t).*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_mtimensec"), /* Last data modification time (nanoseconds).*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_ctime"), /* Time of last status change (time_t).*/
          MemoryLayout.valueLayout(64, ByteOrder.nativeOrder()).withName("st_ctimensec"), /* Time of last status change (nanoseconds).*/
          MemoryLayout.paddingLayout(64), /* unused */
          MemoryLayout.paddingLayout(64), /* unused */
          MemoryLayout.paddingLayout(64) /* unused */
  );

  private static final MemoryLayout STAT_FS_LAYOUT = MemoryLayout.structLayout(
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("type"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("bsize"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("block"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("free"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("bavail"),

          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("files"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("ffree"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("fsid"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("namelen"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("frsize"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("flags"),
          MemoryLayout.sequenceLayout(6, MemoryLayout.valueLayout(Integer.SIZE, ByteOrder.nativeOrder())).withName("spare")
  );

  private static final MemoryLayout DIRENT_LAYOUT = MemoryLayout.structLayout(
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("ino"),
          MemoryLayout.valueLayout(Long.SIZE, ByteOrder.nativeOrder()).withName("off"),
          MemoryLayout.valueLayout(Short.SIZE, ByteOrder.nativeOrder()).withName("reclen"),
          MemoryLayout.valueLayout(Byte.SIZE, ByteOrder.nativeOrder()).withName("type"),
          MemoryLayout.sequenceLayout(MAX_NAME_LEN, MemoryLayout.valueLayout(Byte.SIZE, ByteOrder.nativeOrder())).withName("name")
  );

  // handles to native functions;
  private static final MethodHandle fStrerror;
  private static final MethodHandle fOpen;
  private static final MethodHandle fOpenAt;
  private static final MethodHandle fClose;
  private static final MethodHandle fNameToHandleAt;
  private static final MethodHandle fOpenByHandleAt;
  private static final MethodHandle fSync;
  private static final MethodHandle fDataSync;
  private static final MethodHandle fStatAt;
  private static final MethodHandle fStatFs;
  private static final MethodHandle fUnlinkAt;
  private static final MethodHandle fOpendir;
  private static final MethodHandle fReaddir;
  private static final MethodHandle fSeekdir;
  private static final MethodHandle fPread;
  private static final MethodHandle fSymlinkAt;
  private static final MethodHandle fRenameAt;
  private static final MethodHandle fReadlinkAt;
  private static final MethodHandle fChownAt;
  private static final MethodHandle fMkdirAt;
  private static final MethodHandle fChmod;
  private static final MethodHandle fFtruncate;
  private static final MethodHandle fLinkAt;
  private static final MethodHandle fPwrite;
  private static final MethodHandle fCopyFileRange;
  private static final MethodHandle fListxattr;
  private static final MethodHandle fGetxattr;
  private static final MethodHandle fSetxattr;
  private static final MethodHandle fRemovexattr;
  private static final MethodHandle fMknodeAt;
  private static final MethodHandle fIoctl;

  private static final MethodHandle fErrono;

  private static final CLinker C_LINKER = CLinker.getInstance();
  private static final SymbolLookup LOOKUP = CLinker.systemLookup();

  static {

    // magic function that return pointer to errno variable
    fErrono = C_LINKER.downcallHandle(
                    LOOKUP.lookup("__errno_location").get(),
                    MethodType.methodType(MemoryAddress.class),
                    FunctionDescriptor.of(C_POINTER)
            );

    fStrerror = C_LINKER.downcallHandle(
                    LOOKUP.lookup("strerror").get(),
                    MethodType.methodType(MemoryAddress.class, int.class),
                    FunctionDescriptor.of(C_POINTER, C_INT)
            );

    fOpen = C_LINKER.downcallHandle(
                    LOOKUP.lookup("open").get(),
                    MethodType.methodType(int.class, MemoryAddress.class, int.class, int.class),
                    FunctionDescriptor.of(C_INT, C_POINTER, C_INT, C_INT)
            );

    fOpenAt = C_LINKER.downcallHandle(
                    LOOKUP.lookup("openat").get(),
                    MethodType.methodType(int.class, int.class,  MemoryAddress.class, int.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT, C_INT)
            );

    fClose = C_LINKER.downcallHandle(
                    LOOKUP.lookup("close").get(),
                    MethodType.methodType(int.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT)
            );

    fNameToHandleAt = C_LINKER.downcallHandle(
                    LOOKUP.lookup("name_to_handle_at").get(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, MemoryAddress.class, MemoryAddress.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_POINTER, C_POINTER, C_INT)
            );

    fOpenByHandleAt = C_LINKER.downcallHandle(
                    LOOKUP.lookup("open_by_handle_at").get(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT)
            );

    fDataSync = C_LINKER.downcallHandle(
                    LOOKUP.lookup("fdatasync").get(),
                    MethodType.methodType(int.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT)
            );

    fSync = C_LINKER.downcallHandle(
                    LOOKUP.lookup("fsync").get(),
                    MethodType.methodType(int.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT)
            );

    fStatAt = C_LINKER.downcallHandle(
                    LOOKUP.lookup("fstatat").get(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, MemoryAddress.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_POINTER, C_INT)
            );

    fStatFs = C_LINKER.downcallHandle(
                    LOOKUP.lookup("fstatfs").get(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER)
            );

    fUnlinkAt = C_LINKER.downcallHandle(
                    LOOKUP.lookup("unlinkat").get(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT)
            );

    fOpendir = C_LINKER.downcallHandle(
                    LOOKUP.lookup("fdopendir").get(),
                    MethodType.methodType(MemoryAddress.class, int.class),
                    FunctionDescriptor.of(C_POINTER, C_INT)
            );

    fReaddir = C_LINKER.downcallHandle(
                    LOOKUP.lookup("readdir").get(),
                    MethodType.methodType(MemoryAddress.class, MemoryAddress.class),
                    FunctionDescriptor.of(C_POINTER, C_POINTER)
            );

    fSeekdir = C_LINKER.downcallHandle(
                    LOOKUP.lookup("seekdir").get(),
                    MethodType.methodType(void.class, MemoryAddress.class, long.class),
                    FunctionDescriptor.ofVoid(C_POINTER, CLinker.C_LONG)
            );

    fPread = C_LINKER.downcallHandle(
                    LOOKUP.lookup("pread").get(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class, long.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT, CLinker.C_LONG)
            );

    fPwrite = C_LINKER.downcallHandle(
            LOOKUP.lookup("pwrite").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class, long.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT, C_LONG_LONG)
    );

    fSymlinkAt = C_LINKER.downcallHandle(
                    LOOKUP.lookup("symlinkat").get(),
                    MethodType.methodType(int.class, MemoryAddress.class, int.class, MemoryAddress.class),
                    FunctionDescriptor.of(C_INT, C_POINTER, C_INT, C_POINTER)
            );

    fRenameAt = C_LINKER.downcallHandle(
                    LOOKUP.lookup("renameat").get(),
                    MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class, MemoryAddress.class),
                    FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT, C_POINTER)
            );

    fReadlinkAt = C_LINKER.downcallHandle(
            LOOKUP.lookup("readlinkat").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, MemoryAddress.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_POINTER, C_INT)
    );

    fChownAt = C_LINKER.downcallHandle(
            LOOKUP.lookup("fchownat").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class, int.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT, C_INT, C_INT)
    );

    fMkdirAt = C_LINKER.downcallHandle(
            LOOKUP.lookup("mkdirat").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT)
    );

    fChmod = C_LINKER.downcallHandle(
            LOOKUP.lookup("fchmod").get(),
            MethodType.methodType(int.class, int.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_INT)
    );

    fFtruncate = C_LINKER.downcallHandle(
            LOOKUP.lookup("ftruncate").get(),
            MethodType.methodType(int.class, int.class, long.class),
            FunctionDescriptor.of(C_INT, C_INT, C_LONG_LONG)
    );

    fLinkAt = C_LINKER.downcallHandle(
            LOOKUP.lookup("linkat").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class, MemoryAddress.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT, C_POINTER, C_INT)
    );

    fCopyFileRange = C_LINKER.downcallHandle(
            LOOKUP.lookup("copy_file_range").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class, MemoryAddress.class, long.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT, C_POINTER, C_LONG_LONG, C_INT)
    );

    fListxattr  = C_LINKER.downcallHandle(
            LOOKUP.lookup("flistxattr").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_INT)
    );

    fGetxattr  = C_LINKER.downcallHandle(
            LOOKUP.lookup("fgetxattr").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, MemoryAddress.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_POINTER, C_INT)
    );

    fSetxattr  = C_LINKER.downcallHandle(
            LOOKUP.lookup("fsetxattr").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class, MemoryAddress.class, int.class, int.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER, C_POINTER, C_INT, C_INT)
    );

    fRemovexattr  = C_LINKER.downcallHandle(
            LOOKUP.lookup("fremovexattr").get(),
            MethodType.methodType(int.class, int.class, MemoryAddress.class),
            FunctionDescriptor.of(C_INT, C_INT, C_POINTER)
    );

    fMknodeAt = C_LINKER.downcallHandle(
            LOOKUP.lookup("__xmknodat").get(),
            MethodType.methodType(int.class, int.class, int.class, MemoryAddress.class, int.class, MemoryAddress.class),
            FunctionDescriptor.of(C_INT, C_INT, C_INT, C_POINTER, C_INT, C_POINTER)
    );

    fIoctl  = C_LINKER.downcallHandle(
            LOOKUP.lookup("ioctl").get(),
            MethodType.methodType(int.class, int.class, int.class, MemoryAddress.class),
            FunctionDescriptor.of(C_INT, C_INT, C_INT, C_POINTER)
    );
  }

  public LocalVFS(File root) throws IOException {

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

    try (SystemFd fd = inode2fd(parent, O_NOFOLLOW | O_DIRECTORY); var scope = ResourceScope.newConfinedScope()) {

      var emptyString = CLinker.toCString("", scope);
      var pathRaw = CLinker.toCString(path, scope);

      int rc;
      if (type == Stat.Type.REGULAR) {
        int rfd =  (int)fOpenAt.invokeExact(fd.fd(), pathRaw.address(), O_EXCL | O_CREAT | O_RDWR, mode);
        checkError(rfd >= 0);
        rc = (int) fChownAt.invokeExact(rfd, emptyString.address(), uid, gid, AT_EMPTY_PATH);
        checkError(rc == 0);

        Inode inode = path2fh(rfd, "", AT_EMPTY_PATH).toInode();
        _openFilesCache.put(inode, new SystemFd(rfd));
        return inode;

      } else {

        // FIXME: we should get major and minor numbers from CREATE arguments.
        // dev == (long)major << 32 | minor
        var dev = MemorySegment.allocateNative(Long.BYTES, scope).asByteBuffer();
        dev.putLong(0, type == Stat.Type.BLOCK || type == Stat.Type.CHAR ? 1 : 0);

        rc = (int)fMknodeAt.invokeExact(0, fd.fd(), pathRaw.address(),  mode | type.toMode(), dev);
        checkError(rc >= 0);
        rc = (int) fChownAt.invokeExact(fd.fd(), pathRaw.address(), uid, gid, 0);
        checkError(rc >= 0);
        return path2fh(fd.fd(), path, 0).toInode();
      }
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public FsStat getFsStat() throws IOException {

    try(var scope = ResourceScope.newConfinedScope()) {

      MemorySegment rawStatFS = MemorySegment.allocateNative(STAT_FS_LAYOUT, scope);
      int rc = (int)fStatFs.invokeExact(rootFd, rawStatFS.address());
      checkError(rc == 0);

      ByteBuffer bb = rawStatFS.asByteBuffer().order(ByteOrder.nativeOrder());

      long f_type = bb.getLong();
      long  f_bsize = bb.getLong();
      long f_blocks = bb.getLong();
      long f_bfree = bb.getLong();
      long f_bavail = bb.getLong();
      long f_files = bb.getLong();
      long f_ffree = bb.getLong();
      long f_fsid = bb.getLong();
      long f_namelen = bb.getLong();
      long f_frsize = bb.getLong();
      long f_flags = bb.getLong();

      return new FsStat(
          f_blocks * f_bsize,
          f_files,
          (f_blocks - f_bfree) * f_bsize,
          f_files - f_ffree);

    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public Inode getRootInode() throws IOException {
    return rootFh.toInode();
  }

  @Override
  public Inode lookup(Inode parent, String path) throws IOException {
    try (SystemFd fd = inode2fd(parent, O_PATH | O_NOACCESS )) {
      return path2fh(fd.fd(), path, 0).toInode();
    }
  }

  @Override
  public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
    try (SystemFd dirFd = inode2fd(parent, O_NOFOLLOW | O_DIRECTORY);
         SystemFd inodeFd = inode2fd(link, O_NOFOLLOW); var scope = ResourceScope.newConfinedScope()) {

      var emptyString = CLinker.toCString("", scope);
      var pathRaw = CLinker.toCString(path, scope);

      int rc = (int) fLinkAt.invokeExact(inodeFd.fd(), emptyString.address(), dirFd.fd(), pathRaw.address(), AT_EMPTY_PATH);

      checkError(rc == 0);
      return lookup(parent, path);
    }  catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {

    TreeSet<DirectoryEntry> list = new TreeSet<>();
    try (SystemFd fd = inode2fd(inode, O_DIRECTORY); var scope = ResourceScope.newConfinedScope()) {

      MemoryAddress p = (MemoryAddress) fOpendir.invokeExact(fd.fd());
      checkError(p != MemoryAddress.NULL);

      fSeekdir.invokeExact(p, cookie);

      while (true) {
        MemoryAddress dirent = (MemoryAddress) fReaddir.invokeExact(p);
        if (dirent == MemoryAddress.NULL) {
          break;
        }

        var rawDirent = dirent.asSegment(DIRENT_LAYOUT.byteSize(), scope);
        ByteBuffer bb = rawDirent.asByteBuffer().order(ByteOrder.nativeOrder());

        long ino = bb.getLong();
        long off = bb.getLong();
        int reclen = bb.getShort();
        byte type = bb.get();

        String name = CLinker.toJavaString(rawDirent.asSlice(8+8+2+1));

        Inode fInode = path2fh(fd.fd(), name, 0).toInode();
        Stat stat = getattr(fInode);
        list.add(new DirectoryEntry(name, fInode, stat, off));
      }
      return new DirectoryStream(DirectoryStream.ZERO_VERIFIER, list);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
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
    try (SystemFd fd = inode2fd(parent, O_PATH | O_NOFOLLOW | O_DIRECTORY); var scope = ResourceScope.newConfinedScope()) {

      var pathRaw = CLinker.toCString(path, scope);
      var emptyString = CLinker.toCString("", scope);

      int rc = (int) fMkdirAt.invokeExact(fd.fd(), pathRaw.address(), mode);
      checkError(rc == 0);
      inode = lookup(parent, path);
      try (SystemFd fd1 = inode2fd(inode, O_NOFOLLOW | O_DIRECTORY)) {
        rc = (int) fChownAt.invokeExact(fd1.fd(), emptyString.address(), uid, gid, AT_EMPTY_PATH);
        checkError(rc == 0);
        return inode;
      }
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
    try (SystemFd fd1 = inode2fd(src, O_PATH | O_NOACCESS );
         SystemFd fd2 = inode2fd(dest, O_PATH | O_NOACCESS);
         var scope = ResourceScope.newConfinedScope()) {

      MemorySegment newNameRaw = CLinker.toCString(newName, scope);
      MemorySegment oldNameRaw = CLinker.toCString(oldName, scope);

        int rc = (int) fRenameAt.invokeExact(fd1.fd(), oldNameRaw.address(), fd2.fd(), newNameRaw.address());
        checkError(rc == 0);
        return true;
      } catch (Throwable t) {
        Throwables.throwIfInstanceOf(t, IOException.class);
        throw new RuntimeException(t);
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

    ByteBuffer bb = data;
    if (!data.isDirect()) {
      bb = data.remaining() > MAX_CACHE ? ByteBuffer.allocateDirect(data.remaining()) : BUFFERS.get();
      bb.clear().limit(data.remaining());
    }

    try (var scope = ResourceScope.newConfinedScope()) {
        MemorySegment rawData = MemorySegment.ofByteBuffer(bb);

        int n = (int)fPread.invokeExact(fd.fd(), rawData.address(), data.remaining(), offset);
        checkError(n >=0);
        // JNI interface does not updates the position
        bb.position(data.position() + n);
        if (!data.isDirect()) {
          bb.flip();
          data.put(bb);
        }
        return n;
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public String readlink(Inode inode) throws IOException {
    try (SystemFd fd = inode2fd(inode, O_PATH | O_NOFOLLOW);
        var scope = ResourceScope.newConfinedScope()) {

      MemorySegment emptyString = CLinker.toCString("", scope);
      Stat stat = statByFd(fd);
      var link = MemorySegment.allocateNative(stat.getSize(), scope);

      int rc = (int) fReadlinkAt.invokeExact(fd.fd(), emptyString.address(), link.address(), (int)stat.getSize());
      checkError(rc >= 0);

      return CLinker.toJavaString(link);

    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void remove(Inode parent, String path) throws IOException {
    try (SystemFd fd = inode2fd(parent, O_PATH | O_DIRECTORY); var scope = ResourceScope.newConfinedScope()) {

      MemorySegment pathRaw = CLinker.toCString(path, scope);
      Inode inode = lookup(parent, path);
      Stat stat = getattr(inode);
      int flags = stat.type() == Stat.Type.DIRECTORY ? AT_REMOVEDIR : 0;
      int rc = (int)fUnlinkAt.invokeExact(fd.fd(), pathRaw.address(), flags);
      checkError(rc == 0);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public Inode symlink(Inode parent, String path, String link, Subject subject, int mode)
      throws IOException {
    int uid = (int) UnixSubjects.getUid(subject);
    int gid = (int) UnixSubjects.getPrimaryGid(subject);

    try (SystemFd fd = inode2fd(parent, O_DIRECTORY); var scope = ResourceScope.newConfinedScope()) {

      var linkRaw = CLinker.toCString(link, scope);
      var pathRaw = CLinker.toCString(path, scope);
      var emptyString = CLinker.toCString("", scope);

      int rc = (int) fSymlinkAt.invokeExact(linkRaw.address(), fd.fd(), pathRaw.address());
      checkError(rc == 0);
      Inode inode = lookup(parent, path);
      Stat stat = new Stat();
      stat.setUid(uid);
      stat.setGid(gid);
      try (SystemFd fd1 = inode2fd(inode, O_PATH)) {
        rc = (int) fChownAt.invokeExact(fd1.fd(), emptyString.address(), uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
        checkError(rc == 0);
      }
      return inode;
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
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

    ByteBuffer bb = data;
    if (!data.isDirect()) {
      bb = data.remaining() > MAX_CACHE ? ByteBuffer.allocateDirect(data.remaining()) : BUFFERS.get();
      bb.clear().put(data).flip();
    }

    try {
      MemorySegment dataRaw = MemorySegment.ofByteBuffer(bb);
      int n = (int)fPwrite.invokeExact(fd.fd(), dataRaw.address(), bb.remaining(), offset);
      checkError(n >= 0);

      // JNI interface does not updates the position
      if (data.isDirect()) {
        data.position(data.position() + n);
      }

      int rc = 0;
      switch (stabilityLevel) {
        case UNSTABLE:
          // NOP
          break;
        case DATA_SYNC:
          rc = (int)fDataSync.invokeExact(fd.fd());
          break;
        case FILE_SYNC:
          rc = (int)fSync.invokeExact(fd.fd());
          break;
        default:
          throw new RuntimeException("bad sync type");
      }
      checkError(rc == 0);
      return new WriteResult(stabilityLevel, n);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
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

    try (SystemFd fd = inode2fd(inode, openMode); var scope = ResourceScope.newConfinedScope()) {

      var emptyString = CLinker.toCString("", scope);
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
        rc = (int) fChownAt.invokeExact(fd.fd(), emptyString.address(), uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
        checkError(rc == 0);
      }

      if (currentStat.type() != Stat.Type.SYMLINK) {
        if (stat.isDefined(Stat.StatAttribute.MODE)) {
          rc = (int)fChmod.invokeExact(fd.fd(), stat.getMode());
          checkError(rc == 0);
        }
      }

      if (stat.isDefined(Stat.StatAttribute.SIZE)) {
        rc = (int)fFtruncate.invokeExact(fd.fd(), stat.getSize());
        checkError(rc == 0);
      }
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
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

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var scope = ResourceScope.newConfinedScope()) {

      var out = MemorySegment.allocateNative(1024, scope);
      int rc = (int)fListxattr.invokeExact(fd.fd(), out.address(), (int)out.byteSize());
      checkError(rc >= 0);

      byte[] listRaw = out.toByteArray();
      List<String> list = new ArrayList<>();
      for (int i = 0; i < rc; ) {
        int index = findByte(listRaw, (byte) '\0', i);
        if (index < 0) {
          break;
        }
        list.add(new String(listRaw, i, index - i, UTF_8));
        i = index + 1;
      }
      return list.toArray(String[]::new);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public byte[] getXattr(Inode inode, String name) throws IOException {

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var scope = ResourceScope.newConfinedScope()) {

      var attrName = CLinker.toCString(toXattrName(name), scope);
      var out = MemorySegment.allocateNative(64*1024, scope);
      int rc = (int)fGetxattr.invokeExact(fd.fd(), attrName.address(), out.address(), (int)out.byteSize());
      checkError(rc >= 0);
      return out.asSlice(0, rc).toByteArray();
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
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

    var data = ByteBuffer.allocateDirect(value.length);
    data.put(value);

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var scope = ResourceScope.newConfinedScope()) {
      var attrName = CLinker.toCString(toXattrName(attr), scope);
      var dataRaw = MemorySegment.ofByteBuffer(data);
      int rc = (int)fSetxattr.invokeExact(fd.fd(), attrName.address(), dataRaw.address(), value.length, flag);
      checkError(rc == 0);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void removeXattr(Inode inode, String attr) throws IOException {
    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var scope = ResourceScope.newConfinedScope()) {
      var attrName = CLinker.toCString(toXattrName(attr), scope);
      int rc = (int)fRemovexattr.invokeExact(fd.fd(), attrName.address());
      checkError(rc == 0);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public CompletableFuture<Long> copyFileRange(Inode src, long srcPos, Inode dst, long dstPos, long len) {

    SystemFd fdIn;
    SystemFd fdDst;

    try {
      fdIn = getOfLoadRawFd(src);
      fdDst = getOfLoadRawFd(dst);
    } catch (IOException e) {
      return CompletableFuture.failedFuture(e);
    }

    return CompletableFuture.supplyAsync(
            () -> {
              try (var scope = ResourceScope.newConfinedScope()) {

                var srcPosRef = MemorySegment.allocateNative(Long.BYTES, scope);
                var dstPosRef = MemorySegment.allocateNative(Long.BYTES, scope);

                srcPosRef.asByteBuffer().order(ByteOrder.nativeOrder()).putLong(0, srcPos);
                dstPosRef.asByteBuffer().order(ByteOrder.nativeOrder()).putLong(0, dstPos);

                return (int)fCopyFileRange.invokeExact(
                        fdIn.fd(), srcPosRef.address(),
                fdDst.fd(), dstPosRef.address(), len, 0);
              } catch (Throwable e) {
                throw new CompletionException(e);
              }
            }
    ).thenCompose(rc -> {
        try {
          checkError(rc >= 0);
          return CompletableFuture.completedFuture(Long.valueOf(rc));
        } catch (IOException e) {
        return CompletableFuture.failedFuture(e);
        }
    });

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

    try(var scope = ResourceScope.newConfinedScope()){

      MemorySegment str = CLinker.toCString(path, scope);
      MemorySegment bytes = MemorySegment.allocateNative(KernelFileHandle.MAX_HANDLE_SZ, scope);
      MemorySegment mntId = MemorySegment.allocateNative(Integer.BYTES, scope);

      ByteBuffer asByteBuffer = bytes.asByteBuffer().order(ByteOrder.nativeOrder());
      asByteBuffer.putInt(0, (int)bytes.byteSize());

      int rc = (int)fNameToHandleAt.invokeExact(fd, str.address(), bytes.address(), mntId.address(), flags);
      checkError(rc == 0);

      int handleSize = asByteBuffer.getInt(0) + 8;
      byte[] handle = bytes.asSlice(0, handleSize).toByteArray();

      KernelFileHandle fh = new KernelFileHandle(handle);
      LOG.debug("path = [{}], handle = {}", path, fh);
      return fh;
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  protected void checkError(boolean condition) throws IOException {

    if (condition) {
      return;
    }

    int errno = errno();
    if (errno == 0) {
      return;
    }

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
    byte[] fhBytes = fh.toBytes();
    try (var scope = ResourceScope.newConfinedScope()){

      MemorySegment rawHandle = MemorySegment.allocateNative(fhBytes.length, scope);
      rawHandle.asByteBuffer().put(fhBytes);
      int fd = (int) fOpenByHandleAt.invokeExact(rootFd, rawHandle.address(), flags);
      checkError(fd >= 0);
      return new SystemFd(fd);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  private Stat statByFd(SystemFd fd) throws IOException {

    try(var scope = ResourceScope.newConfinedScope()) {

      MemorySegment rawStat = MemorySegment.allocateNative(STAT_LAYOUT, scope);
      MemorySegment emptyString = CLinker.toCString("", scope);

      int rc = (int) fStatAt.invokeExact(fd.fd(), emptyString.address(), rawStat.address(), AT_EMPTY_PATH);

      checkError(rc == 0);
      return toVfsStat(rawStat);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  private Stat toVfsStat(MemorySegment fileStat) {
    Stat vfsStat = new Stat();

    ByteBuffer bb = fileStat.asByteBuffer().order(ByteOrder.nativeOrder());

    vfsStat.setDev((int)bb.getLong());
    vfsStat.setFileid(bb.getLong());
    vfsStat.setIno((int)vfsStat.getFileId());
    vfsStat.setNlink((int)bb.getLong());
    vfsStat.setMode(bb.getInt());
    vfsStat.setGid(bb.getInt());
    vfsStat.setUid(bb.getInt());
    bb.getInt(); // padding
    vfsStat.setRdev((int)bb.getLong());
    vfsStat.setSize(bb.getLong());
    bb.getLong(); // blksize
    bb.getLong(); // blocks
    vfsStat.setATime(bb.getLong() * 1000);
    bb.getLong(); // atimenanos
    vfsStat.setMTime(bb.getLong() * 1000);
    bb.getLong(); // mtimenanos
    vfsStat.setCTime(bb.getLong() * 1000);
    bb.getLong(); // ctimenanos

    vfsStat.setGeneration(Math.max(vfsStat.getCTime(), vfsStat.getMTime()));

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

  int ioctl(int fd, int request, byte[] data) throws IOException{
    ByteBuffer bb = ByteBuffer.allocateDirect(data.length);
    bb.put(data);
    bb.flip();
    try {
      var dataRaw = MemorySegment.ofByteBuffer(bb);
      return (int)fIoctl.invokeExact(fd, request, dataRaw.address());
    }  catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  // FFI helpers
  private String strerror(int errno) {
    try {
      MemoryAddress o = (MemoryAddress) fStrerror.invokeExact(errno);
      return CLinker.toJavaString(o);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private int open(String name, int flags, int mode) {
    try(var scope = ResourceScope.newConfinedScope()){
      MemorySegment str = CLinker.toCString(name, scope);
      return (int)fOpen.invokeExact(str.address(), flags, mode);
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

  private int errno() {
    try (var scope = ResourceScope.newConfinedScope()) {
      MemoryAddress a = (MemoryAddress)fErrono.invokeExact();
      return a.asSegment(Integer.BYTES, scope).asByteBuffer().order(ByteOrder.nativeOrder()).getInt();
    } catch (Throwable t) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }
  }
}
