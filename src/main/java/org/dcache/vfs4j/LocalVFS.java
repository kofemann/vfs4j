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
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;

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

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.ValueLayout.*;
import static java.nio.charset.StandardCharsets.UTF_8;

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
  private final LoadingCache<KernelFileHandle, SystemFd> _openFilesCache;

  /** Cache of directories for fast lookup. */
  private final LoadingCache<KernelFileHandle, SystemFd> _openDirCache;

  // as in bits/statx-generic.h
  private final static int STATX_TYPE = 0x0001;
  private final static int STATX_MODE = 0x0002;
  private final static int STATX_NLINK = 0x0004;
  private final static int STATX_UID = 0x0008;
  private final static int STATX_GID = 0x0010;
  private final static int STATX_ATIME = 0x0020;
  private final static int STATX_MTIME = 0x0040;
  private final static int STATX_CTIME = 0x0080;
  private final static int STATX_INO = 0x0100;
  private final static int STATX_SIZE = 0x0200;
  private final static int STATX_BLOCKS = 0x0400;
  private final static int STATX_BASIC_STATS = 0x07ff;
  private final static int STATX_ALL = 0x0fff;

  private final static int STATX_BTIME = 0x0800;
  private final static int STATX_MNT_ID = 0x1000;
  private final static int STATX__RESERVED = 0x80000000;
  private final static int STATX_ATTR_COMPRESSED = 0x0004;
  private final static int STATX_ATTR_IMMUTABLE = 0x0010;
  private final static int STATX_ATTR_APPEND = 0x0020;
  private final static int STATX_ATTR_NODUMP = 0x0040;
  private final static int STATX_ATTR_ENCRYPTED = 0x0800;
  private final static int STATX_ATTR_AUTOMOUNT = 0x1000;
  private final static int STATX_ATTR_MOUNT_ROOT = 0x2000;
  private final static int STATX_ATTR_VERITY = 0x100000;
  private final static int STATX_ATTR_DAX = 0x200000;

  // struct statx layout as in bits/types/struct_statx.h
  public static final GroupLayout STATX_LAYOUT = MemoryLayout.structLayout(

          JAVA_INT.withName("stx_mask"), /* Mask of bits indicating filled fields. */
          JAVA_INT.withName("stx_blksize"), /* Optimal block size for IO.*/
          JAVA_LONG.withName("stx_attributes"), /* Extra file attribute indicators. */
          JAVA_INT.withName("stx_nlink"), /* Object link count.	*/
          JAVA_INT.withName("stx_uid"), /* User ID of the file's owner. */
          JAVA_INT.withName("stx_gid"), /* Group ID of the file's owner.*/
          JAVA_SHORT.withName("stx_mode"), /* File type and mode.	*/
          MemoryLayout.paddingLayout(16), /* padding */

          JAVA_LONG.withName("stx_ino"), /* File inode number.	*/
          JAVA_LONG.withName("stx_size"), /* File's size in bytes.*/
          JAVA_LONG.withName("stx_blocks"), /* Number of 512-byte blocks allocated */
          JAVA_LONG.withName("stx_attributes_mask"), /* Mask of supported attributes */

          /* The following fields are file timestamps */
          JAVA_LONG.withName("stx_atime"), /* Last access, sec. */
          JAVA_INT.withName("stx_atimensec"), /* Last access, nannoseconds.*/
          MemoryLayout.paddingLayout(32), /* padding */

          JAVA_LONG.withName("stx_btime"), /* Creation time, sec. */
          JAVA_INT.withName("stx_btimensec"), /* Creation time, nannoseconds.*/
          MemoryLayout.paddingLayout(32), /* padding */

          JAVA_LONG.withName("stx_ctime"), /* Status change time, sec. */
          JAVA_INT.withName("stx_ctimensec"), /* Status change time, nannoseconds.*/
          MemoryLayout.paddingLayout(32), /* padding */

          JAVA_LONG.withName("stx_mtime"), /* Last modification access, sec. */
          JAVA_INT.withName("stx_mtimensec"), /* Last modification access, nannoseconds.*/
          MemoryLayout.paddingLayout(32), /* padding */

           /* If this file represents a device, then the next two fields contain the ID of the device */
          JAVA_LONG.withName("stx_rdev_min"), /* Device. */
          JAVA_LONG.withName("stx_rdev_maj"), /* Device number, if device.*/

           /* The next two fields contain the ID of the device containing the filesystem where the file resides */

          JAVA_LONG.withName("stx_dev_min"), /* Device. */
          JAVA_LONG.withName("stx_rdev_max"), /* Device number, if device.*/

          MemoryLayout.sequenceLayout(14, MemoryLayout.paddingLayout(64)) /* padding*/
  );

  private static final MemoryLayout STAT_FS_LAYOUT = MemoryLayout.structLayout(
          JAVA_LONG.withName("type"),
          JAVA_LONG.withName("bsize"),
          JAVA_LONG.withName("blocks"),
          JAVA_LONG.withName("free"),
          JAVA_LONG.withName("bavail"),

          JAVA_LONG.withName("files"),
          JAVA_LONG.withName("ffree"),
          JAVA_LONG.withName("fsid"),
          JAVA_LONG.withName("namelen"),
          JAVA_LONG.withName("frsize"),
          JAVA_LONG.withName("flags"),
          MemoryLayout.sequenceLayout(6, JAVA_INT).withName("spare")
  );

  private static final VarHandle VH_STATFS_TYPE = STAT_FS_LAYOUT.varHandle(groupElement("type"));
  private static final VarHandle VH_STATFS_BSIZE = STAT_FS_LAYOUT.varHandle(groupElement("bsize"));
  private static final VarHandle VH_STATFS_BLOCKS = STAT_FS_LAYOUT.varHandle(groupElement("blocks"));
  private static final VarHandle VH_STATFS_FREE = STAT_FS_LAYOUT.varHandle(groupElement("free"));
  private static final VarHandle VH_STATFS_BAVAIL = STAT_FS_LAYOUT.varHandle(groupElement("bavail"));
  private static final VarHandle VH_STATFS_FILES = STAT_FS_LAYOUT.varHandle(groupElement("files"));
  private static final VarHandle VH_STATFS_FFREE = STAT_FS_LAYOUT.varHandle(groupElement("ffree"));
  private static final VarHandle VH_STATFS_FSID = STAT_FS_LAYOUT.varHandle(groupElement("fsid"));
  private static final VarHandle VH_STATFS_NAMELEN = STAT_FS_LAYOUT.varHandle(groupElement("namelen"));
  private static final VarHandle VH_STATFS_FRSIZE = STAT_FS_LAYOUT.varHandle(groupElement("frsize"));
  private static final VarHandle VH_STATFS_FLAGS = STAT_FS_LAYOUT.varHandle(groupElement("flags"));

  private static final MemoryLayout DIRENT_LAYOUT = MemoryLayout.structLayout(
          JAVA_LONG.withName("ino"),
          JAVA_LONG.withName("off"),
          JAVA_SHORT.withName("reclen"),
          JAVA_BYTE.withName("type"),
          MemoryLayout.sequenceLayout(MAX_NAME_LEN, JAVA_BYTE).withName("name")
  );

  private static final VarHandle VH_DIRENT_INO = DIRENT_LAYOUT.varHandle(groupElement("ino"));
  private static final VarHandle VH_DIRENT_OFF = DIRENT_LAYOUT.varHandle(groupElement("off"));
  private static final VarHandle VH_DIRENT_RECLEN = DIRENT_LAYOUT.varHandle(groupElement("reclen"));
  private static final VarHandle VH_DIRENT_TYPE = DIRENT_LAYOUT.varHandle(groupElement("type"));
  // private static final VarHandle VH_DIRENT_NAME = DIRENT_LAYOUT.varHandle(groupElement("name"), sequenceElement());

  // handles to native functions;
  private static final MethodHandle fStrerror;
  private static final MethodHandle fOpen;
  private static final MethodHandle fOpenAt;
  private static final MethodHandle fClose;
  private static final MethodHandle fNameToHandleAt;
  private static final MethodHandle fOpenByHandleAt;
  private static final MethodHandle fSync;
  private static final MethodHandle fDataSync;
  private static final MethodHandle fStatx;
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

  private static final MethodHandle fGetdents;

  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup STDLIB = LINKER.defaultLookup();

  static {

    // The Foreign Function & Memory API
    // https://bugs.openjdk.org/browse/JDK-8282048

    // magic function that return pointer to errno variable
    fErrono = LINKER.downcallHandle(
            STDLIB.find("__errno_location").orElseThrow(() -> new NoSuchElementException("__errno_location")),
                    FunctionDescriptor.of(ADDRESS.asUnbounded())
            );

    fStrerror = LINKER.downcallHandle(
            STDLIB.find("strerror").orElseThrow(() -> new NoSuchElementException("strerror")),
                    FunctionDescriptor.of(ADDRESS.asUnbounded(), JAVA_INT)
            );

    fOpen = LINKER.downcallHandle(
            STDLIB.find("open").orElseThrow(() -> new NoSuchElementException("open")),
                    FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)
            );

    fOpenAt = LINKER.downcallHandle(
            STDLIB.find("openat").orElseThrow(() -> new NoSuchElementException("openat")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)
            );

    fClose = LINKER.downcallHandle(
            STDLIB.find("close").orElseThrow(() -> new NoSuchElementException("close")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT)
            );

    fNameToHandleAt = LINKER.downcallHandle(
            STDLIB.find("name_to_handle_at").orElseThrow(() -> new NoSuchElementException("name_to_handle_at")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT)
            );

    fOpenByHandleAt = LINKER.downcallHandle(
            STDLIB.find("open_by_handle_at").orElseThrow(() -> new NoSuchElementException("open_by_handle_at")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT)
            );

    fDataSync = LINKER.downcallHandle(
            STDLIB.find("fdatasync").orElseThrow(() -> new NoSuchElementException("fdatasync")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT)
            );

    fSync = LINKER.downcallHandle(
            STDLIB.find("fsync").orElseThrow(() -> new NoSuchElementException("fsync")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT)
            );

    fStatx = LINKER.downcallHandle(
            STDLIB.find("statx").orElseThrow(() -> new NoSuchElementException("statx")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS)
            );

    fStatFs = LINKER.downcallHandle(
            STDLIB.find("fstatfs").orElseThrow(() -> new NoSuchElementException("fstatfs")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS)
            );

    fUnlinkAt = LINKER.downcallHandle(
            STDLIB.find("unlinkat").orElseThrow(() -> new NoSuchElementException("unlinkat")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT)
            );

    fOpendir = LINKER.downcallHandle(
            STDLIB.find("fdopendir").orElseThrow(() -> new NoSuchElementException("fdopendir")),
                    FunctionDescriptor.of(ADDRESS, JAVA_INT)
            );

    fReaddir = LINKER.downcallHandle(
            STDLIB.find("readdir").orElseThrow(() -> new NoSuchElementException("readdir")),
                    FunctionDescriptor.of(ADDRESS, ADDRESS)
            );

    fSeekdir = LINKER.downcallHandle(
            STDLIB.find("seekdir").orElseThrow(() -> new NoSuchElementException("seekdir")),
                    FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG)
            );

    fPread = LINKER.downcallHandle(
            STDLIB.find("pread").orElseThrow(() -> new NoSuchElementException("pread")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG)
            );

    fPwrite = LINKER.downcallHandle(
            STDLIB.find("pwrite").orElseThrow(() -> new NoSuchElementException("pwrite")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG)
    );

    fSymlinkAt = LINKER.downcallHandle(
            STDLIB.find("symlinkat").orElseThrow(() -> new NoSuchElementException("symlinkat")),
                    FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS)
            );

    fRenameAt = LINKER.downcallHandle(
            STDLIB.find("renameat").orElseThrow(() -> new NoSuchElementException("renameat")),
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS)
            );

    fReadlinkAt = LINKER.downcallHandle(
            STDLIB.find("readlinkat").orElseThrow(() -> new NoSuchElementException("readlinkat")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT)
    );

    fChownAt = LINKER.downcallHandle(
            STDLIB.find("fchownat").orElseThrow(() -> new NoSuchElementException("fchownat")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT, JAVA_INT)
    );

    fMkdirAt = LINKER.downcallHandle(
            STDLIB.find("mkdirat").orElseThrow(() -> new NoSuchElementException("mkdirat")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT)
    );

    fChmod = LINKER.downcallHandle(
            STDLIB.find("fchmod").orElseThrow(() -> new NoSuchElementException("fchmod")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT)
    );

    fFtruncate = LINKER.downcallHandle(
            STDLIB.find("ftruncate").orElseThrow(() -> new NoSuchElementException("ftruncate")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG)
    );

    fLinkAt = LINKER.downcallHandle(
            STDLIB.find("linkat").orElseThrow(() -> new NoSuchElementException("linkat")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT)
    );

    fCopyFileRange = LINKER.downcallHandle(
            STDLIB.find("copy_file_range").orElseThrow(() -> new NoSuchElementException("copy_file_range")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT)
    );

    fListxattr  = LINKER.downcallHandle(
            STDLIB.find("flistxattr").orElseThrow(() -> new NoSuchElementException("flistxattr")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT)
    );

    fGetxattr  = LINKER.downcallHandle(
            STDLIB.find("fgetxattr").orElseThrow(() -> new NoSuchElementException("fgetxattr")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT)
    );

    fSetxattr  = LINKER.downcallHandle(
            STDLIB.find("fsetxattr").orElseThrow(() -> new NoSuchElementException("fsetxattr")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT)
    );

    fRemovexattr  = LINKER.downcallHandle(
            STDLIB.find("fremovexattr").orElseThrow(() -> new NoSuchElementException("fremovexattr")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS)
    );

    fMknodeAt = LINKER.downcallHandle(
            STDLIB.find("__xmknodat").orElseThrow(() -> new NoSuchElementException("__xmknodat")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS)
    );

    fIoctl  = LINKER.downcallHandle(
            STDLIB.find("ioctl").orElseThrow(() -> new NoSuchElementException("ioctl")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS)
    );

    fGetdents  = LINKER.downcallHandle(
            STDLIB.find("getdents64").orElseThrow(() -> new NoSuchElementException("getdents64")),
            FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG)
    );
  }

  public LocalVFS(File root) throws IOException {

    rootFd = open(root.getAbsolutePath());
    checkError(rootFd >= 0);

    rootFh = path2fh(rootFd, "", AT_EMPTY_PATH);

    _openFilesCache =
        CacheBuilder.newBuilder()
            .maximumSize(1024)
            .removalListener(new FileCloser())
            .build(new FileOpenner());

    _openDirCache =
            CacheBuilder.newBuilder()
                    .maximumSize(1024)
                    .removalListener(new FileCloser())
                    .build(new DirOpenner());
  }

  @Override
  public int access(Subject subject, Inode inode, int mode) throws IOException {
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

    try (var arena = Arena.openConfined()) {

      SystemFd fd = _openDirCache.get(new KernelFileHandle(parent));

      var emptyString = arena.allocateUtf8String("");
      var pathRaw = arena.allocateUtf8String(path);

      int rc;
      if (type == Stat.Type.REGULAR) {
        int rfd =  (int)fOpenAt.invokeExact(fd.fd(), pathRaw, O_EXCL | O_CREAT | O_RDWR, mode);
        checkError(rfd >= 0);
        rc = (int) fChownAt.invokeExact(rfd, emptyString, uid, gid, AT_EMPTY_PATH);
        checkError(rc == 0);


        KernelFileHandle handle = path2fh(rfd, "", AT_EMPTY_PATH);
        _openFilesCache.put(handle, new SystemFd(rfd));
        return handle.toInode();

      } else {

        // FIXME: we should get major and minor numbers from CREATE arguments.
        // dev == (long)major << 32 | minor
        var ms = arena.allocate(Long.BYTES);
        var dev = ms.asByteBuffer();
        dev.putLong(0, type == Stat.Type.BLOCK || type == Stat.Type.CHAR ? 1 : 0);

        rc = (int)fMknodeAt.invokeExact(0, fd.fd(), pathRaw,  mode | type.toMode(), ms);
        checkError(rc >= 0);
        rc = (int) fChownAt.invokeExact(fd.fd(), pathRaw, uid, gid, 0);
        checkError(rc >= 0);
        return lookup0(fd.fd(), path);
      }
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public FsStat getFsStat() throws IOException {

    try(var arena = Arena.openConfined()) {

      MemorySegment rawStatFS = arena.allocate(STAT_FS_LAYOUT);
      int rc = (int)fStatFs.invokeExact(rootFd, rawStatFS);
      checkError(rc == 0);

      long  f_bsize = (long)VH_STATFS_BSIZE.get(rawStatFS);
      long f_blocks = (long)VH_STATFS_BLOCKS.get(rawStatFS);
      long f_free = (long)VH_STATFS_FREE.get(rawStatFS);
      long f_files = (long)VH_STATFS_FILES.get(rawStatFS);
      long f_ffree = (long)VH_STATFS_FFREE.get(rawStatFS);

      return new FsStat(
          f_blocks * f_bsize,
          f_files,
          (f_blocks - f_free) * f_bsize,
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
    try {
      SystemFd fd = _openDirCache.get(new KernelFileHandle(parent));
      return lookup0(fd.fd(), path);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  /**
   * lookup in parent directory.
   * @param parent parent directory file descriptor.
   * @param path path to lookup
   * @return file's inode
   * @throws IOException
   */
  private Inode lookup0(int parent, String path) throws IOException {
      return path2fh(parent, path, 0).toInode();
  }

  @Override
  public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
    try (SystemFd dirFd = inode2fd(parent, O_NOFOLLOW | O_DIRECTORY);
         SystemFd inodeFd = inode2fd(link, O_NOFOLLOW); var arena = Arena.openConfined()) {

      var emptyString = arena.allocateUtf8String("");
      var pathRaw = arena.allocateUtf8String(path);

      int rc = (int) fLinkAt.invokeExact(inodeFd.fd(), emptyString, dirFd.fd(), pathRaw, AT_EMPTY_PATH);

      checkError(rc == 0);
      return lookup0(dirFd.fd(), path);
    }  catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {

    TreeSet<DirectoryEntry> list = new TreeSet<>();
    try (var arena = Arena.openConfined(); var fd =  inode2fd(new KernelFileHandle(inode), O_NOFOLLOW | O_DIRECTORY)) {

      MemorySegment dirents = arena.allocate(DIRENT_LAYOUT.byteSize()*8192);
      while (true) {
        long n = (long) fGetdents.invokeExact(fd.fd(), dirents, DIRENT_LAYOUT.byteSize());
        checkError(n >= 0);
        if ( n == 0) {
          break;
        }

        long offiset = 0L;
        while(n > 0) {
          MemorySegment dirent = dirents.asSlice(offiset);
          long off = (long)VH_DIRENT_OFF.get(dirent);
          int reclen = (int)VH_DIRENT_RECLEN.get(dirent);

          if (off > cookie) {
            String name = dirent.asSlice(DIRENT_LAYOUT.byteOffset(groupElement("name")), reclen).getUtf8String(0);
            Inode fInode = lookup0(fd.fd(), name);
            Stat stat = getattr(fInode);
            list.add(new DirectoryEntry(name, fInode, stat, off));
          }
          offiset += reclen;
          n -= reclen;
        }
      }
      return new DirectoryStream(DirectoryStream.ZERO_VERIFIER, list);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  //@Override
  // REVISIT: this version doesn't work as expected
  public DirectoryStream list_dead(Inode inode, byte[] verifier, long cookie) throws IOException {

    TreeSet<DirectoryEntry> list = new TreeSet<>();
    try (var arena = Arena.openConfined()) {

      SystemFd fd = _openDirCache.get(new KernelFileHandle(inode));
      MemorySegment p = (MemorySegment) fOpendir.invokeExact(fd.fd());
      checkError(p != MemorySegment.NULL);

      fSeekdir.invokeExact(p, cookie);

      while (true) {
        MemorySegment dirent = (MemorySegment) fReaddir.invokeExact(p);
        if (dirent == MemorySegment.NULL) {
          break;
        }

        long off = (long)VH_DIRENT_OFF.get(dirent);
        int reclen = (int)VH_DIRENT_RECLEN.get(dirent);

        String name = dirent.asSlice(DIRENT_LAYOUT.byteOffset(groupElement("name")), reclen).getUtf8String(0);
        Inode fInode = lookup0(fd.fd(), name);
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
    try (var arena = Arena.openConfined()) {

      SystemFd fd = _openDirCache.get(new KernelFileHandle(parent));

      var emptyString = arena.allocateUtf8String("");
      var pathRaw = arena.allocateUtf8String(path);

      int rc = (int) fMkdirAt.invokeExact(fd.fd(), pathRaw, mode);
      checkError(rc == 0);
      inode = lookup0(fd.fd(), path);
      try (SystemFd fd1 = inode2fd(inode, O_NOFOLLOW | O_DIRECTORY)) {
        rc = (int) fChownAt.invokeExact(fd1.fd(), emptyString, uid, gid, AT_EMPTY_PATH);
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
         var arena = Arena.openConfined()) {

      MemorySegment newNameRaw = arena.allocateUtf8String(newName);
      MemorySegment oldNameRaw = arena.allocateUtf8String(oldName);

        int rc = (int) fRenameAt.invokeExact(fd1.fd(), oldNameRaw, fd2.fd(), newNameRaw);
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

    try (var arena = Arena.openConfined()) {
        MemorySegment rawData = MemorySegment.ofBuffer(bb);

        int n = (int)fPread.invokeExact(fd.fd(), rawData, data.remaining(), offset);
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
        var arena = Arena.openConfined()) {



      var emptyString = arena.allocateUtf8String("");

      var stat = statByFd(fd); // get link size
      var link = arena.allocate(stat.getSize() + 1); // space for null-terminator

      int rc = (int) fReadlinkAt.invokeExact(fd.fd(), emptyString, link, (int)link.byteSize());
      checkError(rc >= 0);

      return link.getUtf8String(0);

    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void remove(Inode parent, String path) throws IOException {
    try (var arena = Arena.openConfined()) {

      SystemFd fd = _openDirCache.get(new KernelFileHandle(parent));
      var pathRaw = arena.allocateUtf8String(path);
      Inode inode = lookup0(fd.fd(), path);
      Stat stat = getattr(inode);
      int flags = stat.type() == Stat.Type.DIRECTORY ? AT_REMOVEDIR : 0;
      int rc = (int)fUnlinkAt.invokeExact(fd.fd(), pathRaw, flags);
      checkError(rc == 0);
      if (stat.type() == Stat.Type.DIRECTORY) {
        _openDirCache.invalidate(new KernelFileHandle(inode));
      } else {
        _openFilesCache.invalidate(new KernelFileHandle(inode));
      }
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

    try (var arena = Arena.openConfined()) {

      SystemFd fd = _openDirCache.get(new KernelFileHandle(parent));

      var emptyString = arena.allocateUtf8String("");
      var pathRaw = arena.allocateUtf8String(path);
      var linkRaw = arena.allocateUtf8String(link);

      int rc = (int) fSymlinkAt.invokeExact(linkRaw, fd.fd(), pathRaw);
      checkError(rc == 0);
      Inode inode = lookup0(fd.fd(), path);
      Stat stat = new Stat();
      stat.setUid(uid);
      stat.setGid(gid);
      try (SystemFd fd1 = inode2fd(inode, O_PATH)) {
        rc = (int) fChownAt.invokeExact(fd1.fd(), emptyString, uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
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
      MemorySegment dataRaw = MemorySegment.ofBuffer(bb);
      int n = (int)fPwrite.invokeExact(fd.fd(), dataRaw, bb.remaining(), offset);
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

    try (SystemFd fd = inode2fd(inode, openMode); var arena = Arena.openConfined()) {

      var emptyString = arena.allocateUtf8String("");

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
        rc = (int) fChownAt.invokeExact(fd.fd(), emptyString, uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
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

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var arena = Arena.openConfined()) {


      var out = arena.allocate(0);

      int rc;
      do {
        // get the expected reply size
        rc = (int)fListxattr.invokeExact(fd.fd(), MemorySegment.NULL, 0);
        checkError(rc >= 0);

        out = arena.allocate(rc);
        rc = (int)fListxattr.invokeExact(fd.fd(), out, (int)out.byteSize());
      }while (rc == Errno.ERANGE.errno());
      checkError(rc >= 0);

      byte[] listRaw = out.toArray(JAVA_BYTE);
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

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var arena = Arena.openConfined()) {

      var attrName = arena.allocateUtf8String(toXattrName(name));
      var out = arena.allocate(64*1024);
      int rc = (int)fGetxattr.invokeExact(fd.fd(), attrName, out, (int)out.byteSize());
      checkError(rc >= 0);
      return out.asSlice(0, rc).toArray(JAVA_BYTE);
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

    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var arena = Arena.openConfined()) {

      var attrName = arena.allocateUtf8String(toXattrName(attr));
      var dataRaw = MemorySegment.ofBuffer(data);
      int rc = (int)fSetxattr.invokeExact(fd.fd(), attrName, dataRaw, value.length, flag);
      checkError(rc == 0);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void removeXattr(Inode inode, String attr) throws IOException {
    try (SystemFd fd = inode2fd(inode, O_NOFOLLOW); var arena = Arena.openConfined()) {
      var attrName = arena.allocateUtf8String(toXattrName(attr));
      int rc = (int)fRemovexattr.invokeExact(fd.fd(), attrName);
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
              try (var arena = Arena.openConfined()) {

                var srcPosRef = arena.allocate(Long.BYTES);
                var dstPosRef = arena.allocate(Long.BYTES);

                srcPosRef.asByteBuffer().order(ByteOrder.nativeOrder()).putLong(0, srcPos);
                dstPosRef.asByteBuffer().order(ByteOrder.nativeOrder()).putLong(0, dstPos);

                return (int)fCopyFileRange.invokeExact(
                        fdIn.fd(), srcPosRef,
                fdDst.fd(), dstPosRef, len, 0);
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

    try(var arena = Arena.openConfined()){

      MemorySegment str = arena.allocateUtf8String(path);
      MemorySegment bytes = arena.allocate(KernelFileHandle.MAX_HANDLE_SZ);
      MemorySegment mntId = arena.allocate(Integer.BYTES);

      ByteBuffer asByteBuffer = bytes.asByteBuffer().order(ByteOrder.nativeOrder());
      asByteBuffer.putInt(0, (int)bytes.byteSize());

      int rc = (int)fNameToHandleAt.invokeExact(fd, str, bytes, mntId, flags);
      checkError(rc == 0);

      int handleSize = bytes.get(JAVA_INT, 0) + 8; // handle size and type 4+4
      byte[] handle = bytes.asSlice(0, handleSize).toArray(JAVA_BYTE);

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
      case ENOSPC -> throw new NoSpcException(msg);
      case EPERM -> throw new PermException(msg);
      default -> {
        IOException t = new ServerFaultException(msg);
        LOG.error("Unhandled POSIX error {} : {}", e,  msg);
        throw t;
      }
    }
  }

  private SystemFd inode2fd(Inode inode, int flags) throws IOException {
    return inode2fd(new KernelFileHandle(inode), flags);
  }

  private SystemFd inode2fd(KernelFileHandle fh, int flags) throws IOException {
    byte[] fhBytes = fh.toBytes();
    try (var arena = Arena.openConfined()){

      MemorySegment rawHandle = arena.allocate(fhBytes.length);
      rawHandle.asByteBuffer().put(fhBytes);
      int fd = (int) fOpenByHandleAt.invokeExact(rootFd, rawHandle, flags);
      checkError(fd >= 0);
      return new SystemFd(fd);
    } catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  private Stat statByFd(SystemFd fd) throws IOException {

    try(var arena = Arena.openConfined()) {

      MemorySegment rawStat = arena.allocate(STATX_LAYOUT);
      var emptyString = arena.allocateUtf8String("");

      int rc = (int) fStatx.invokeExact(fd.fd(), emptyString, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW, STATX_ALL, rawStat);

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

    int valuesMask = bb.getInt(); // mask
    bb.getInt(); // blksize
    bb.getLong(); // attributes
    vfsStat.setNlink(bb.getInt());
    vfsStat.setUid(bb.getInt());
    vfsStat.setGid(bb.getInt());
    vfsStat.setMode(Short.toUnsignedInt(bb.getShort()));
    bb.getShort(); // padding
    vfsStat.setIno(bb.getLong());
    vfsStat.setSize(bb.getLong());
    bb.getLong(); // blocks
    bb.getLong(); // attributes_mask

    vfsStat.setATime(Math.multiplyExact(bb.getLong(), 1000));
    bb.getInt(); // atimenanos
    bb.getInt(); // padding

    vfsStat.setBTime(Math.multiplyExact(bb.getLong(), 1000));
    bb.getInt(); // btimenanos
    bb.getInt(); // padding

    vfsStat.setCTime(Math.multiplyExact(bb.getLong(), 1000));
    bb.getInt(); // ctimenanos
    bb.getInt(); // padding

    vfsStat.setMTime(Math.multiplyExact(bb.getLong(), 1000));
    bb.getInt(); // mtimenanos
    bb.getInt(); // padding

    vfsStat.setRdev(bb.getInt());
    bb.getInt(); // rdev_maj

    vfsStat.setDev(bb.getInt());
    bb.getInt(); // dev_maj

    vfsStat.setGeneration(Math.max(vfsStat.getCTime(), vfsStat.getMTime()));

    return vfsStat;
  }

  private class FileCloser implements RemovalListener<KernelFileHandle, SystemFd> {

    @Override
    public void onRemoval(RemovalNotification<KernelFileHandle, SystemFd> notification) {
      close(notification.getValue().fd());
    }
  }

  private class FileOpenner extends CacheLoader<KernelFileHandle, SystemFd> {

    @Override
    public SystemFd load(@Nonnull KernelFileHandle key) throws Exception {
      return inode2fd(key, O_NOFOLLOW | O_RDWR);
    }
  }

  private class DirOpenner extends CacheLoader<KernelFileHandle, SystemFd> {

    @Override
    public SystemFd load(@Nonnull KernelFileHandle key) throws Exception {
      return inode2fd(key, O_NOFOLLOW | O_DIRECTORY);
    }
  }

  private SystemFd getOfLoadRawFd(Inode inode) throws IOException {
    try {
      return _openFilesCache.get(new KernelFileHandle(inode));
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
      var dataRaw = MemorySegment.ofBuffer(bb);
      return (int)fIoctl.invokeExact(fd, request, dataRaw);
    }  catch (Throwable t) {
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new RuntimeException(t);
    }
  }

  // FFI helpers
  private String strerror(int errno) {
    try {
      MemorySegment o = (MemorySegment) fStrerror.invokeExact(errno);
      return o.getUtf8String(0);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private int open(String name) {
    try(var arena = Arena.openConfined()){

      MemorySegment str = arena.allocateUtf8String(name);
      return (int)fOpen.invokeExact(str, O_DIRECTORY, O_RDONLY);
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

    @Override
    public String toString() {
      return "SystemFd{" +
              "fd=" + fd +
              '}';
    }
  }

  private int errno() {
    try (var arena = Arena.openConfined()) {
      MemorySegment a = (MemorySegment)fErrono.invokeExact();
      return a.get(JAVA_INT, 0);
    } catch (Throwable t) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }
  }
}
