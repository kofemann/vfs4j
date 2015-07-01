package org.dcache.vfs4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;

import jnr.ffi.provider.FFIProvider;
import jnr.constants.platform.Errno;
import jnr.ffi.Address;
import jnr.ffi.annotations.*;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.ServerFaultException;

import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class LocalVFS implements VirtualFileSystem {

    private final Logger LOG = LoggerFactory.getLogger(LocalVFS.class);

    // stolen from /usr/include/bits/fcntl-linux.h
    private final static int O_DIRECTORY = 0200000;
    private final static int O_RDONLY = 00;
    private final static int O_PATH = 010000000;

    private final static int AT_EMPTY_PATH = 0x1000;

    private final static int NONE = 0;

    private final SysVfs sysVfs;
    private final jnr.ffi.Runtime runtime;

    private final File root;
    private final KernelFileHandle rootFh;
    private final int rootFd;
    private final int mountId;

    public LocalVFS(File root) throws IOException {
        this.root = root;

        sysVfs = FFIProvider.getSystemProvider()
                .createLibraryLoader(SysVfs.class)
                .load("c");
        runtime = jnr.ffi.Runtime.getRuntime(sysVfs);

        rootFd = sysVfs.open(root.getAbsolutePath(), O_RDONLY | O_DIRECTORY, NONE);
        checkError(rootFd >= 0);
        rootFh = new KernelFileHandle(runtime);

        int[] mntId = new int[1];
        int rc = sysVfs.name_to_handle_at(rootFd, "", rootFh, mntId, AT_EMPTY_PATH);
        checkError(rc == 0);
        mountId = mntId[0];

        LOG.debug("handle  =  {}, mountid = {}", rootFh, mountId);

    }

    @Override
    public int access(Inode inode, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public FsStat getFsStat() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Inode getRootInode() throws IOException {
        return toInode(rootFh);
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        int fd = inode2fd(parent);
        return toInode(path2fh(fd, path, 0));
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {

        List<DirectoryEntry> list = new ArrayList<>();
        int fd = inode2fd(inode);
        Address p = sysVfs.fdopendir(fd);
        checkError(p != null);

        while (true) {
            Dirent dirent = sysVfs.readdir(p);
//            checkError(dirent != null);

            if (dirent == null) {
                break;
            }

            byte[] b = new byte[255];
            int i = 0;
            for (; dirent.d_name[i].get() != '\0'; i++) {
                b[i] = dirent.d_name[i].get();
            }
            String name = new String(b, 0, i, StandardCharsets.UTF_8);
            Inode fInode = lookup(inode, name);
            Stat stat = getattr(fInode);
            list.add( new DirectoryEntry(name, fInode, stat));

        }
        int rc = sysVfs.closedir(p);
        checkError(rc == 0);
        return list;
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {

        int fd = inode2fd(inode);
        FileStat stat = new FileStat(runtime);
        int rc = sysVfs.__fxstat64(0, fd, stat);
        checkError(rc == 0);
        return toVfsStat(stat);
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public AclCheckable getAclCheckable() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NfsIdMapping getIdMapper() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

        KernelFileHandle fh = new KernelFileHandle(runtime);

        int[] mntId = new int[1];
        int rc = sysVfs.name_to_handle_at(fd, path, fh, mntId, flags);
        checkError(rc == 0);
        LOG.debug("path = [{}], handle = {}", path, fh);
        return fh;
    }

    private void checkError(boolean condition) throws IOException {

        if (condition) {
            return;
        }

        int errno = runtime.getLastError();
        Errno e = Errno.valueOf(errno);
        String msg = sysVfs.strerror(errno) + " " + e.name() + "(" + errno + ")";
        LOG.info("Last error: {}", msg);

        switch (e) {
            case ENOENT:
                throw new NoEntException(msg);
            case EIO:
                throw new NfsIoException(msg);
            default:
                throw new ServerFaultException(msg);
        }
    }

    private int inode2fd(Inode inode) throws IOException {
        KernelFileHandle fh = toKernelFh(inode);
        int fd = sysVfs.open_by_handle_at(rootFd, fh, O_RDONLY );
        checkError(fd >= 0);
        return fd;
    }

    private KernelFileHandle toKernelFh(Inode inode) {
        byte[] data = inode.getFileId();
        KernelFileHandle fh = new KernelFileHandle(runtime);
        fh.handleType.set(rootFh.handleType.intValue());
        fh.handleBytes.set(data.length);
        for (int i = 0; i < data.length; i++) {
            fh.handleData[i].set(data[i]);
        }
        return fh;
    }

    private Inode toInode(KernelFileHandle fh) {
        byte[] data = new byte[fh.handleBytes.intValue()];
        for (int i = 0; i < data.length; i++) {
            data[i] = fh.handleData[i].get();
        }
        return Inode.forFile(data);
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
        vfsStat.setGeneration(fileStat.st_ctime.get() ^ fileStat.st_mtime.get());

        return vfsStat;
    }

    public interface SysVfs {

        String strerror(int e);

        int open(CharSequence path, int flags, int mode);

        int name_to_handle_at(int fd, CharSequence name, @Out @In KernelFileHandle fh, @Out int[] mntId, int flag);

        int open_by_handle_at(int mount_fd, @In KernelFileHandle fh, int flags);

        int __fxstat64(int version, int fd, @Transient @Out FileStat fileStat);

        Address fdopendir(int fd);

        int closedir(@In Address dirp);

        Dirent readdir(@In @Out Address dirp);
    }
}
