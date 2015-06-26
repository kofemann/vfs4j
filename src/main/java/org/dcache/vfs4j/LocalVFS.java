package org.dcache.vfs4j;

import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.security.auth.Subject;

import jnr.ffi.provider.FFIProvider;
import jnr.constants.platform.Errno;
import jnr.ffi.annotations.*;

import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;

/**
 */
public class LocalVFS implements VirtualFileSystem {

    // stolen from /usr/include/bits/fcntl-linux.h

    private final static int O_DIRECTORY = 0200000;
    private final static int O_RDONLY = 00;

    private final static int AT_EMPTY_PATH = 0x1000;


    private final static int MAX_HANDLE_SIZE = 48;
    private final static int MIN_HANDLE_SIZE = 4;

    private final static int NONE = 0;

    private final File root;
    private final SysVfs sysVfs;
    private final jnr.ffi.Runtime runtime;

    public LocalVFS(File root) {
        this.root = root;

        sysVfs = FFIProvider.getSystemProvider()
                .createLibraryLoader(SysVfs.class)
                .load("c");
        runtime = jnr.ffi.Runtime.getRuntime(sysVfs);

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
        __getRootInode();
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

    private String getError() {
        return sysVfs.strerror(runtime.getLastError());
    }



    private void fd_to_fh(int fd) throws IOException {

        byte[] fh = new byte[512];
        int[] mntId = new int[1];
        int len = sysVfs.name_to_handle_at(fd, "", fh, mntId, AT_EMPTY_PATH);
        checkError(len);
        System.out.println("handle len = " + len);
    }

    private int __getRootInode() throws IOException {
        int fd = sysVfs.open(root.getAbsolutePath(), O_RDONLY | O_DIRECTORY, NONE);
        checkError(fd);
        fd_to_fh(fd);
        return fd;
    }

    private void checkError(int error) throws IOException {
        if (error < 0) {
            throw new IOException(getError());
        }
    }
    public interface SysVfs {
        String strerror(int e);
        int open(CharSequence path, int flags, int mode);
        int name_to_handle_at(int fd, CharSequence name, @Out byte[] kernel_fh, @Out int[] mntId, int flag);
    }
}
