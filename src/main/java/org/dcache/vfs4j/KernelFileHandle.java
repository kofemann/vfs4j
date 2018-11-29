package org.dcache.vfs4j;

import com.google.common.io.BaseEncoding;

import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.dcache.nfs.vfs.Inode;
import org.dcache.oncrpc4j.util.Bytes;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 */
public class KernelFileHandle {

    // stolen from /usr/include/bits/fcntl-linux.h
    public final static int MAX_HANDLE_SZ = 128;

    private final byte[] handleData;
    private final int type;

    protected KernelFileHandle(byte[] bytes) {

        checkArgument(bytes.length >= 8);
        int len = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).getInt(0);
        type = Bytes.getInt(bytes, 4);
        handleData = Arrays.copyOfRange(bytes, 8, 8 + len);
    }

    protected KernelFileHandle(int type, Inode inode) {
        this.type = type;
        handleData = inode.getFileId();
    }

    int getType() {
        return type;
    }

    byte[] toBytes() {
        byte[] bytes = new byte[8+handleData.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).putInt(0, handleData.length);
        Bytes.putInt(bytes, 4, type);
        System.arraycopy(handleData, 0, bytes, 8, handleData.length);
        return bytes;
    }

    Inode toInode() {
        return Inode.forFile(handleData);
    }

    @Override
    public java.lang.String toString() {
        return "[" + BaseEncoding.base16().lowerCase().encode(handleData) + "],"
                + " len = " + handleData.length;
    }
}
