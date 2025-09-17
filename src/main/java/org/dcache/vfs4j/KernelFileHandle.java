package org.dcache.vfs4j;

import org.dcache.nfs.vfs.Inode;

import java.util.Arrays;
import java.util.HexFormat;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A representation of a file handle as used by the Linux kernel.
 */
public class KernelFileHandle {

  // stolen from /usr/include/bits/fcntl-linux.h
  public static final int MAX_HANDLE_SZ = 128;

  private final byte[] handleData;

  protected KernelFileHandle(byte[] bytes) {
    checkArgument(bytes.length >= 8);
    handleData = bytes;
  }

  protected KernelFileHandle(Inode inode) {
    handleData = inode.getFileId();
  }

  byte[] toBytes() {
    return handleData;
  }

  Inode toInode() {
    return Inode.forFile(handleData);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KernelFileHandle that = (KernelFileHandle) o;
    return Arrays.equals(handleData, that.handleData);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(handleData);
  }

  @Override
  public java.lang.String toString() {
    return "["
            + HexFormat.of().formatHex(handleData)
            + "],"
            + " len = "
            + handleData.length;
  }
}
