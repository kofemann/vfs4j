package org.dcache.vfs4j;

import com.google.common.io.BaseEncoding;

import org.dcache.nfs.vfs.Inode;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/** */
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
        + BaseEncoding.base16().lowerCase().encode(handleData)
        + "],"
        + " len = "
        + handleData.length;
  }
}
