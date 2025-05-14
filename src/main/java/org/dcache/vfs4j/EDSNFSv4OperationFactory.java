package org.dcache.vfs4j;

import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.AbstractOperationExecutor;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.OperationBIND_CONN_TO_SESSION;
import org.dcache.nfs.v4.OperationCOMMIT;
import org.dcache.nfs.v4.OperationCREATE_SESSION;
import org.dcache.nfs.v4.OperationDESTROY_CLIENTID;
import org.dcache.nfs.v4.OperationDESTROY_SESSION;
import org.dcache.nfs.v4.OperationEXCHANGE_ID;
import org.dcache.nfs.v4.OperationGETATTR;
import org.dcache.nfs.v4.OperationILLEGAL;
import org.dcache.nfs.v4.OperationPUTFH;
import org.dcache.nfs.v4.OperationPUTROOTFH;
import org.dcache.nfs.v4.OperationRECLAIM_COMPLETE;
import org.dcache.nfs.v4.OperationSEQUENCE;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.WRITE4res;
import org.dcache.nfs.v4.xdr.WRITE4resok;
import org.dcache.nfs.v4.xdr.count4;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** */
public class EDSNFSv4OperationFactory extends AbstractOperationExecutor {


  private final LocalVFS vfs;

  public EDSNFSv4OperationFactory(LocalVFS vfs) {
    this.vfs = vfs;
  }

  @Override
  protected AbstractNFSv4Operation getOperation(nfs_argop4 op) {
    switch (op.argop) {
      case nfs_opnum4.OP_COMMIT:
        return new OperationCOMMIT(op);
      case nfs_opnum4.OP_GETATTR:
        return new OperationGETATTR(op);
      case nfs_opnum4.OP_PUTFH:
        return new OperationPUTFH(op);
      case nfs_opnum4.OP_PUTROOTFH:
        return new OperationPUTROOTFH(op);
      case nfs_opnum4.OP_READ:
        return new EOperationREAD(op);
      case nfs_opnum4.OP_WRITE:
        return new EOperationWRITE(op);
      case nfs_opnum4.OP_EXCHANGE_ID:
        return new OperationEXCHANGE_ID(op);
      case nfs_opnum4.OP_CREATE_SESSION:
        return new OperationCREATE_SESSION(op);
      case nfs_opnum4.OP_DESTROY_SESSION:
        return new OperationDESTROY_SESSION(op);
      case nfs_opnum4.OP_SEQUENCE:
        return new OperationSEQUENCE(op);
      case nfs_opnum4.OP_RECLAIM_COMPLETE:
        return new OperationRECLAIM_COMPLETE(op);
      case nfs_opnum4.OP_BIND_CONN_TO_SESSION:
        return new OperationBIND_CONN_TO_SESSION(op);
      case nfs_opnum4.OP_DESTROY_CLIENTID:
        return new OperationDESTROY_CLIENTID(op);
      case nfs_opnum4.OP_ILLEGAL:
    }
    return new OperationILLEGAL(op);
  }

  private class EOperationREAD extends AbstractNFSv4Operation {

    private static final Logger _log =
        LoggerFactory.getLogger(org.dcache.nfs.v4.OperationREAD.class);

    public EOperationREAD(nfs_argop4 args) {
      super(args, nfs_opnum4.OP_READ);
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) throws IOException {
      final READ4res res = result.opread;


      var inode = context.currentInode();
      Stat inodeStat = vfs.getattr(inode);

      long offset = _args.opread.offset.value;
      int count = _args.opread.count.value;

      ByteBuffer buf = ByteBuffer.allocate(count);

      int bytesReaded = vfs.read(inode, buf, offset);
      if (bytesReaded < 0) {
        throw new NfsIoException("IO not allowed");
      }

      buf.flip();
      res.status = nfsstat.NFS_OK;
      res.resok4 = new READ4resok();

      res.resok4.data = buf;

      if (offset + bytesReaded >= inodeStat.getSize()) {
        res.resok4.eof = true;
      }
    }
  }

  public class EOperationWRITE extends AbstractNFSv4Operation {

    private static final Logger _log =
        LoggerFactory.getLogger(org.dcache.nfs.v4.OperationWRITE.class);

    public EOperationWRITE(nfs_argop4 args) {
      super(args, nfs_opnum4.OP_WRITE);
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result)
        throws ChimeraNFSException, IOException {

      final WRITE4res res = result.opwrite;

      _args.opwrite.offset.checkOverflow(
          _args.opwrite.data.remaining(), "offset + length overflow");


      var inode = context.currentInode();

      long offset = _args.opwrite.offset.value;
      VirtualFileSystem.WriteResult writeResult =
          vfs
              .write(
                  inode,
                  _args.opwrite.data,
                  offset,
                  VirtualFileSystem.StabilityLevel.fromStableHow(_args.opwrite.stable));

      if (writeResult.getBytesWritten() < 0) {
        throw new NfsIoException("IO not allowed");
      }

      res.status = nfsstat.NFS_OK;
      res.resok4 = new WRITE4resok();
      res.resok4.count = new count4(writeResult.getBytesWritten());
      res.resok4.committed = writeResult.getStabilityLevel().toStableHow();
      res.resok4.writeverf = context.getRebootVerifier();
    }
  }
}
