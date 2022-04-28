package org.dcache.vfs4j;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.ExportTable;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v4.MDSOperationExecutor;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import picocli.CommandLine;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.File;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "nfs4j", mixinStandardHelpOptions = true, version = "0.0.1")
public class NfsMain implements Callable<Void> {

  @CommandLine.Option(
      names = {"--cert"},
      description = "PEM encoded host certificate",
      defaultValue = "hostcert.pem")
  private String cert;

  @CommandLine.Option(
      names = {"--key"},
      description = "PEM encoded host key",
      defaultValue = "hostkey.pem")
  private String key;

  @CommandLine.Option(
      names = "--ca-chain",
      description = "Trusted CA chain",
      defaultValue = "ca-chain.pem")
  private String chain;

  @CommandLine.Option(
      names = {"--with-tls"},
      description = "Enable RPC-over-TLS",
      defaultValue = "false")
  private boolean tls;

  @CommandLine.Option(
          names = {"--with-mutual-tls"},
          description = "Enable mutual TLS authentication",
          defaultValue = "false")
  private boolean mutual;

  @CommandLine.Parameters(index = "0", description = "directory to export")
  private File dir;

  @CommandLine.Parameters(index = "1", description = "path to export file")
  private File export;

  public static void main(String[] args) throws Exception {
    new CommandLine(new NfsMain()).execute(args);
  }

  public Void call() throws Exception {

    SSLParameters sslParameters = null;
    SSLContext sslContext = null;
    if (tls) {
      sslContext = TlsUtils.createSslContext(cert, key, new char[0], chain);
      sslParameters = sslContext.getDefaultSSLParameters();
      sslParameters.setNeedClientAuth(mutual);
    }

    VirtualFileSystem vfs = new LocalVFS(dir);
    OncRpcSvc nfsSvc =
        new OncRpcSvcBuilder()
            .withPort(2049)
            .withTCP()
            .withAutoPublish()
            .withWorkerThreadIoStrategy()
            .withStartTLS()
            .withSSLContext(sslContext)
            .withSSLParameters(sslParameters)
            .build();

    ExportTable exportFile = new ExportFile(export);

    NFSServerV41 nfs4 =
        new NFSServerV41.Builder()
            .withExportTable(exportFile)
            .withVfs(vfs)
            .withOperationExecutor(new MDSOperationExecutor())
            .build();

    NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
    MountServer mountd = new MountServer(exportFile, vfs);

    nfsSvc.register(new OncRpcProgram(100003, 3), nfs3);
    nfsSvc.register(new OncRpcProgram(100005, 3), mountd);

    nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
    nfsSvc.start();

    Thread.currentThread().join();
    return null;
  }
}
