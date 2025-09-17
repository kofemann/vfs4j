# VFS4J

This project implements a simple NFS (Network File System) server using the [nfs4j](https://github.com/dcache/nfs4j) library.
It supports NFS versions 3 and 4.1, with optional pNFS (Parallel NFS) support. The server is
backed by a local file system utilizing  Linux's `open-by-handle` API called via Java 22
[Foreign Function and Memory API][1].

> [!WARNING]
> THIS NFS SERVER IS NOT INTENDED FOR PRODUCTION USE.

### Features

NFSv3 and NFSv4.1 Support: Provides compatibility with both NFS versions.
pNFS Support: Enables parallel file access for improved performance.
TLS Support: Secure communication using RPC-over-TLS with optional mutual authentication.
Configurable Options: Allows customization of ports, export directories, and security settings.

## Usage

### Command-Line Options

| Option                  | Description                                                                                     | Default Value       |
|-------------------------|-------------------------------------------------------------------------------------------------|---------------------|
| **--cert**              | Path to the PEM-encoded host certificate                                                       | `hostcert.pem`      |
| **--key**               | Path to the PEM-encoded host key                                                               | `hostkey.pem`       |
| **--ca-chain**          | Path to the trusted CA chain                                                                   | `ca-chain.pem`      |
| **--with-tls**          | Enable RPC-over-TLS                                                                            | `false`             |
| **--with-mutual-tls**   | Enable mutual TLS authentication                                                               | `false`             |
| **--insecure**          | Skip TLS certificate chain verification step                                                   | `false`             |
| **--with-v3**           | Enable NFS version 3                                                                           | `false`             |
| **--with-v4**           | Enable NFS version 4.1                                                                         | `true`              |
| **--pnfs**              | Enable pNFS                                                                                    | `true`              |
| **--port**              | Specify NFS server port to listen on                                                           | `2049`              |
| **--ds-port**           | Specify pNFS DS port to listen on                                                              | `2053`              |
| **directory to export** | Directory to export                                                                            |                     |
| **path to export file** | Path to the export file |

```bash
sudo java --enable-native-access=ALL-UNNAMED \
    -jar target/vfs4j-1.0-SNAPSHOT.jar /directory/to/export /path/to/export/file
```

The **exports** file is compatible with the standard **/etc/exports** file format.


## Building

To build the project, you need to have [Maven](https://maven.apache.org/) and java 24+ installed.
A native image can be built with [GraalVM](https://graalvm.org) using the following command:

```bash
export JAVA_HOME=/path/to/graalvm
mvn package -Pnative
````
## How to contribute

**VFS4J** uses the linux kernel model where git is not only the source repository,
but also the way to track contributions and copyrights.

Each submitted patch must have a "Signed-off-by" line.  Patches without
this line will not be accepted.

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as an open-source patch.  The rules are pretty simple: if you
can certify the below:
```
    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
         have the right to submit it under the open source license
         indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.
```
Then you add a line saying ( git commit -s )

    Signed-off-by: Random J Developer <random@developer.example.org>

using your real name (sorry, no pseudonyms or anonymous contributions.)

## License

This work is published under the [Apache License 2.0][2]

[1]: https://docs.oracle.com/en/java/javase/22/core/foreign-function-and-memory-api.html
[2]: LICENSE
