# Run as:
#   docker run -ti --rm --privileged --name nfsd -p 2049:2049 dcache/nfs4j

FROM openjdk:19 as jre-build

# for objcopy
#RUN apk add --no-cache binutils

RUN $JAVA_HOME/bin/jlink \
         --add-modules ALL-MODULE-PATH \
         --strip-debug \
         --no-man-pages \
         --no-header-files \
         --compress=2 \
         --output /javaruntime


FROM debian:stable-slim

COPY --from=jre-build /javaruntime /opt/openjdk

# where we store the data
RUN mkdir -p /opt/nfs4j /data
RUN echo '/ *(rw,no_root_squash)' > /opt/nfs4j/exports

# Add JARS
COPY maven /opt/nfs4j/jars


# expose TCP ports for network services
EXPOSE 2049

VOLUME /data
WORKDIR /opt/nfs4j

CMD ["/opt/openjdk/bin/java", "--enable-preview", "-cp", "/opt/nfs4j/jars/*", "org.dcache.vfs4j.NfsMain", "/data", "exports"]
