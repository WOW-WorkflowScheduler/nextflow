FROM gcc AS scheduler-script
COPY getStatsAndResolveSymlinks.c /build/getStatsAndResolveSymlinks.c
RUN gcc -static /build/getStatsAndResolveSymlinks.c -o /build/getStatsAndResolveSymlinks

FROM amazoncorretto:17.0.7
RUN yum install -y procps-ng shadow-utils

ENV NXF_HOME=/.nextflow
ARG TARGETPLATFORM=linux/amd64

# copy docker client
COPY dist/${TARGETPLATFORM}/docker /usr/local/bin/docker
COPY entry.sh /usr/local/bin/entry.sh
COPY nextflow /usr/local/bin/nextflow
COPY --from=scheduler-script /build/getStatsAndResolveSymlinks /usr/local/bin/getStatsAndResolveSymlinks

# download runtime
RUN mkdir /.nextflow \
 && touch /.nextflow/dockerized \
 && chmod 755 /usr/local/bin/nextflow \
 && chmod 755 /usr/local/bin/entry.sh \
 && nextflow info

# define the entry point
ENTRYPOINT ["/usr/local/bin/entry.sh"]
