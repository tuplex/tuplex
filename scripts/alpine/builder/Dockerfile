FROM tuplex/musl:latest

# mount tuplex in /code.

# mount wheelhouse in /wheelhouse.

WORKDIR /work

COPY build.sh /work/build.sh
RUN chmod +x /work/build.sh

ENTRYPOINT ["/work/build.sh"]
