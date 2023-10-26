FROM scratch

COPY ./diener /bin/diener

ENTRYPOINT ["diener"]
