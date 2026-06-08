ARG FAABRIC_VERSION=0.22.0
FROM ghcr.io/faasm/planner:${FAABRIC_VERSION}

ARG LQVD_BRANCH=main

WORKDIR /

RUN rm -rf /code/faabric \
    && git clone \
        https://github.com/lqvd/faabric \
        /code/faabric \
    && cd /code/faabric \
    && ./bin/create_venv.sh \
    && source venv/bin/activate \
    && inv dev.conan --build=Release \
    && inv dev.cmake --build=Release \
    && inv dev.cc planner_server

ENTRYPOINT ["/code/faabric/bin/planner_entrypoint.sh"]