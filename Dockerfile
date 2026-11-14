# The Brainstorm Assistants' scheduler. Build from the repository root:
#   docker build -t assistant-runner .
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew :runner:installDist --no-daemon --console=plain

FROM eclipse-temurin:21-jre
COPY --from=build /src/runner/build/install/assistant-runner /opt/assistant-runner
# No volume and no state of its own: the leader lock is a Postgres session lock, and the rest (the
# last wake and the settled occurrences) is in memory, which a restart costs one wake to rebuild.
USER 65534:65534
ENTRYPOINT ["/opt/assistant-runner/bin/assistant-runner"]
