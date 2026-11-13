# The Brainstorm Assistants' scheduler. Build from the repository root:
#   docker build -t assistant-runner .
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew :runner:installDist --no-daemon --console=plain

FROM eclipse-temurin:21-jre
COPY --from=build /src/runner/build/install/assistant-runner /opt/assistant-runner
# No volume: the runner's state is in Postgres (the leader lock, the runs it reads) and in
# memory (the last wake per occurrence, which a restart only repeats once).
USER 65534:65534
ENTRYPOINT ["/opt/assistant-runner/bin/assistant-runner"]
