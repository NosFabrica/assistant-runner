plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
    // The throwaway Postgres cluster the tests here and in :runner start.
    `java-test-fixtures`
    // JitPack builds `publishToMavenLocal` (jitpack.yml): the Observer will depend on :core.
    `maven-publish`
}

kotlin {
    jvmToolchain(21)
}

// JitPack versions are commit hashes, and hashes carry no order. Gradle keeps the
// lexicographically higher one when two parts of the graph disagree, which for hashes means an
// arbitrary one chosen silently. force() is the only thing that makes the pin mean what it says.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    api(libs.quartz)
    api(libs.kotlinx.coroutines.core)
    api(libs.kotlinx.serialization.json)
    implementation(libs.postgresql)

    testFixturesApi(libs.kotlinx.coroutines.core)
    testFixturesImplementation(kotlin("test-junit5"))
    testFixturesImplementation(libs.postgresql)

    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
}

java {
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("core") {
            artifactId = "core"
            from(components["java"])
        }
    }
}

tasks.test {
    useJUnitPlatform()
    // The trigger contract's vectors, read where they live, at the repository root.
    val vectors = rootProject.file("contract/v1/vectors")
    inputs.dir(vectors)
    systemProperty("contract.vectors", vectors.absolutePath)
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
