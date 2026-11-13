plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.nosfabrica.assistant.runner.MainKt")
    applicationName = "assistant-runner"
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
    implementation(libs.quartz)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.okhttp)
    implementation(libs.postgresql)

    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
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
