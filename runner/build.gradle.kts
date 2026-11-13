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

// Same rule as :core: JitPack versions are commit hashes, which carry no order.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    implementation(project(":core"))
    implementation(libs.okhttp)

    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(testFixtures(project(":core")))
}

tasks.test {
    useJUnitPlatform()
    val vectors = rootProject.file("contract/v1/vectors")
    inputs.dir(vectors)
    systemProperty("contract.vectors", vectors.absolutePath)
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
