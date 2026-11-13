package com.nosfabrica.assistant.runner

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull

class RunnerConfigTest {
    private val base = mapOf("DATABASE_URL" to "jdbc:postgresql://db:5432/brainstorm", "TASK_URL_OBSERVER" to "http://observer:8080/")

    @Test
    fun thePlansDefaults() {
        val config = RunnerConfig.fromEnv(base)
        assertEquals("/run/secrets/nsec_encryption_keys", config.keysFile)
        assertEquals(listOf("wss://search.brainstorm.world"), config.indexRelays)
        assertEquals(300, config.pollSeconds)
        assertEquals(600, config.wakeRetrySeconds)
        assertEquals("http://observer:8080", config.services.serviceFor("observer"))
        assertNull(config.services.serviceFor("Observer"))
        assertNull(config.services.serviceFor("digest"))
    }

    @Test
    fun whatItRefuses() {
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base - "DATABASE_URL") }
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base - "TASK_URL_OBSERVER") }
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base + ("TASK_URL_OBSERVER" to "observer:8080")) }
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base + ("POLL_SECONDS" to "5")) }
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base + ("WAKE_RETRY_SECONDS" to "soon")) }
        // Every index relay must be a wss relay, and there must be one.
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base + ("INDEX_RELAYS" to "wss://a.example,ws://b.example")) }
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base + ("INDEX_RELAYS" to "wss://a.example,not a relay")) }
        assertFailsWith<IllegalArgumentException> { RunnerConfig.fromEnv(base + ("INDEX_RELAYS" to " , ")) }
        assertEquals(
            listOf("wss://a.example", "wss://b.example"),
            RunnerConfig.fromEnv(base + ("INDEX_RELAYS" to "wss://a.example, wss://b.example")).indexRelays,
        )
    }

    @Test
    fun thePasswordIsNotInTheDatabasesName() {
        val config =
            RunnerConfig.fromEnv(
                base + ("DATABASE_URL" to "jdbc:postgresql://db/brainstorm?password=hunter2") + ("DATABASE_PASSWORD" to "s3cret"),
            )
        assertFalse("hunter2" in config.db.toString())
        assertFalse("s3cret" in config.db.toString())
    }
}
