package com.nosfabrica.assistant.runner

import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** What a server an owner named can make the runner hold. The rest of the worker's limits (prints, run records) stayed in the Observer. */
class RunnerLimitsTest {
    private fun answering(
        code: Int,
        body: ByteArray,
        type: String = "text/event-stream",
    ) = Interceptor { chain ->
        Response
            .Builder()
            .request(chain.request())
            .protocol(Protocol.HTTP_1_1)
            .code(code)
            .message("x")
            .body(body.toResponseBody(type.toMediaType()))
            .build()
    }

    private fun get(client: OkHttpClient) = client.newCall(Request.Builder().url("https://ai.example.com/v1/models").build()).execute()

    @Test
    fun aBodyPastTheCapFailsAndAnErrorBodyIsTrimmed() {
        val cap = BodyCapInterceptor(maxBytes = 1_000, maxErrorBytes = 100)
        val big =
            OkHttpClient
                .Builder()
                .addInterceptor(cap)
                .addInterceptor(answering(200, ByteArray(5_000) { 'a'.code.toByte() }))
                .build()
        assertFailsWith<IOException> { get(big).use { it.body.string() } }

        val small =
            OkHttpClient
                .Builder()
                .addInterceptor(cap)
                .addInterceptor(answering(200, ByteArray(900)))
                .build()
        assertEquals(900, get(small).use { it.body.bytes().size })

        val error =
            OkHttpClient
                .Builder()
                .addInterceptor(cap)
                .addInterceptor(answering(500, ByteArray(50_000)))
                .build()
        assertEquals(100, get(error).use { it.body.bytes().size })
    }

    @Test
    fun theGuardedClientCapsBodies() {
        assertTrue(guardedClient(OkHttpClient()).interceptors.any { it is BodyCapInterceptor })
    }

    @Test
    fun theGuardedClientNeverInflatesAWebSocketMessage() {
        assertTrue(guardedClient(OkHttpClient(), setOf("wss")).interceptors.any { it is NoWebSocketCompression })
    }

    @Test
    fun whatOwnersRelaysCanMakeOnePollHoldStaysUnder128Mb() {
        // README.md and PLAN.md give this number: change them with it.
        assertEquals(96L * 1024 * 1024, RELAY_WORST_CASE_BYTES)
    }
}
