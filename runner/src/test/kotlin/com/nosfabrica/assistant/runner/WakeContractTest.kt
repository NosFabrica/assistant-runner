package com.nosfabrica.assistant.runner

import com.nosfabrica.assistant.core.WakeRequest
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import okhttp3.Headers
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import okio.Buffer
import java.io.File
import java.io.IOException
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

/** [HttpTaskWaker] against contract/v1's vectors, behind an OkHttp interceptor: no socket is ever opened. */
class WakeContractTest {
    private val vectors = File(System.getProperty("contract.vectors") ?: "../contract/v1/vectors")

    private val sent = mutableListOf<Request>()
    private val bodies = mutableListOf<String>()

    private fun waker(
        code: Int,
        body: String = "",
        headers: Map<String, String> = emptyMap(),
        nowMs: Long = System.currentTimeMillis(),
    ) = HttpTaskWaker(
        OkHttpClient
            .Builder()
            .addInterceptor(
                Interceptor { chain ->
                    val request = chain.request()
                    sent += request
                    bodies += Buffer().also { request.body!!.writeTo(it) }.readUtf8()
                    Response
                        .Builder()
                        .request(request)
                        .protocol(Protocol.HTTP_1_1)
                        .code(code)
                        .message("x")
                        .apply { headers.forEach { (k, v) -> header(k, v) } }
                        .body(body.toResponseBody("application/json".toMediaType()))
                        .build()
                },
            ).build(),
        nowMs = { nowMs },
    )

    private val wake = WakeRequest("observer", "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798", 1763020800)

    @Test
    fun theWakeIsTheVector() =
        runTest {
            assertEquals(WakeAnswer.Accepted, waker(202).wake("http://observer:8080/", wake))
            assertEquals("POST", sent.single().method)
            assertEquals("http://observer:8080/v1/run", sent.single().url.toString())
            assertEquals(Json.parseToJsonElement(File(vectors, "wake-observer.json").readText()), Json.parseToJsonElement(bodies.single()))
        }

    @Test
    fun aSettledAnswerCarriesItsOutcome() =
        runTest {
            val answer = waker(200, File(vectors, "answer-not-due.json").readText()).wake("http://observer:8080", wake)
            assertEquals("NOT_DUE", assertIs<WakeAnswer.Settled>(answer).outcome)
            // A body that isn't an answer is still settled, with nothing to log from it.
            assertNull(assertIs<WakeAnswer.Settled>(waker(200, "<html>").wake("http://observer:8080", wake)).outcome)
        }

    @Test
    fun busyReadsRetryAfterInSecondsOrAsADate() =
        runTest {
            assertEquals(
                120L,
                assertIs<WakeAnswer.Busy>(waker(429, headers = mapOf("Retry-After" to "120")).wake("http://s", wake)).retryAfterSeconds,
            )
            val now = 1_800_000_000_000L
            val date = Headers.Builder().add("Retry-After", Date(now + 300_000)).build()["Retry-After"]!!
            assertEquals(
                300L,
                assertIs<WakeAnswer.Busy>(
                    waker(429, headers = mapOf("Retry-After" to date), nowMs = now).wake("http://s", wake),
                ).retryAfterSeconds,
            )
            assertNull(assertIs<WakeAnswer.Busy>(waker(429).wake("http://s", wake)).retryAfterSeconds)
        }

    @Test
    fun malformedAndFailures() =
        runTest {
            assertEquals(WakeAnswer.Malformed, waker(400).wake("http://s", wake))
            assertEquals("http 503", assertIs<WakeAnswer.Failed>(waker(503).wake("http://s", wake)).reason)
            val down =
                HttpTaskWaker(
                    OkHttpClient.Builder().addInterceptor(Interceptor { throw IOException("connection refused to secret") }).build(),
                )
            assertEquals("IOException", assertIs<WakeAnswer.Failed>(down.wake("http://s", wake)).reason)
        }
}
