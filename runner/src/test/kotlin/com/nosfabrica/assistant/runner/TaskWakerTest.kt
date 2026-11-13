package com.nosfabrica.assistant.runner

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertTrue

/** A task service that takes the wake and never answers: the wake ends, and can be cancelled. Loopback only. */
class TaskWakerTest {
    private val server = ServerSocket(0, 10, InetAddress.getLoopbackAddress())

    init {
        thread(isDaemon = true) {
            while (!server.isClosed) {
                try {
                    server.accept()
                } catch (e: Exception) {
                    return@thread
                }
            }
        }
    }

    @AfterTest
    fun close() = server.close()

    private val service = "http://127.0.0.1:${server.localPort}"
    private val wake = WakeRequest("observer", "a".repeat(64), 1)

    @Test
    fun aHungServiceIsGivenUpOnByTheCallTimeout() =
        runBlocking {
            val client = OkHttpClient.Builder().callTimeout(1, TimeUnit.SECONDS).build()
            val answer = withTimeout(10_000) { HttpTaskWaker(client).wake(service, wake) }
            assertTrue(answer is WakeAnswer.Failed)
        }

    @Test
    fun aWakeIsCancelledWithItsCoroutine() =
        runBlocking {
            val client = OkHttpClient.Builder().readTimeout(60, TimeUnit.SECONDS).build()
            val started = System.nanoTime()
            val call = async(Dispatchers.Default) { HttpTaskWaker(client).wake(service, wake) }
            delay(300)
            call.cancel()
            withTimeout(5_000) { call.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 5_000, "took $tookMs ms")
        }

    @Test
    fun theServiceClientHasShortTimeouts() {
        val client = serviceClient(OkHttpClient())
        assertTrue(client.connectTimeoutMillis <= 5_000)
        assertTrue(client.callTimeoutMillis in 1..10_000)
        assertTrue(!client.followRedirects)
    }
}
