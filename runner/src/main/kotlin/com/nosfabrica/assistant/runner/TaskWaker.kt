package com.nosfabrica.assistant.runner

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.util.Date

/** What a task service answered a wake (PLAN.md, "The trigger contract (v1)"). */
sealed interface WakeAnswer {
    /** `202`: it looks at this owner now. */
    data object Accepted : WakeAnswer

    /** `200 {"outcome": …}`: settled, nothing to do. [outcome] is null when the body wasn't one. */
    class Settled(
        val outcome: String?,
    ) : WakeAnswer

    /** `429`: busy; wake again after [retryAfterSeconds] (null when it gave no `Retry-After`). */
    class Busy(
        val retryAfterSeconds: Long?,
    ) : WakeAnswer

    /** `400`: the service calls the wake malformed. A bug on one side: the vectors should have caught it. */
    data object Malformed : WakeAnswer

    /** Anything else: another status, or no answer. [reason] is ours (`http 503`, an exception's class). */
    class Failed(
        val reason: String,
    ) : WakeAnswer
}

/** Wakes a task service. Tests fake it. */
fun interface TaskWaker {
    suspend fun wake(
        service: String,
        request: WakeRequest,
    ): WakeAnswer
}

/**
 * [TaskWaker] over HTTP: `POST {service}/v1/run`, inside the cluster, no token (a wake only makes
 * the service do what the owner's own signed schedule asks, and the service checks all of it).
 * [client] is the runner's internal one: no redirects, a call timeout.
 */
class HttpTaskWaker(
    private val client: OkHttpClient,
    private val nowMs: () -> Long = System::currentTimeMillis,
) : TaskWaker {
    override suspend fun wake(
        service: String,
        request: WakeRequest,
    ): WakeAnswer =
        withContext(Dispatchers.IO) {
            val http =
                Request
                    .Builder()
                    .url(service.trimEnd('/') + WakeRequest.PATH)
                    .post(request.toJson().toString().toRequestBody(JSON))
                    .build()
            try {
                client.newCall(http).execute().use { response ->
                    when (response.code) {
                        202 -> {
                            WakeAnswer.Accepted
                        }

                        200 -> {
                            // Bounded: an answer is a few bytes, and only an enum name from it is ever logged.
                            val body = response.body.byteStream().use { it.readNBytes(MAX_ANSWER_BYTES) }
                            WakeAnswer.Settled(SettledAnswer.outcomeOf(body.decodeToString()))
                        }

                        429 -> {
                            WakeAnswer.Busy(retryAfter(response.header("Retry-After"), response.headers.getDate("Retry-After")))
                        }

                        400 -> {
                            WakeAnswer.Malformed
                        }

                        else -> {
                            WakeAnswer.Failed("http ${response.code}")
                        }
                    }
                }
            } catch (e: IOException) {
                WakeAnswer.Failed(e.javaClass.simpleName)
            }
        }

    /** `Retry-After` as seconds, or as an HTTP date (RFC 9110 allows both); null when it is neither. */
    private fun retryAfter(
        value: String?,
        date: Date?,
    ): Long? {
        value?.trim()?.toLongOrNull()?.let { return it.coerceAtLeast(0) }
        return date?.let { ((it.time - nowMs()) / 1000).coerceAtLeast(0) }
    }

    companion object {
        private val JSON = "application/json".toMediaType()
        private const val MAX_ANSWER_BYTES = 16 * 1024
    }
}
