package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess

/** The runner's settings, all from the environment (PLAN.md, "Configuration"). The database password is never logged. */
class RunnerConfig(
    val db: Db,
    val keysFile: String,
    val indexRelays: List<String>,
    val services: TaskServices,
    val pollSeconds: Long,
    val wakeRetrySeconds: Long,
) {
    companion object {
        /** Brainstorm's index relay, which is also its search relay: asked with `include:spam`. */
        const val DEFAULT_INDEX_RELAY = "wss://search.brainstorm.world"

        /**
         * `INDEX_RELAYS`: every entry a `wss` relay, and at least one. A relay that didn't
         * normalise would be dropped in silence, and with none left every owner is `NO_QUORUM`.
         */
        private fun indexRelays(raw: String): List<String> {
            val relays = raw.split(',').map { it.trim() }.filter { it.isNotEmpty() }
            require(relays.isNotEmpty()) { "INDEX_RELAYS names no relay" }
            for (r in relays) {
                val normal = RelayUrlNormalizer.normalizeOrNull(r)
                require(normal != null && normal.url.startsWith("wss://")) { "INDEX_RELAYS: every relay must be a wss:// URL" }
            }
            return relays
        }

        fun fromEnv(env: Map<String, String> = System.getenv()): RunnerConfig {
            fun opt(name: String) = env[name]?.trim()?.takeIf { it.isNotEmpty() }

            fun seconds(
                name: String,
                default: Long,
                min: Long,
            ): Long {
                val raw = opt(name) ?: return default
                val n = raw.toLongOrNull() ?: throw IllegalArgumentException("$name is not a number of seconds")
                require(n >= min) { "$name must be at least $min" }
                return n
            }
            val services = TaskServices.fromEnv(env)
            require(services.tasks.isNotEmpty()) { "no task service: set TASK_URL_<TASK>, e.g. TASK_URL_OBSERVER=http://observer:8080" }
            return RunnerConfig(
                db = Db.fromEnv(env),
                keysFile = opt("NSEC_KEYS_FILE") ?: FernetKeyFile.DEFAULT_PATH,
                indexRelays = opt("INDEX_RELAYS")?.let(::indexRelays) ?: listOf(DEFAULT_INDEX_RELAY),
                services = services,
                pollSeconds = seconds("POLL_SECONDS", 300, 30),
                wakeRetrySeconds = seconds("WAKE_RETRY_SECONDS", Runner.DEFAULT_WAKE_RETRY_SECONDS, 60),
            )
        }
    }
}

/**
 * What an owner's relay socket may read per [RELAY_READ_WINDOW_MS]: far over a page of schedules
 * (at most [QuartzRelayAccess.MAX_EVENTS_PER_RELAY] events of about 1 KB), far under the heap.
 * Frames are never compressed ([NoWebSocketCompression]), so these bytes are the message's.
 */
const val RELAY_READ_BUDGET_BYTES = 4L * 1024 * 1024

const val RELAY_READ_WINDOW_MS = 10_000L

/**
 * The most an owner's relays can make one poll hold at once: [RunnerNostr.RELAY_PARALLELISM]
 * relays, each read for at most [QuartzRelayAccess.FETCH_DEADLINE_MS], which spans three budget
 * windows. 8 × 3 × 4 MB = 96 MB of frames, about twice that once decoded: under a 512 MB heap.
 */
const val RELAY_WORST_CASE_BYTES =
    RunnerNostr.RELAY_PARALLELISM * (QuartzRelayAccess.FETCH_DEADLINE_MS / RELAY_READ_WINDOW_MS + 1) * RELAY_READ_BUDGET_BYTES

private const val TAG = "AssistantRunner"

fun main() {
    val config =
        try {
            RunnerConfig.fromEnv()
        } catch (e: IllegalArgumentException) {
            System.err.println("assistant-runner: ${e.message}")
            exitProcess(2)
        }
    Log.minLevel = LogLevel.INFO

    val stopping = AtomicBoolean(false)
    val done = CountDownLatch(1)
    val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    val lock = LeaderLock(config.db)
    val keys = FernetKeyFile(Paths.get(config.keysFile))

    val base =
        OkHttpClient
            .Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .build()
    val serviceClient = serviceClient(base)
    // An owner's relays came from the owner: each socket goes through the guard and reads within a
    // budget. The index relays are Brainstorm's own and aren't held to the budget.
    val guardedSockets = guardedClient(base, setOf("wss"))
    val budgetedSockets = guardedClient(base, setOf("wss"), readBudget = ReadBudget(RELAY_READ_BUDGET_BYTES, RELAY_READ_WINDOW_MS))
    val index = RunnerNostr.relaysOf(config.indexRelays)
    val search = RelayUrlNormalizer.normalizeOrNull(RunnerConfig.DEFAULT_INDEX_RELAY)?.takeIf { it in index }
    val client =
        NostrClient(
            BasicOkHttpWebSocket.Builder(
                { if (it in index) guardedSockets else budgetedSockets },
                { UrlGuard.refusal(it.url, setOf("wss")) == null },
            ),
            scope,
        )
    val leadership = LeadershipGate({ withContext(Dispatchers.IO) { lock.isAlive() } })
    val runner =
        Runner(
            directory = AssistantKeys(config.db, keys),
            nostr = RunnerNostr(QuartzRelayAccess(client), index, search),
            waker = HttpTaskWaker(serviceClient),
            services = config.services,
            wakeRetrySeconds = config.wakeRetrySeconds,
            // Before every wake: not stopping, and still the leader (asked again when the last answer is over 5 s old).
            accepting = { !stopping.get() && leadership.holds() },
        )
    val loop =
        RunnerLoop(
            tryAcquire = { lock.tryAcquireAsync() },
            isAlive = { withContext(Dispatchers.IO) { lock.isAlive() } },
            poll = {
                val outcomes = runner.poll()
                Log.i(TAG) { "poll done: ${outcomes.size} schedules, ${outcomes.values.groupingBy { it }.eachCount()}" }
            },
            stopping = { stopping.get() },
            pollMs = config.pollSeconds * 1000,
        )

    // SIGTERM: no new wake; the poll under way is cut short (a wake is harmless to interrupt), and
    // the lock goes with its connection.
    Runtime.getRuntime().addShutdownHook(
        Thread {
            stopping.set(true)
            Log.i(TAG) { "stopping" }
            if (!done.await(STOP_GRACE_SECONDS, TimeUnit.SECONDS)) {
                scope.cancel()
                done.await(STOP_GRACE_SECONDS, TimeUnit.SECONDS)
            }
        },
    )

    val keyCount = keys.keyCount()
    Log.i(TAG) { "started: $keyCount Fernet keys, ${index.size} index relays, tasks ${config.services.tasks.sorted()}" }
    // Not fatal: the server may write the file after this starts. Until then every encrypted key is NSEC_UNREADABLE.
    if (keyCount == 0) Log.w(TAG) { "the key file (NSEC_KEYS_FILE) holds no Fernet key: no encrypted Assistant key will open" }
    val end =
        try {
            runBlocking { scope.async { loop.run() }.await() }
        } catch (e: Exception) {
            // Cancelled by the shutdown hook, or the loop failed: the class only.
            if (!stopping.get()) Log.e(TAG) { "stopped: ${e.javaClass.simpleName}" }
            if (stopping.get()) LoopEnd.STOPPED else null
        } finally {
            lock.close()
            client.disconnect()
            done.countDown()
        }
    // 4: the lock was lost; 5: the loop failed. Either way the orchestrator restarts it.
    if (end == LoopEnd.LEADERSHIP_LOST) exitProcess(4)
    if (end == null) exitProcess(5)
}

/**
 * The client for the task services, inside the cluster: the one unguarded client, and it follows
 * nothing. A wake's answer is immediate (a `202` only queues the run), so 5 s to connect and 10 s
 * for the whole call are plenty, and a hung service costs a poll that much at most.
 */
fun serviceClient(base: OkHttpClient): OkHttpClient =
    base
        .newBuilder()
        .followRedirects(false)
        .followSslRedirects(false)
        .connectTimeout(5, TimeUnit.SECONDS)
        .callTimeout(10, TimeUnit.SECONDS)
        .build()

/** How long SIGTERM waits for the loop to notice, and then again after cancelling it. */
private const val STOP_GRACE_SECONDS = 10L
