package com.nosfabrica.assistant.runner

import com.nosfabrica.assistant.core.AssistantKeys
import com.nosfabrica.assistant.core.Db
import com.nosfabrica.assistant.core.FernetKeyFile
import com.nosfabrica.assistant.core.LeaderLock
import com.nosfabrica.assistant.core.Migrations
import com.nosfabrica.assistant.core.PostgresRunStore
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
                indexRelays = opt("INDEX_RELAYS")?.split(',')?.map { it.trim() }?.filter { it.isNotEmpty() } ?: listOf(DEFAULT_INDEX_RELAY),
                services = services,
                pollSeconds = seconds("POLL_SECONDS", 300, 30),
                wakeRetrySeconds = seconds("WAKE_RETRY_SECONDS", Runner.DEFAULT_WAKE_RETRY_SECONDS, 60),
            )
        }
    }
}

/** What an owner's relay socket may read per [RELAY_READ_WINDOW_MS]: far over any schedule list, far under the heap. */
const val RELAY_READ_BUDGET_BYTES = 16L * 1024 * 1024

const val RELAY_READ_WINDOW_MS = 10_000L

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
    // The task services are inside the cluster: the one unguarded client, and it follows nothing.
    val serviceClient =
        base
            .newBuilder()
            .followRedirects(false)
            .followSslRedirects(false)
            .callTimeout(30, TimeUnit.SECONDS)
            .build()
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
    val runner =
        Runner(
            directory = AssistantKeys(config.db, keys),
            nostr = RunnerNostr(QuartzRelayAccess(client), index, search),
            runs = PostgresRunStore(config.db),
            waker = HttpTaskWaker(serviceClient),
            services = config.services,
            wakeRetrySeconds = config.wakeRetrySeconds,
            accepting = { !stopping.get() },
        )
    val loop =
        RunnerLoop(
            tryAcquire = { lock.tryAcquireAsync() },
            isAlive = { withContext(Dispatchers.IO) { lock.isAlive() } },
            migrate = { Migrations.migrate(config.db) },
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

    Log.i(TAG) { "started: ${keys.keyCount()} Fernet keys, ${index.size} index relays, tasks ${config.services.tasks.sorted()}" }
    val end =
        try {
            runBlocking { scope.async { loop.run() }.await() }
        } catch (e: Exception) {
            // Cancelled by the shutdown hook, or the migration failed: the class only.
            if (!stopping.get()) Log.e(TAG) { "stopped: ${e.javaClass.simpleName}" }
            if (stopping.get()) LoopEnd.STOPPED else null
        } finally {
            lock.close()
            client.disconnect()
            done.countDown()
        }
    // 4: the lock was lost; 5: the migration or the loop failed. Either way the orchestrator restarts it.
    if (end == LoopEnd.LEADERSHIP_LOST) exitProcess(4)
    if (end == null) exitProcess(5)
}

/** How long SIGTERM waits for the loop to notice, and then again after cancelling it. */
private const val STOP_GRACE_SECONDS = 10L
