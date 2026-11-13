package com.nosfabrica.assistant.runner

import com.nosfabrica.assistant.core.AssistantDirectory
import com.nosfabrica.assistant.core.AssistantKey
import com.nosfabrica.assistant.core.AssistantPair
import com.nosfabrica.assistant.core.KeyFailure
import com.nosfabrica.assistant.core.RunStore
import com.nosfabrica.assistant.core.WakeRequest
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nipFBScheduledTasks.CronZone
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTask
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTaskClaims
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTaskClaims.OwnRun
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTaskEvent
import com.vitorpamplona.quartz.nipFBScheduledTasks.ZoneIdCronZone
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.TimeUtils
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap

/** How one schedule went on one poll. Logged by name, with the owner: never a key, a plaintext or the task's name. */
enum class RunnerOutcome {
    /** The service answered `202`: it looks at the owner now. */
    WOKEN,

    /** The service answered `200`: settled, nothing to do. */
    SETTLED,

    /** The service answered `429`: woken again after its `Retry-After`. */
    BUSY,

    /** The service answered `400`: a contract bug, on one side or the other. */
    WAKE_REFUSED,

    /** The service didn't answer, or answered something else. Woken again after `WAKE_RETRY_SECONDS`. */
    WAKE_FAILED,

    /** This occurrence was woken less than `WAKE_RETRY_SECONDS` ago (or the service's `Retry-After` hasn't passed). */
    WOKEN_RECENTLY,

    /** The owner's newest schedule names no runner: their devices run it. */
    NOT_NAMED,

    /** More than one public `p`: NIP-FB's conflict, nobody runs it. */
    RUNNER_CONFLICT,

    /** `checkRunner` refused it: not authored by the owner, naming another key than their Assistant, or a bad signature. */
    REJECTED,

    /** Too few of the owner's write relays answered to trust the schedule read is the newest. */
    NO_QUORUM,

    /** The Assistant's key: no row in the view, a token no Fernet key opens, or an nsec that isn't the stored Assistant. */
    NO_ASSISTANT,
    NSEC_UNREADABLE,
    NSEC_MISMATCH,

    /** The private tags don't decrypt with the Assistant's key and the owner as peer, or aren't a tag array. */
    UNREADABLE,

    /** No service is configured for the schedule's `task` (`TASK_URL_<TASK>`). */
    UNKNOWN_TASK,
    UNKNOWN_ZONE,
    NOT_RUNNABLE,
    NOT_DUE,

    /** `assistant_runner.runs` has a row for the occurrence: the service started (or ran) it. */
    ATTEMPTED,

    /** A device of the owner holds a live claim on the occurrence. */
    CLAIMED_BY_DEVICE,

    /** A device of the owner finished it (`ran`). */
    ALREADY_DONE,

    /** The runner is stopping: no new wake. */
    STOPPING,
    ERROR,
}

/** One schedule: the owner and its public `d`. */
data class ScheduleKey(
    val owner: HexKey,
    val d: String,
)

/**
 * The task services by task name, from `TASK_URL_<TASK>`: `TASK_URL_OBSERVER=http://observer:8080`
 * serves the task `observer`. A task whose name an environment variable can't spell (a `-` in it)
 * has no service.
 */
class TaskServices(
    private val urls: Map<String, String>,
) {
    fun serviceFor(task: String?): String? = task?.let { urls[it] }

    val tasks: Set<String> get() = urls.keys

    companion object {
        const val PREFIX = "TASK_URL_"

        fun fromEnv(env: Map<String, String>): TaskServices =
            TaskServices(
                env
                    .filterKeys { it.startsWith(PREFIX) && it.length > PREFIX.length }
                    .mapKeys { it.key.removePrefix(PREFIX).lowercase() }
                    .mapValues { it.value.trim().trimEnd('/') }
                    .filterValues { it.isNotEmpty() }
                    .onEach { (task, url) ->
                        require(WakeRequest.isTaskName(task)) { "$PREFIX${task.uppercase()} doesn't name a task" }
                        require(
                            url.startsWith("http://") || url.startsWith("https://"),
                        ) { "$PREFIX${task.uppercase()} must be an http(s) URL" }
                    },
            )
    }
}

/**
 * The scheduler: one [poll] reads every owner's schedules that name their Assistant, decrypts
 * them with the Assistant's key, decides with Quartz's runner rules, and wakes the task's service
 * for an occurrence that is due. It signs nothing, spends nothing and writes nothing about runs:
 * "already started here" is a row the service wrote in [runs].
 */
class Runner(
    private val directory: AssistantDirectory,
    private val nostr: RunnerNostr,
    private val runs: RunStore,
    private val waker: TaskWaker,
    private val services: TaskServices,
    private val zoneOf: (String?) -> CronZone? = { it?.let(ZoneIdCronZone::of) },
    private val now: () -> Long = { TimeUtils.now() },
    /** At most one wake per (task, owner, occurrence) this often, until the service writes a `runs` row. */
    private val wakeRetrySeconds: Long = DEFAULT_WAKE_RETRY_SECONDS,
    /** How many schedules are decided (and woken) at once. */
    private val parallelism: Int = PARALLELISM,
    /** False once the runner is stopping: no new wake. */
    private val accepting: () -> Boolean = { true },
) {
    private data class WakeKey(
        val task: String,
        val owner: HexKey,
        val occurrence: Long,
    )

    /**
     * When each occurrence may be woken again. In memory: a restart only means one extra wake,
     * which the service refuses as settled.
     */
    private val nextWake = ConcurrentHashMap<WakeKey, Long>()

    /** Each schedule's merged tags by event id: an event never changes, so neither does its plaintext. Never logged. */
    private val decrypted = ConcurrentHashMap<String, ScheduledTask>()

    /** One pass over every schedule. Returns each schedule's outcome, for tests and the log. */
    suspend fun poll(): Map<ScheduleKey, RunnerOutcome> {
        val pairs = directory.pairs()
        val byOwner = pairs.associateBy { it.owner }
        val reads = nostr.schedules(byOwner.keys)
        decrypted.keys.retainAll(reads.map { it.event.id }.toSet())
        val at = now()
        nextWake.values.removeIf { it <= at }
        val gate = Semaphore(parallelism)
        val outcomes =
            coroutineScope {
                reads
                    .mapNotNull { read -> byOwner[read.event.pubKey]?.let { read to it } }
                    .map { (read, pair) ->
                        async {
                            val key = ScheduleKey(pair.owner, read.event.dTag())
                            key to
                                gate.withPermit {
                                    try {
                                        decide(read, pair)
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Exception) {
                                        // The class only: a message could carry what a relay, the database or a service echoed.
                                        report(pair.owner, RunnerOutcome.ERROR, e.javaClass.simpleName)
                                    }
                                }
                        }
                    }.awaitAll()
            }
        return outcomes.toMap(LinkedHashMap())
    }

    private suspend fun decide(
        read: ScheduleRead,
        pair: AssistantPair,
    ): RunnerOutcome {
        val event = read.event
        val owner = pair.owner
        val named = ScheduledTask(event.tags).runners
        // The owner's devices run it (their newest version names no runner): said quietly.
        if (named.isEmpty()) return report(owner, RunnerOutcome.NOT_NAMED)
        // NIP-FB: with more than one `p`, nobody runs the task. Nothing is decrypted.
        if (named.size != 1) return report(owner, RunnerOutcome.RUNNER_CONFLICT, "${named.size} p tags")
        // Being named is not enough: the author must be the owner of THAT Assistant, and it must verify.
        val check = ScheduledTaskClaims.checkRunner(event, pair.assistant, owner)
        if (check != ScheduledTaskClaims.RunnerCheck.OK) return report(owner, RunnerOutcome.REJECTED, check.name)
        // Too few of the owner's relays answered: a newer version (the task taken back) may be on the others.
        if (!read.quorum) return report(owner, RunnerOutcome.NO_QUORUM)

        val task =
            decrypted[event.id] ?: when (val opened = open(event, owner)) {
                is Opened.Task -> opened.task.also { decrypted[event.id] = it }
                is Opened.Failed -> return report(owner, opened.outcome)
            }
        val name = task.task ?: return report(owner, RunnerOutcome.UNKNOWN_TASK)
        val service = services.serviceFor(name) ?: return report(owner, RunnerOutcome.UNKNOWN_TASK)
        val zone = zoneOf(task.tz) ?: return report(owner, RunnerOutcome.UNKNOWN_ZONE)
        val at = now()

        // "Already started here" is the service's runs row, read before deciding, so Quartz's order holds.
        val due = if (task.paused) null else ScheduledTaskClaims.occurrence(task, zone, at)
        val own = due?.let { runs.ownRun(name, owner, it) } ?: OwnRun.NONE
        val decision =
            ScheduledTaskClaims.decideAsRunner(task, name, pair.assistant, zone, at, ownRun = { if (it == due) own else OwnRun.NONE })
        if (!decision.shouldRun) return report(owner, verdictOutcome(decision.verdict))
        val occurrence = decision.occurrence!!

        val wakeKey = WakeKey(name, owner, occurrence)
        nextWake[wakeKey]?.let { if (at < it) return report(owner, RunnerOutcome.WOKEN_RECENTLY) }
        if (!accepting()) return report(owner, RunnerOutcome.STOPPING)
        val answer = waker.wake(service, WakeRequest(name, owner, occurrence))
        val (outcome, again, detail) =
            when (answer) {
                WakeAnswer.Accepted -> Triple(RunnerOutcome.WOKEN, wakeRetrySeconds, null)
                is WakeAnswer.Settled -> Triple(RunnerOutcome.SETTLED, wakeRetrySeconds, answer.outcome)
                // The service names when: honoured as given, within a day, whatever WAKE_RETRY_SECONDS says.
                is WakeAnswer.Busy ->
                    Triple(
                        RunnerOutcome.BUSY,
                        (answer.retryAfterSeconds ?: wakeRetrySeconds).coerceIn(1, MAX_RETRY_AFTER_SECONDS),
                        null,
                    )
                WakeAnswer.Malformed -> Triple(RunnerOutcome.WAKE_REFUSED, wakeRetrySeconds, null)
                is WakeAnswer.Failed -> Triple(RunnerOutcome.WAKE_FAILED, wakeRetrySeconds, answer.reason)
            }
        nextWake[wakeKey] = at + again
        return report(owner, outcome, detail)
    }

    private sealed interface Opened {
        class Task(
            val task: ScheduledTask,
        ) : Opened

        class Failed(
            val outcome: RunnerOutcome,
        ) : Opened
    }

    /**
     * [event]'s private tags, decrypted with the owner's Assistant's key: NIP-44 with the owner as
     * peer (the private tags are encrypted to the runner `p`), through Quartz's
     * `ScheduledTaskEvent.task`. The key lives only for this call; the plaintext is kept by event id.
     */
    private suspend fun open(
        event: ScheduledTaskEvent,
        owner: HexKey,
    ): Opened {
        val signer =
            when (val key = directory.key(owner)) {
                is AssistantKey.Ready -> {
                    key.signer
                }

                is AssistantKey.Failed -> {
                    return Opened.Failed(
                        when (key.failure) {
                            KeyFailure.NO_ASSISTANT -> RunnerOutcome.NO_ASSISTANT
                            KeyFailure.NSEC_UNREADABLE -> RunnerOutcome.NSEC_UNREADABLE
                            KeyFailure.NSEC_MISMATCH -> RunnerOutcome.NSEC_MISMATCH
                        },
                    )
                }
            }
        val task =
            try {
                event.task(signer)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                null
            }
        return task?.let { Opened.Task(it) } ?: Opened.Failed(RunnerOutcome.UNREADABLE)
    }

    private fun verdictOutcome(verdict: ScheduledTaskClaims.RunnerVerdict): RunnerOutcome =
        when (verdict) {
            ScheduledTaskClaims.RunnerVerdict.NOT_THIS_RUNNER -> RunnerOutcome.REJECTED
            ScheduledTaskClaims.RunnerVerdict.RUNNER_CONFLICT -> RunnerOutcome.RUNNER_CONFLICT
            ScheduledTaskClaims.RunnerVerdict.NOT_RUNNABLE -> RunnerOutcome.NOT_RUNNABLE
            ScheduledTaskClaims.RunnerVerdict.NOT_DUE -> RunnerOutcome.NOT_DUE
            ScheduledTaskClaims.RunnerVerdict.ALREADY_DONE -> RunnerOutcome.ALREADY_DONE
            ScheduledTaskClaims.RunnerVerdict.ATTEMPTED_HERE -> RunnerOutcome.ATTEMPTED
            ScheduledTaskClaims.RunnerVerdict.CLAIMED_BY_DEVICE -> RunnerOutcome.CLAIMED_BY_DEVICE
            ScheduledTaskClaims.RunnerVerdict.RUN -> RunnerOutcome.ERROR
        }

    private fun report(
        owner: HexKey,
        outcome: RunnerOutcome,
        detail: String? = null,
    ): RunnerOutcome {
        // Owner and outcome; the detail is always ours (an enum name, a count, an HTTP status, an exception's class).
        if (outcome in QUIET) {
            Log.d(TAG) { "owner=$owner outcome=$outcome" }
        } else {
            Log.i(TAG) { "owner=$owner outcome=$outcome" + (detail?.let { " detail=$it" } ?: "") }
        }
        return outcome
    }

    companion object {
        private const val TAG = "Runner"

        const val PARALLELISM = 8

        const val DEFAULT_WAKE_RETRY_SECONDS = 600L

        /** The longest a service's `Retry-After` holds a wake back: past it, a broken header would silence an owner. */
        const val MAX_RETRY_AFTER_SECONDS = 86_400L

        /** What a steady poll says of most owners every few minutes: debug, so the log carries what changed. */
        private val QUIET =
            setOf(
                RunnerOutcome.NOT_NAMED,
                RunnerOutcome.NOT_DUE,
                RunnerOutcome.ATTEMPTED,
                RunnerOutcome.ALREADY_DONE,
                RunnerOutcome.WOKEN_RECENTLY,
                RunnerOutcome.UNKNOWN_TASK,
            )
    }
}
