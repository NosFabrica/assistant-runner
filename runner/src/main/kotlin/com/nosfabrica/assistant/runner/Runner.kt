package com.nosfabrica.assistant.runner

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

    /** The service answered `200`: settled, nothing to do. The occurrence is not woken again. */
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

    /** The Assistant's key: no row (or no Assistant) in `brainstorm_nsec`, a token no Fernet key opens, or an nsec that isn't the stored Assistant. */
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

    /** The service already answered `200` (settled) for this occurrence: it started, ran or declined it. */
    ATTEMPTED,

    /** A device of the owner holds a live claim on the occurrence. */
    CLAIMED_BY_DEVICE,

    /** A device of the owner finished it (`ran`). */
    ALREADY_DONE,

    /** The task's service failed [Runner.MAX_FAILED_WAKES] wakes in a row this poll: not woken again until the next. */
    SERVICE_UNAVAILABLE,

    /** The runner is stopping, or no longer holds the leader lock: no new wake. */
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
 * for an occurrence that is due. It signs nothing, spends nothing and writes nothing anywhere:
 * "already started here" is what the service answered, a `200` for the occurrence, kept in memory.
 */
class Runner(
    private val directory: AssistantDirectory,
    private val nostr: RunnerNostr,
    private val waker: TaskWaker,
    private val services: TaskServices,
    private val zoneOf: (String?) -> CronZone? = { it?.let(ZoneIdCronZone::of) },
    private val now: () -> Long = { TimeUtils.now() },
    /** At most one wake per (task, owner, occurrence) this often, until the service answers it settled. */
    private val wakeRetrySeconds: Long = DEFAULT_WAKE_RETRY_SECONDS,
    /** How many schedules are decided (and woken) at once. */
    private val parallelism: Int = PARALLELISM,
    /** False once the runner is stopping, or has lost the leader lock: no new wake. */
    private val accepting: suspend () -> Boolean = { true },
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

    /**
     * The occurrences the service answered `200` for, with when: settled, so each is never woken
     * again. Quartz's runner rules read one as this runner's own ended run ([OwnRun.ENDED]). Kept
     * per occurrence, not one per (task, owner): two schedules of one task (06:00 and 06:15) each
     * settle their own, and neither unsettles the other. In memory: an occurrence older than
     * [SETTLED_KEEP_SECONDS] is dropped (no schedule is due at it any more), and so is an owner's
     * whose Assistant is gone. A restart forgets them, which costs at most one wake per due
     * occurrence, and the service answers that wake as settled again.
     */
    private val settled = ConcurrentHashMap<WakeKey, Long>()

    /** The occurrences a wake is under way for: a second schedule due at the same one waits its turn, then finds it woken. */
    private val waking = ConcurrentHashMap.newKeySet<WakeKey>()

    /**
     * The owners whose own relays are read on every poll, index or not: those that had a due
     * schedule last poll (woken, failed, busy, without a quorum) or have a wake pending. An owner
     * the index doesn't show would otherwise be read hourly, and their retry wait an hour.
     */
    @Volatile
    private var pending: Set<HexKey> = emptySet()

    /** Each task service's failed wakes in a row, this poll. */
    private val failedInARow = ConcurrentHashMap<String, Int>()

    /** The versions (event id) that didn't decrypt with an Assistant's key (its pubkey): one never will. */
    private val unreadable = ConcurrentHashMap.newKeySet<Pair<String, HexKey>>()

    /** Each schedule's merged tags by event id: an event never changes, so neither does its plaintext. Never logged. */
    private val decrypted = ConcurrentHashMap<String, ScheduledTask>()

    /** One pass over every schedule. Returns each schedule's outcome, for tests and the log. */
    suspend fun poll(): Map<ScheduleKey, RunnerOutcome> {
        val pairs = directory.pairs()
        val byOwner = pairs.associateBy { it.owner }
        // An owner whose Assistant is gone takes its settled occurrences with it; so does time.
        val oldest = now() - SETTLED_KEEP_SECONDS
        settled.keys.retainAll { it.owner in byOwner && it.occurrence >= oldest }
        failedInARow.clear()
        val reads = nostr.schedules(byOwner.keys, alwaysOwnRelays = pending + nextWake.keys.map { it.owner })
        val ids = reads.mapTo(HashSet()) { it.event.id }
        decrypted.keys.retainAll(ids)
        unreadable.retainAll { it.first in ids }
        val at = now()
        nextWake.values.removeIf { it <= at }
        val paired = reads.mapNotNull { read -> byOwner[read.event.pubKey]?.let { read to it } }
        // The keys this poll decrypts with, read in one batch, and zeroed once the poll is done.
        val keys = keysFor(paired.filter { (read, pair) -> needsKey(read, pair) }.map { it.second.owner })
        val gate = Semaphore(parallelism)
        val outcomes =
            try {
                coroutineScope {
                    paired
                        .map { (read, pair) ->
                            async {
                                val key = ScheduleKey(pair.owner, read.event.dTag())
                                key to
                                    gate.withPermit {
                                        try {
                                            decide(read, pair, keys)
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
            } finally {
                keys?.values?.forEach { wipe(it) }
            }
        pending = outcomes.filter { it.second in PENDING }.mapTo(HashSet()) { it.first.owner }
        return outcomes.toMap(LinkedHashMap())
    }

    /** Whether [read] will need its owner's key: named this owner's Assistant alone, read with a quorum, not decrypted yet. */
    private fun needsKey(
        read: ScheduleRead,
        pair: AssistantPair,
    ): Boolean {
        val event = read.event
        return read.quorum &&
            event.pubKey == pair.owner &&
            !decrypted.containsKey(event.id) &&
            (event.id to pair.assistant) !in unreadable &&
            ScheduledTask(event.tags).runners == listOf(pair.assistant)
    }

    /** [owners]' keys in one batch; null when it failed (each then fails on its own, by the exception's class). */
    private suspend fun keysFor(owners: List<HexKey>): Map<HexKey, AssistantKey>? {
        if (owners.isEmpty()) return emptyMap()
        return try {
            directory.keys(owners)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Log.w(TAG) { "could not read ${owners.size} Assistant keys: ${e.javaClass.simpleName}" }
            null
        }
    }

    /** Zeroes a key's private bytes: the signer is never used after. */
    private fun wipe(key: AssistantKey) {
        if (key is AssistantKey.Ready) {
            key.signer.keyPair.privKey
                ?.fill(0)
        }
    }

    private suspend fun decide(
        read: ScheduleRead,
        pair: AssistantPair,
        keys: Map<HexKey, AssistantKey>?,
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

        // A version that didn't decrypt with this Assistant's key never will: not tried again.
        if ((event.id to pair.assistant) in unreadable) return report(owner, RunnerOutcome.UNREADABLE)
        val task =
            decrypted[event.id] ?: when (val opened = open(event, owner, keys)) {
                is Opened.Task -> {
                    opened.task.also { decrypted[event.id] = it }
                }

                is Opened.Failed -> {
                    if (opened.outcome == RunnerOutcome.UNREADABLE) unreadable += event.id to pair.assistant
                    return report(owner, opened.outcome)
                }
            }
        val name = task.task ?: return report(owner, RunnerOutcome.UNKNOWN_TASK)
        val service = services.serviceFor(name) ?: return report(owner, RunnerOutcome.UNKNOWN_TASK)
        val zone = zoneOf(task.tz) ?: return report(owner, RunnerOutcome.UNKNOWN_ZONE)
        val at = now()

        // "Already started here" is the service's settled answer for the occurrence, so Quartz's order holds.
        val decision =
            ScheduledTaskClaims.decideAsRunner(task, name, pair.assistant, zone, at, ownRun = {
                if (settled.containsKey(WakeKey(name, owner, it))) OwnRun.ENDED else OwnRun.NONE
            })
        if (!decision.shouldRun) return report(owner, verdictOutcome(decision.verdict))
        val occurrence = decision.occurrence!!

        val wakeKey = WakeKey(name, owner, occurrence)
        // One wake at a time per occurrence: another schedule of the same task due at it finds it woken, or settled.
        if (!waking.add(wakeKey)) return report(owner, RunnerOutcome.WOKEN_RECENTLY)
        try {
            return wake(wakeKey, service, at)
        } finally {
            waking.remove(wakeKey)
        }
    }

    /** Wakes [key]'s service, unless it was woken too recently or settled meanwhile. Called holding [waking] for [key]. */
    private suspend fun wake(
        key: WakeKey,
        service: String,
        at: Long,
    ): RunnerOutcome {
        val owner = key.owner
        if (settled.containsKey(key)) return report(owner, RunnerOutcome.ATTEMPTED)
        nextWake[key]?.let { if (at < it) return report(owner, RunnerOutcome.WOKEN_RECENTLY) }
        if (!accepting()) return report(owner, RunnerOutcome.STOPPING)
        // A service that failed this many in a row is down: the rest of its wakes wait for the next poll.
        if ((failedInARow[service] ?: 0) >= MAX_FAILED_WAKES) return report(owner, RunnerOutcome.SERVICE_UNAVAILABLE)
        val answer = waker.wake(service, WakeRequest(key.task, owner, key.occurrence))
        if (answer is WakeAnswer.Failed || (answer is WakeAnswer.Settled && answer.outcome == null)) {
            failedInARow.merge(service, 1, Int::plus)
        } else {
            failedInARow.remove(service)
        }
        // When it may be woken again: null for never, the service having settled it.
        val (outcome, again, detail) =
            when (answer) {
                WakeAnswer.Accepted -> Triple(RunnerOutcome.WOKEN, wakeRetrySeconds, null)
                // A 200 whose body names no outcome isn't an answer: it settles nothing.
                is WakeAnswer.Settled ->
                    if (answer.outcome == null) {
                        Triple(RunnerOutcome.WAKE_FAILED, wakeRetrySeconds, "no outcome")
                    } else {
                        Triple(RunnerOutcome.SETTLED, null, answer.outcome)
                    }
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
        if (again == null) {
            settled[key] = at
            nextWake.remove(key)
        } else {
            nextWake[key] = at + again
        }
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
        keys: Map<HexKey, AssistantKey>?,
    ): Opened {
        // The poll's batch has it; one it doesn't (it failed, or the checks disagreed) is read alone, and zeroed here.
        val batched = keys?.get(owner)
        val key = batched ?: directory.key(owner)
        try {
            return openWith(event, key)
        } finally {
            if (batched == null) wipe(key)
        }
    }

    private suspend fun openWith(
        event: ScheduledTaskEvent,
        key: AssistantKey,
    ): Opened {
        val signer =
            when (key) {
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

        /** A task service's failed wakes in a row before the rest of its wakes wait for the next poll. */
        const val MAX_FAILED_WAKES = 3

        /** Outcomes that mean a schedule was due, or may be: its owner's relays are read on the next poll, index or not. */
        private val PENDING =
            setOf(
                RunnerOutcome.WOKEN,
                RunnerOutcome.BUSY,
                RunnerOutcome.WAKE_REFUSED,
                RunnerOutcome.WAKE_FAILED,
                RunnerOutcome.WOKEN_RECENTLY,
                RunnerOutcome.NO_QUORUM,
                RunnerOutcome.SERVICE_UNAVAILABLE,
                RunnerOutcome.STOPPING,
            )

        /** How long a settled occurrence is remembered: past two days no schedule is still due at it. */
        const val SETTLED_KEEP_SECONDS = 2 * 86_400L

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
