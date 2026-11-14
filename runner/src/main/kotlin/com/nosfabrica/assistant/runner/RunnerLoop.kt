package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.utils.Log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicBoolean

/** Why [RunnerLoop.run] returned. */
enum class LoopEnd {
    /** Asked to stop (SIGTERM). */
    STOPPED,

    /** The leader lock's connection died: another runner may hold it now, so this one must exit and be restarted. */
    LEADERSHIP_LOST,
}

/**
 * The runner's life: wait for the leader lock, then start a poll every [pollMs] while the lock
 * holds. A second runner (a rolling deploy's new pod) waits here and never polls until the first
 * lets go. Checked every [tickMs] for a stop, and every [aliveCheckMs] for the lock between
 * polls; during a poll a watchdog asks every [pollAliveCheckMs], and cuts the poll short when the
 * lock is gone, since another runner may hold it by then and two would wake the same owners.
 *
 * The period runs from one poll's start to the next, on a monotonic clock ([nowMs]): a poll that
 * took longer than [pollMs] is followed at once, never by a whole period more.
 */
class RunnerLoop(
    private val tryAcquire: suspend () -> Boolean,
    private val isAlive: suspend () -> Boolean,
    private val poll: suspend () -> Unit,
    private val stopping: () -> Boolean,
    private val pollMs: Long,
    private val lockRetryMs: Long = 10_000,
    private val tickMs: Long = 1_000,
    private val aliveCheckMs: Long = 30_000,
    private val pollAliveCheckMs: Long = POLL_ALIVE_CHECK_MS,
    private val nowMs: () -> Long = { System.nanoTime() / 1_000_000 },
) {
    suspend fun run(): LoopEnd {
        if (!awaitLock()) return LoopEnd.STOPPED
        Log.i(TAG) { "leader lock taken" }
        while (true) {
            if (stopping()) return LoopEnd.STOPPED
            if (!isAlive()) return lost()
            val started = nowMs()
            if (!pollWatched()) return lost()
            var lastCheck = nowMs()
            while (true) {
                val at = nowMs()
                val left = pollMs - (at - started)
                if (left <= 0) break
                if (stopping()) return LoopEnd.STOPPED
                if (at - lastCheck >= aliveCheckMs) {
                    if (!isAlive()) return lost()
                    lastCheck = at
                }
                delay(minOf(tickMs, left))
            }
        }
    }

    /** One [poll], with a watchdog on the lock beside it. False when the lock went: the poll was cancelled. */
    private suspend fun pollWatched(): Boolean {
        val lost = AtomicBoolean(false)
        coroutineScope {
            val polling =
                launch {
                    try {
                        poll()
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        // The class only: a message could carry what a relay or the database echoed.
                        Log.w(TAG) { "poll failed: ${e.javaClass.simpleName}" }
                    }
                }
            val watchdog =
                launch {
                    while (true) {
                        delay(pollAliveCheckMs)
                        val alive =
                            try {
                                isAlive()
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                false
                            }
                        if (!alive) {
                            lost.set(true)
                            polling.cancel()
                            return@launch
                        }
                    }
                }
            polling.join()
            watchdog.cancel()
        }
        return !lost.get()
    }

    private fun lost(): LoopEnd {
        Log.e(TAG) { "the leader lock's connection is gone: exiting so the orchestrator restarts this runner" }
        return LoopEnd.LEADERSHIP_LOST
    }

    /** True once the lock is held; false when asked to stop first. */
    private suspend fun awaitLock(): Boolean {
        var said = false
        while (!stopping()) {
            val got =
                try {
                    tryAcquire()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    Log.w(TAG) { "could not ask for the leader lock: ${e.javaClass.simpleName}" }
                    false
                }
            if (got) return true
            if (!said) {
                Log.i(TAG) { "another runner holds the leader lock: waiting" }
                said = true
            }
            var waited = 0L
            while (waited < lockRetryMs && !stopping()) {
                delay(tickMs)
                waited += tickMs
            }
        }
        return false
    }

    companion object {
        private const val TAG = "RunnerLoop"

        /** How often the lock is asked about while a poll runs. */
        const val POLL_ALIVE_CHECK_MS = 5_000L
    }
}

/**
 * Asked before every wake: whether this runner still holds the leader lock, from [isAlive]'s
 * last answer while it is at most [maxAgeMs] old, else asked again. The loop's watchdog cancels a
 * poll whose lock is gone; this closes the seconds between its checks. Once lost, lost for good.
 */
class LeadershipGate(
    private val isAlive: suspend () -> Boolean,
    private val maxAgeMs: Long = MAX_AGE_MS,
    private val nowMs: () -> Long = { System.nanoTime() / 1_000_000 },
) {
    private val mutex = Mutex()
    private var checkedAt: Long? = null
    private var alive = true

    suspend fun holds(): Boolean =
        mutex.withLock {
            if (!alive) return@withLock false
            val at = nowMs()
            val last = checkedAt
            if (last == null || at - last > maxAgeMs) {
                alive =
                    try {
                        isAlive()
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        false
                    }
                checkedAt = at
            }
            alive
        }

    companion object {
        const val MAX_AGE_MS = 5_000L
    }
}
