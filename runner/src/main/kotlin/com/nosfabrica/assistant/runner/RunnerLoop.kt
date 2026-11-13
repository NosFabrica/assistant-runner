package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.utils.Log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay

/** Why [RunnerLoop.run] returned. */
enum class LoopEnd {
    /** Asked to stop (SIGTERM). */
    STOPPED,

    /** The leader lock's connection died: another runner may hold it now, so this one must exit and be restarted. */
    LEADERSHIP_LOST,
}

/**
 * The runner's life: wait for the leader lock, migrate, then poll every [pollMs] while the lock
 * holds. A second runner (a rolling deploy's new pod) waits here and never polls until the first
 * lets go. Checked every [tickMs] for a stop, and every [aliveCheckMs] for the lock.
 */
class RunnerLoop(
    private val tryAcquire: suspend () -> Boolean,
    private val isAlive: suspend () -> Boolean,
    private val migrate: suspend () -> Unit,
    private val poll: suspend () -> Unit,
    private val stopping: () -> Boolean,
    private val pollMs: Long,
    private val lockRetryMs: Long = 10_000,
    private val tickMs: Long = 1_000,
    private val aliveCheckMs: Long = 30_000,
) {
    suspend fun run(): LoopEnd {
        if (!awaitLock()) return LoopEnd.STOPPED
        Log.i(TAG) { "leader lock taken" }
        migrate()
        while (true) {
            if (stopping()) return LoopEnd.STOPPED
            if (!isAlive()) return lost()
            try {
                poll()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // The class only: a message could carry what a relay or the database echoed.
                Log.w(TAG) { "poll failed: ${e.javaClass.simpleName}" }
            }
            var waited = 0L
            var sinceCheck = 0L
            while (waited < pollMs) {
                if (stopping()) return LoopEnd.STOPPED
                if (sinceCheck >= aliveCheckMs) {
                    if (!isAlive()) return lost()
                    sinceCheck = 0
                }
                delay(tickMs)
                waited += tickMs
                sinceCheck += tickMs
            }
        }
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
    }
}
