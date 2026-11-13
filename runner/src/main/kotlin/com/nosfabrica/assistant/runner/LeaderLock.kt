package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.utils.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.sql.SQLException

/** The advisory-lock keys the runner takes. Advisory locks are per database, so they're spelled out once, here. */
object AdvisoryLocks {
    /** Held by the one active runner for its whole life ([LeaderLock]). ASCII "asr-lead". */
    const val RUNNER_LEADER = 0x6173722d6c656164L
}

/**
 * Exactly one active runner: a session-level `pg_try_advisory_lock` on a connection of its own,
 * held for the life of the process. A second process (a rolling deploy's new pod) doesn't get it
 * and waits. The lock is the session's, so it goes when the connection does: [isAlive] says
 * whether it still holds, and a process that finds it doesn't must stop (and be restarted), since
 * another may have taken it meanwhile.
 */
class LeaderLock(
    private val db: Db,
    private val key: Long = AdvisoryLocks.RUNNER_LEADER,
) : AutoCloseable {
    private var connection: Connection? = null

    /** Takes the lock if it is free. True when this process holds it now (or already did). */
    @Synchronized
    fun tryAcquire(): Boolean {
        if (connection != null) return true
        val c = db.connect()
        val got =
            try {
                c.prepareStatement("SELECT pg_try_advisory_lock(?)").use { st ->
                    st.setLong(1, key)
                    st.executeQuery().use { it.next() && it.getBoolean(1) }
                }
            } catch (e: SQLException) {
                c.close()
                throw e
            }
        if (got) connection = c else c.close()
        return got
    }

    /** [tryAcquire] on the IO dispatcher. */
    suspend fun tryAcquireAsync(): Boolean = withContext(Dispatchers.IO) { tryAcquire() }

    /**
     * Whether the lock is still held: its connection answers within [timeoutSeconds], and its
     * backend still holds the lock (an operator's `pg_advisory_unlock_all`, a pooler that reset
     * the session). False once the connection is gone, and then for good.
     */
    @Synchronized
    fun isAlive(timeoutSeconds: Int = 5): Boolean {
        val c = connection ?: return false
        return try {
            c.isValid(timeoutSeconds) && holds(c)
        } catch (e: SQLException) {
            Log.w(TAG) { "the lock's connection failed: ${e.javaClass.simpleName}" }
            false
        }.also { if (!it) drop() }
    }

    private fun holds(c: Connection): Boolean =
        c
            .prepareStatement(
                "SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted " +
                    "AND classid::bigint = ? AND objid::bigint = ? AND objsubid = 1",
            ).use { st ->
                // A bigint key is reported as its high and low halves.
                st.setLong(1, key ushr 32)
                st.setLong(2, key and 0xffffffffL)
                st.executeQuery().use { it.next() }
            }

    private fun drop() {
        try {
            connection?.close()
        } catch (e: SQLException) {
            Log.d(TAG) { "close: ${e.javaClass.simpleName}" }
        }
        connection = null
    }

    /** Releases the lock, by closing its session. */
    @Synchronized
    override fun close() = drop()

    companion object {
        private const val TAG = "LeaderLock"
    }
}
