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
    /** Asks whether the server is a standby. A test's only way to stand one in. */
    private val recoveryCheck: String = "SELECT pg_is_in_recovery()",
) : AutoCloseable {
    private var connection: Connection? = null

    /**
     * Takes the lock if it is free. True when this process holds it now (or already did). Never
     * on a standby: its advisory locks are its own, so every runner reaching one would lead.
     */
    @Synchronized
    fun tryAcquire(): Boolean {
        if (connection != null) return true
        val c = db.connect(SESSION_SETTINGS)
        val got =
            try {
                if (inRecovery(c)) {
                    Log.w(TAG) { "the database is a standby: DATABASE_URL must reach the primary, directly; not leading" }
                    false
                } else {
                    c.prepareStatement("SELECT pg_try_advisory_lock(?)").use { st ->
                        st.setLong(1, key)
                        st.executeQuery().use { it.next() && it.getBoolean(1) }
                    }
                }
            } catch (e: SQLException) {
                c.close()
                throw e
            }
        if (got) connection = c else c.close()
        return got
    }

    private fun inRecovery(c: Connection): Boolean =
        c.createStatement().use { st -> st.executeQuery(recoveryCheck).use { it.next() && it.getBoolean(1) } }

    /** [tryAcquire] on the IO dispatcher. */
    suspend fun tryAcquireAsync(): Boolean = withContext(Dispatchers.IO) { tryAcquire() }

    /**
     * Whether the lock is still held: its backend answers (within the connection's socket
     * timeout) that it still holds the lock, which an operator's `pg_advisory_unlock_all` or a
     * pooler that reset the session would have undone. That one query proves the connection
     * alive too. False once the connection is gone, and then for good.
     */
    @Synchronized
    fun isAlive(): Boolean {
        val c = connection ?: return false
        return try {
            holds(c)
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

        /**
         * The lock's session, set by itself (each has context `user`: no grant needed). After a
         * partition the server would otherwise hold the lock, and keep every other runner out,
         * until the kernel's keepalive gave up, about two hours on: with these it notices a dead
         * client within about a minute. `idle_session_timeout` is off for it whatever the
         * database's default: the lock's connection is idle between checks by design.
         */
        val SESSION_SETTINGS =
            mapOf(
                "tcp_keepalives_idle" to "30",
                "tcp_keepalives_interval" to "10",
                "tcp_keepalives_count" to "3",
                "tcp_user_timeout" to "60000",
                "idle_session_timeout" to "0",
            )
    }
}
