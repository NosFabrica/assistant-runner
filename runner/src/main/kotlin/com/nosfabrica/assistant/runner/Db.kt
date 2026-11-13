package com.nosfabrica.assistant.runner

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.postgresql.Driver
import java.sql.Connection
import java.util.Properties

/**
 * brainstorm_server's Postgres, through plain JDBC, read-only: the runner reads four columns of
 * `brainstorm_nsec` ([AssistantKeys]) and holds an advisory lock ([LeaderLock], on a connection of
 * its own), and writes nothing. One connection per operation and no pool: a poll makes a handful
 * of calls, and a pool's idle connections would be one more thing to size, health-check and leak.
 *
 * Every call runs on [io]: JDBC blocks. The password is never part of [toString].
 */
class Db(
    val url: String,
    private val user: String?,
    private val password: String?,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    init {
        require(url.startsWith("jdbc:postgresql:")) { "DATABASE_URL must be a jdbc:postgresql: URL" }
    }

    /** A new connection, autocommit on. The caller closes it. */
    fun connect(): Connection {
        val props = Properties()
        user?.let { props.setProperty("user", it) }
        password?.let { props.setProperty("password", it) }
        // A dead peer is noticed by the OS, not only by the next query: the leader lock's
        // connection sits idle between polls.
        props.setProperty("tcpKeepAlive", "true")
        props.setProperty("connectTimeout", "10")
        props.setProperty("ApplicationName", "assistant-runner")
        // Through the driver itself, not DriverManager: a jar the service loader didn't register
        // (a shaded build, an isolated class loader) still connects.
        return DRIVER.connect(url, props) ?: throw IllegalArgumentException("DATABASE_URL is not a Postgres URL")
    }

    /** [block] on a fresh connection, on the IO dispatcher. */
    suspend fun <T> use(block: (Connection) -> T): T = withContext(io) { connect().use(block) }

    // Without the query: a JDBC URL may carry `?password=`.
    override fun toString() = "Db(${url.substringBefore('?')})"

    companion object {
        private val DRIVER = Driver()

        /** `DATABASE_URL` (a JDBC URL), `DATABASE_USER`, `DATABASE_PASSWORD`. */
        fun fromEnv(env: Map<String, String> = System.getenv()): Db {
            val url = env["DATABASE_URL"]?.trim()?.takeIf { it.isNotEmpty() } ?: throw IllegalArgumentException("DATABASE_URL is not set")
            return Db(
                url,
                env["DATABASE_USER"]?.takeIf { it.isNotEmpty() },
                env["DATABASE_PASSWORD"]?.takeIf { it.isNotEmpty() },
            )
        }
    }
}
