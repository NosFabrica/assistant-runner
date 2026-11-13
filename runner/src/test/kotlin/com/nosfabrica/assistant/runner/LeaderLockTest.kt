package com.nosfabrica.assistant.runner

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LeaderLockTest {
    @Test
    fun aSecondProcessDoesNotGetTheLockUntilTheFirstLetsGo() {
        val db = TestPostgres.database()
        LeaderLock(db).use { first ->
            LeaderLock(db).use { second ->
                assertTrue(first.tryAcquire())
                assertTrue(first.isAlive())
                assertFalse(second.tryAcquire())
                assertFalse(second.isAlive())
                first.close()
                assertFalse(first.isAlive())
                assertTrue(second.tryAcquire())
                assertTrue(second.isAlive())
            }
        }
    }

    @Test
    fun aDeadConnectionIsNoticedAndFreesTheLock() {
        val db = TestPostgres.database()
        LeaderLock(db).use { lock ->
            assertTrue(lock.tryAcquire())
            // The server drops the session, as a failover or a network cut would.
            db.connect().use { c ->
                c.createStatement().use {
                    it.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' " +
                            "AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) " +
                            "AND objid::bigint = ${AdvisoryLocks.RUNNER_LEADER and 0xffffffffL}",
                    )
                }
            }
            assertFalse(lock.isAlive())
            LeaderLock(db).use { next -> assertTrue(next.tryAcquire()) }
        }
    }

    @Test
    fun theLocksSessionKeepsItselfAliveAndTheRunnersRoleMaySaySo() {
        // Set by the session itself (context `user`), so the runner's role, with its one column
        // grant, may: the server notices a dead client within about a minute and frees the lock.
        val db = TestPostgres.database().also { BrainstormSchema.create(it) }
        val role = TestPostgres.runnerRole(db)
        role.connect(LeaderLock.SESSION_SETTINGS).use { c ->
            for ((name, value) in listOf(
                "tcp_keepalives_idle" to "30",
                "tcp_keepalives_interval" to "10",
                "tcp_keepalives_count" to "3",
                "tcp_user_timeout" to "60000",
                "idle_session_timeout" to "0",
            )) {
                c.createStatement().use { st ->
                    st.executeQuery("SHOW $name").use {
                        it.next()
                        assertEquals(value, it.getString(1), name)
                    }
                }
            }
        }
        LeaderLock(role).use { assertTrue(it.tryAcquire()) }
    }

    @Test
    fun anIdleLockOutlivesTheSocketTimeout() {
        // The socket timeout bounds a call waiting for its answer, never a connection sitting idle.
        val db = TestPostgres.database().with(socketTimeoutSeconds = 1)
        LeaderLock(db).use { lock ->
            assertTrue(lock.tryAcquire())
            Thread.sleep(2_500)
            assertTrue(lock.isAlive())
        }
    }

    @Test
    fun aReplicaIsNeverLed() {
        // A standby (or a pooler in front of one) would hand every runner its own lock.
        LeaderLock(TestPostgres.database(), recoveryCheck = "SELECT true").use { lock ->
            assertFalse(lock.tryAcquire())
            assertFalse(lock.isAlive())
        }
    }

    @Test
    fun anotherDatabaseOnTheSameServerIsAnotherLock() {
        // Advisory locks are per database: a staging and a test database don't block each other.
        LeaderLock(TestPostgres.database()).use { a ->
            LeaderLock(TestPostgres.database()).use { b ->
                assertTrue(a.tryAcquire())
                assertTrue(b.tryAcquire())
            }
        }
    }
}
