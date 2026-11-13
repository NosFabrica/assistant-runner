package com.nosfabrica.assistant.core

import kotlin.test.Test
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
