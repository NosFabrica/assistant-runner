package com.nosfabrica.assistant.runner

import com.nosfabrica.assistant.core.Db
import com.nosfabrica.assistant.core.LeaderLock
import com.nosfabrica.assistant.core.Migrations
import com.nosfabrica.assistant.core.TestPostgres
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Two runners on one database, as a rolling deploy runs them: only the lock holder polls. */
class RunnerLoopTest {
    private class Instance(
        db: Db,
    ) {
        val lock = LeaderLock(db)
        val polls = AtomicInteger()
        val stop = AtomicBoolean(false)
        val loop =
            RunnerLoop(
                tryAcquire = { withContext(Dispatchers.IO) { lock.tryAcquire() } },
                isAlive = { withContext(Dispatchers.IO) { lock.isAlive() } },
                migrate = { Migrations.migrate(db) },
                poll = { polls.incrementAndGet() },
                stopping = { stop.get() },
                pollMs = 50,
                lockRetryMs = 50,
                tickMs = 10,
                aliveCheckMs = 20,
            )
    }

    @Test
    fun aSecondRunnerDoesNotPollUntilTheFirstLetsGo() =
        runBlocking {
            val db = TestPostgres.database()
            val first = Instance(db)
            val second = Instance(db)
            val a = async(Dispatchers.IO) { first.loop.run() }
            while (first.polls.get() == 0) delay(10)
            val b = async(Dispatchers.IO) { second.loop.run() }
            delay(500)
            assertTrue(first.polls.get() > 2)
            assertEquals(0, second.polls.get())
            // The first stops (SIGTERM) and lets go of the lock: the second takes over.
            first.stop.set(true)
            assertEquals(LoopEnd.STOPPED, a.await())
            first.lock.close()
            while (second.polls.get() == 0) delay(10)
            second.stop.set(true)
            assertEquals(LoopEnd.STOPPED, b.await())
            second.lock.close()
        }

    @Test
    fun aRunnerWhoseLockConnectionDiesStops() =
        runBlocking {
            val db = TestPostgres.database()
            val one = Instance(db)
            val run = async(Dispatchers.IO) { one.loop.run() }
            while (one.polls.get() == 0) delay(10)
            db.connect().use { c ->
                c.createStatement().use {
                    it.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()",
                    )
                }
            }
            assertEquals(LoopEnd.LEADERSHIP_LOST, run.await())
        }
}
