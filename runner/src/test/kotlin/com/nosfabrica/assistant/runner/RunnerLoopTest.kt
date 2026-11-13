package com.nosfabrica.assistant.runner

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
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
    fun aLockLostMidPollCutsThePollShort() =
        runBlocking {
            val alive = AtomicBoolean(true)
            val loop =
                RunnerLoop(
                    tryAcquire = { true },
                    isAlive = { alive.get() },
                    // A poll stuck on a slow relay: it would end long after another runner took over.
                    poll = { awaitCancellation() },
                    stopping = { false },
                    pollMs = 60_000,
                    tickMs = 10,
                    pollAliveCheckMs = 50,
                )
            val run = async(Dispatchers.IO) { loop.run() }
            delay(200)
            alive.set(false)
            assertEquals(LoopEnd.LEADERSHIP_LOST, withTimeout(5_000) { run.await() })
        }

    @Test
    fun aPollStartsEveryPeriodHoweverLongTheLastTook() =
        runTest {
            val starts = ArrayList<Long>()
            val loop =
                RunnerLoop(
                    tryAcquire = { true },
                    isAlive = { true },
                    poll = {
                        starts += testScheduler.currentTime
                        delay(200)
                    },
                    stopping = { testScheduler.currentTime >= 1_000 },
                    pollMs = 300,
                    tickMs = 10,
                    nowMs = { testScheduler.currentTime },
                )
            loop.run()
            // Every 300 ms from start to start, not 300 ms after each poll ended.
            assertEquals(listOf(0L, 300L, 600L, 900L), starts)
        }

    @Test
    fun theGateBeforeAWakeAsksTheLockWhenItsLastAnswerIsOld() =
        runTest {
            var asked = 0
            var alive = true
            val gate =
                LeadershipGate({
                    asked++
                    alive
                }, maxAgeMs = 5_000, nowMs = { testScheduler.currentTime })
            assertTrue(gate.holds())
            assertTrue(gate.holds())
            assertEquals(1, asked)
            alive = false
            testScheduler.advanceTimeBy(5_001)
            assertFalse(gate.holds())
            // Lost is lost: never asked again, never true again.
            alive = true
            testScheduler.advanceTimeBy(5_001)
            assertFalse(gate.holds())
            assertEquals(2, asked)
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
