package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTask
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTaskEvent
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** How long a relay can hold the runner, and which reads are trusted. Ported from the-nostr-observer's. */
class RunnerBoundsTest : RunnerWorld() {
    @Test
    fun aRelayThatNeverAnswersDoesNotHoldThePoll() =
        runTest {
            world()
            schedule()
            // The owner's relay takes every REQ and never ends one: no EOSE, no CLOSED, no error.
            val hanging =
                object : RelayAccess {
                    override suspend fun fetch(
                        relays: Set<NormalizedRelayUrl>,
                        filter: Filter,
                    ): RelayRead = if (ownerRelay in relays) awaitCancellation() else this@RunnerBoundsTest.relays.fetch(relays, filter)
                }
            val outcomes = runnerWith(access = hanging).poll()
            // It answered for nobody: no quorum on the owner's relays, nobody woken.
            assertEquals(RunnerOutcome.NO_QUORUM, outcomes[ScheduleKey(owner.pubKey, "observer")])
            assertTrue(waker.wakes.isEmpty())
        }

    @Test
    fun aRunWaitingForAPermitWhenTheRunnerStopsWakesNothing() =
        runTest {
            world()
            schedule()
            schedule(task(runner = strangersAssistant), by = stranger)
            var stopping = false
            // One schedule at a time; SIGTERM arrives during the first wake.
            waker.answer = {
                stopping = true
                WakeAnswer.Accepted
            }
            val outcomes = runnerWith(accepting = { !stopping }, parallelism = 1).poll()
            assertEquals(1, waker.wakes.size)
            assertEquals(listOf(RunnerOutcome.WOKEN, RunnerOutcome.STOPPING), outcomes.values.sorted())
        }

    @Test
    fun aRelayThatCutsAPageShortIsNotTakenAsHavingAnsweredForEveryone() =
        runTest {
            world()
            schedule()
            // Two more owners whose schedules are on the same relay, written later than the owner's.
            val others = listOf(newSigner(), newSigner())
            val assistants = others.map { newSigner() }
            others.zip(assistants).forEachIndexed { i, (who, theirs) ->
                relays.put(index, who.signed(10002, arrayOf(arrayOf("r", ownerRelay.url, "write")), createdAt = 1))
                val t = ScheduledTask.create("observer", "observer", "0 6 * * *", "Europe/London", setAt = 0).withRunner(theirs.pubKey, 0)
                relays.put(ownerRelay, typed(who.sign(ScheduledTaskEvent.update(t, who, "observer", createdAt = now - 60 + i))))
            }
            // The owner took the task back on their relay; the index still has the version naming the Assistant.
            schedule(task(runner = null), onIndex = false, createdAt = now - 1_800)
            // The owners' relay sends two events for a REQ with no limit, and says nothing about it.
            relays.defaultLimit = { if (it == ownerRelay) 2 else null }
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock })
            val reads = nostr.schedules(listOf(owner.pubKey) + others.map { it.pubKey })
            val mine = reads.single { it.event.pubKey == owner.pubKey }
            // The newest is the one without the Assistant, read in full.
            assertTrue(ScheduledTask(mine.event.tags).runners.isEmpty())
            assertTrue(mine.quorum)
        }

    @Test
    fun aRelayThatClosesABigBatchIsAskedAgainInSmallerOnes() =
        runTest {
            world()
            schedule()
            relays.refuses = { relay, filter -> relay == ownerRelay && filter.authors.orEmpty().size > 1 }
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock }, authorsPerReq = 4, minChunk = 1)
            val read = nostr.schedules(listOf(owner.pubKey, stranger.pubKey)).single { it.event.pubKey == owner.pubKey }
            assertTrue(read.quorum)
            assertTrue(
                relays.asked.any { (to, f) ->
                    to == setOf(ownerRelay) && f.authors == listOf(owner.pubKey) && f.limit == RunnerNostr.SCHEDULES_PER_OWNER
                },
            )
        }

    @Test
    fun anOwnersOtherScheduleCutFromAFullPageIsAskedForAgain() =
        runTest {
            world()
            // The owner has two schedules; the stranger, on the same relay, five newer ones.
            schedule(onIndex = false, createdAt = now - 3_600)
            schedule(task(taskName = "digest", d = "digest"), d = "digest", onIndex = false, createdAt = now - 7_200)
            repeat(5) { i ->
                schedule(
                    task(d = "s$i", runner = strangersAssistant),
                    by = stranger,
                    d = "s$i",
                    onIndex = false,
                    createdAt =
                        now - 60 + i,
                )
            }
            // Three a person, two people a REQ: a limit of six, which the stranger's five and the
            // owner's newest fill. The owner sent one, but the page was full: their other was cut.
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock }, authorsPerReq = 2, schedulesPerOwner = 3)
            val reads = nostr.schedules(listOf(owner.pubKey, stranger.pubKey))
            val mine = reads.filter { it.event.pubKey == owner.pubKey }
            assertEquals(setOf("observer", "digest"), mine.map { it.event.dTag() }.toSet())
            assertTrue(mine.all { it.quorum })
            // The stranger, alone on a page as full as their limit, is paged on and read in full.
            val theirs = reads.filter { it.event.pubKey == stranger.pubKey }
            assertEquals(5, theirs.size)
            assertTrue(theirs.all { it.quorum })
        }

    @Test
    fun manyOwnersWithSeveralSchedulesEachAreAllReadOnARelayThatCutsAt100() =
        runTest {
            // 500 owners with 4 schedules each, all on one relay that sends at most 100 events a REQ
            // and says nothing about it. Paged by `until`, never halved into ever smaller REQs.
            val owners = List(500) { newSigner() }
            val d = listOf("a", "b", "c", "d")
            for ((i, who) in owners.withIndex()) {
                relays.put(index, who.signed(10002, arrayOf(arrayOf("r", ownerRelay.url, "write")), createdAt = 1))
                for ((j, tag) in d.withIndex()) {
                    val t = ScheduledTask.create(tag, "observer", "0 6 * * *", "Europe/London", setAt = 0)
                    relays.put(ownerRelay, typed(who.sign(ScheduledTaskEvent.update(t, who, tag, createdAt = now - 10_000 + i * 4 + j))))
                }
            }
            relays.maxLimit = { 100 }
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock })
            val reads = nostr.schedules(owners.map { it.pubKey })
            assertEquals(2_000, reads.size)
            assertTrue(reads.all { it.quorum })
            val toOwnerRelay = relays.asked.count { (to, f) -> to == setOf(ownerRelay) && f.kinds == listOf(ScheduledTaskEvent.KIND) }
            assertTrue(toOwnerRelay <= 25, "$toOwnerRelay REQs")
        }

    @Test
    fun aChunkTheEventCapCutOffIsPagedNotDropped() =
        runTest {
            world()
            schedule(onIndex = false, createdAt = now - 3_600)
            repeat(6) { i ->
                schedule(task(d = "s$i", runner = strangersAssistant), by = stranger, d = "s$i", onIndex = false, createdAt = now - 60 + i)
            }
            // Every page past 3 events is cut off by the read's event cap: neither served nor refused.
            relays.cap = { if (it == ownerRelay) 3 else null }
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock })
            val reads = nostr.schedules(listOf(owner.pubKey, stranger.pubKey))
            assertTrue(reads.single { it.event.pubKey == owner.pubKey }.quorum)
            assertEquals(6, reads.count { it.event.pubKey == stranger.pubKey && it.quorum })
        }

    @Test
    fun aRelayThatSaysRateLimitedIsNotAskedAgain() =
        runTest {
            world()
            schedule()
            relays.refuses = { relay, _ -> relay == ownerRelay }
            relays.refusal = "rate-limited: slow down"
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock }, authorsPerReq = 4, minChunk = 1)
            nostr.schedules(listOf(owner.pubKey, stranger.pubKey))
            assertEquals(1, relays.asked.count { (to, _) -> to == setOf(ownerRelay) })
        }

    @Test
    fun aClosedChunkIsHalvedFromItsOwnSizeAndASingleAuthorNotAskedAgain() =
        runTest {
            world()
            schedule()
            // Three owners on a relay that closes every REQ: 3, then 2 and 1, then 1 and 1. Never the same single author twice.
            val third = newSigner()
            relays.put(index, third.signed(10002, arrayOf(arrayOf("r", ownerRelay.url, "write")), createdAt = 1))
            relays.refuses = { relay, _ -> relay == ownerRelay }
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock }, authorsPerReq = 4, minChunk = 1)
            nostr.schedules(listOf(owner.pubKey, stranger.pubKey, third.pubKey))
            val sizes = relays.asked.filter { (to, _) -> to == setOf(ownerRelay) }.map { it.second.authors!!.size }
            assertEquals(listOf(3, 2, 1, 1, 1), sizes)
        }

    @Test
    fun aFullPageIsJudgedOnWhatTheRelaySentNotOnWhatVerified() =
        runTest {
            world()
            schedule()
            // A forged copy beside the real one: the relay sent 2 events for a limit of 2 (a full page),
            // though only one verifies. The owner is asked again rather than taken as answered for.
            val real = relays.held[ownerRelay]!!.first { it.kind == ScheduledTaskEvent.KIND }
            val forged = Event("f".repeat(64), owner.pubKey, real.createdAt - 1, real.kind, real.tags, real.content, "0".repeat(128))
            relays.put(ownerRelay, forged)
            val nostr = RunnerNostr(relays, setOf(index), null, now = { clock }, schedulesPerOwner = 2)
            nostr.schedules(listOf(owner.pubKey))
            assertTrue(relays.asked.count { (to, f) -> to == setOf(ownerRelay) && f.kinds == listOf(ScheduledTaskEvent.KIND) } > 1)
        }

    @Test
    fun aForgedCopyUnderTheSameIdDoesNotHideTheRealOne() =
        runTest {
            world()
            val real = schedule(onIndex = false)
            val forged = Event(real.id, real.pubKey, real.createdAt, real.kind, real.tags, real.content, "0".repeat(128))
            // A relay that sends the forgery first, then the real event, in one read.
            val forging =
                object : RelayAccess {
                    override suspend fun fetch(
                        relays: Set<NormalizedRelayUrl>,
                        filter: Filter,
                    ): RelayRead {
                        val read = this@RunnerBoundsTest.relays.fetch(relays, filter)
                        if (ownerRelay !in relays || filter.kinds != listOf(ScheduledTaskEvent.KIND)) return read
                        return RelayRead(listOf(forged) + read.events, read.served, read.refused, sent = read.sent + 1)
                    }
                }
            val read = RunnerNostr(forging, setOf(index), null, now = { clock }).schedules(listOf(owner.pubKey)).single()
            assertEquals(real.id, read.event.id)
            assertEquals(real.sig, read.event.sig)
        }
}
