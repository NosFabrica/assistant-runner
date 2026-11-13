package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import com.vitorpamplona.quartz.nip51Lists.encryption.PrivateTagsInContent
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTask
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTaskEvent
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** PLAN.md's "The runner" list: what is woken, what isn't, and how often. */
class RunnerTest : RunnerWorld() {
    private val mine = ScheduleKey(owner.pubKey, "observer")

    @Test
    fun aDueScheduleWakesItsService() =
        runTest {
            world()
            schedule()
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            assertEquals(listOf(observerService to WakeRequest("observer", owner.pubKey, six)), waker.wakes)
        }

    @Test
    fun aScheduleNotDueYetWakesNothingUntilItIs() =
        runTest {
            world()
            // Named at 06:20: this morning's 06:00 came before it was set, so tomorrow's is the first.
            schedule(task().withRunner(null, 0).withRunner(assistant.pubKey, now - 600))
            assertEquals(RunnerOutcome.NOT_DUE, runner.poll()[mine])
            assertTrue(waker.wakes.isEmpty())
            clock = six + 86_400 + 300
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            assertEquals(
                six + 86_400,
                waker.wakes
                    .single()
                    .second.occurrence,
            )
        }

    @Test
    fun aPausedScheduleIsNotRunnable() =
        runTest {
            world()
            schedule(task().withEnabled(false, now))
            assertEquals(RunnerOutcome.NOT_RUNNABLE, runner.poll()[mine])
            assertTrue(waker.wakes.isEmpty())
        }

    @Test
    fun anUnknownTaskIsSkippedAndItsNameNeverLogged() =
        runTest {
            world()
            schedule(task(taskName = "secret-digest", d = "x1"), d = "x1")
            schedule()
            val (outcomes, logs) = capturingLogs { runner.poll() }
            assertEquals(RunnerOutcome.UNKNOWN_TASK, outcomes[ScheduleKey(owner.pubKey, "x1")])
            assertEquals(RunnerOutcome.WOKEN, outcomes[mine])
            assertEquals(listOf("observer"), waker.wakes.map { it.second.task })
            assertFalse("secret-digest" in logs, logs)
        }

    @Test
    fun aScheduleNamingSomeoneElseIsRejected() =
        runTest {
            world()
            // The owner names the stranger's Assistant: not this owner's, whatever it says.
            schedule(task(runner = strangersAssistant))
            // And the stranger names the owner's Assistant: anyone can tag a runner.
            schedule(task(runner = assistant), by = stranger)
            val outcomes = runner.poll()
            assertEquals(RunnerOutcome.REJECTED, outcomes[mine])
            assertEquals(RunnerOutcome.REJECTED, outcomes[ScheduleKey(stranger.pubKey, "observer")])
            assertTrue(waker.wakes.isEmpty())
            // Nothing was decrypted for either.
            assertTrue(directory.keysAsked.isEmpty())
        }

    @Test
    fun aScheduleTakenBackOrInConflictIsNotRun() =
        runTest {
            world()
            schedule(task(runner = null))
            assertEquals(RunnerOutcome.NOT_NAMED, runner.poll()[mine])
            val two = task().let { t -> ScheduledTask(t.publicTags + arrayOf(arrayOf("p", stranger.pubKey)), t.privateTags) }
            schedule(two, createdAt = now - 60)
            assertEquals(RunnerOutcome.RUNNER_CONFLICT, runner.poll()[mine])
            assertTrue(waker.wakes.isEmpty())
            assertTrue(directory.keysAsked.isEmpty())
        }

    @Test
    fun withoutAQuorumNothingIsWoken() =
        runTest {
            world()
            schedule()
            // The index has it, but none of the owner's own relays answer: a newer version may be there.
            relays.down = setOf(ownerRelay)
            assertEquals(RunnerOutcome.NO_QUORUM, runner.poll()[mine])
            assertTrue(waker.wakes.isEmpty())
            relays.down = emptySet()
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
        }

    @Test
    fun anOwnerWhoseRelayListWasNotReadHasNoQuorum() =
        runTest {
            world()
            schedule()
            // The index serves the schedule, but CLOSEs the relay-list read: the owner's relays are
            // unknown, not "none", and the index alone can't stand in for them.
            relays.refuses = { relay, filter -> relay == index && filter.kinds == listOf(10002) }
            assertEquals(RunnerOutcome.NO_QUORUM, runner.poll()[mine])
            assertTrue(waker.wakes.isEmpty())
            relays.refuses = { _, _ -> false }
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
        }

    @Test
    fun anOwnerWhoseListedRelaysAreAllRefusedHasNoQuorum() =
        runTest {
            // Every write relay the owner lists is refused (plain ws, an internal host): none is asked.
            relays.put(
                index,
                owner.signed(
                    10002,
                    arrayOf(arrayOf("r", "ws://owner.example", "write"), arrayOf("r", "wss://10.0.0.1", "write")),
                    createdAt = 1,
                ),
            )
            schedule()
            assertEquals(RunnerOutcome.NO_QUORUM, runner.poll()[mine])
            assertTrue(waker.wakes.isEmpty())
        }

    @Test
    fun anOwnerKnownToHaveNoRelayListIsReadOnTheIndex() =
        runTest {
            // The index answered the relay-list read in full and holds none: the index is all there is.
            schedule()
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
        }

    @Test
    fun aWakeIsNotRepeatedWithinTheRetryTime() =
        runTest {
            world()
            schedule()
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            clock += 300
            assertEquals(RunnerOutcome.WOKEN_RECENTLY, runner.poll()[mine])
            assertEquals(1, waker.wakes.size)
            clock += 300
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            assertEquals(2, waker.wakes.size)
        }

    @Test
    fun aSettledAnswerStopsFurtherWakes() =
        runTest {
            world()
            schedule()
            // The service has a run of it (printing, or ended): it answers 200, settled.
            waker.answer = { WakeAnswer.Settled("ATTEMPTED") }
            assertEquals(RunnerOutcome.SETTLED, runner.poll()[mine])
            // Never again for that occurrence, however long after: Quartz reads it as this runner's own run.
            for (later in listOf(600L, 3_600L, 6 * 3_600L)) {
                clock = now + later
                assertEquals(RunnerOutcome.ATTEMPTED, runner.poll()[mine])
            }
            assertEquals(1, waker.wakes.size)
            // The next morning's occurrence is a new one, and is woken.
            waker.answer = { WakeAnswer.Accepted }
            clock = now + 86_400
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            assertEquals(listOf(six, six + 86_400), waker.wakes.map { it.second.occurrence })
        }

    @Test
    fun twoSchedulesOfOneTaskEachSettleTheirOwnOccurrence() =
        runTest {
            world()
            // The same task twice: 06:00 and 06:15. Each occurrence is settled on its own.
            schedule()
            val quarter = ScheduledTask.create("late", "observer", "15 6 * * *", "Europe/London", setAt = 0).withRunner(assistant.pubKey, 0)
            schedule(quarter, d = "late")
            waker.answer = { WakeAnswer.Settled("ATTEMPTED") }
            repeat(6) {
                runner.poll()
                clock += 600
            }
            assertEquals(listOf(six, six + 900), waker.wakes.map { it.second.occurrence }.sorted())
        }

    @Test
    fun twoSchedulesDueAtTheSameOccurrenceWakeItOnce() =
        runTest {
            world()
            schedule()
            schedule(task(d = "again"), d = "again")
            // A wake that takes a moment: both schedules are decided while the first is under way.
            val slow =
                object : TaskWaker {
                    override suspend fun wake(
                        service: String,
                        request: WakeRequest,
                    ): WakeAnswer {
                        delay(100)
                        return waker.wake(service, request)
                    }
                }
            val r =
                Runner(
                    directory = directory,
                    nostr = RunnerNostr(relays, setOf(index), null, now = { clock }),
                    waker = slow,
                    services = services,
                    now = { clock },
                )
            val outcomes = r.poll()
            assertEquals(1, waker.wakes.size)
            assertEquals(listOf(RunnerOutcome.WOKEN, RunnerOutcome.WOKEN_RECENTLY), outcomes.values.sorted())
        }

    @Test
    fun aRestartWakesOnceAndAcceptsTheSettledAnswer() =
        runTest {
            world()
            schedule()
            waker.answer = { WakeAnswer.Settled("ATTEMPTED") }
            assertEquals(RunnerOutcome.SETTLED, runner.poll()[mine])
            assertEquals(RunnerOutcome.ATTEMPTED, runner.poll()[mine])
            // A new process remembers nothing: it wakes the occurrence once more, and the service
            // answers it settled again.
            val restarted = runnerWith()
            clock += 60
            assertEquals(RunnerOutcome.SETTLED, restarted.poll()[mine])
            clock += 600
            assertEquals(RunnerOutcome.ATTEMPTED, restarted.poll()[mine])
            assertEquals(listOf(six, six), waker.wakes.map { it.second.occurrence })
        }

    @Test
    fun aBusyServiceIsWokenAgainAfterItsRetryAfter() =
        runTest {
            world()
            schedule()
            waker.answer = { WakeAnswer.Busy(1_200) }
            assertEquals(RunnerOutcome.BUSY, runner.poll()[mine])
            // Past WAKE_RETRY_SECONDS, but not past the service's Retry-After.
            clock += 900
            assertEquals(RunnerOutcome.WOKEN_RECENTLY, runner.poll()[mine])
            clock += 300
            waker.answer = { WakeAnswer.Busy(60) }
            assertEquals(RunnerOutcome.BUSY, runner.poll()[mine])
            // A short Retry-After is honoured too, under WAKE_RETRY_SECONDS.
            clock += 60
            waker.answer = { WakeAnswer.Accepted }
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            assertEquals(3, waker.wakes.size)
        }

    @Test
    fun aFailedOrRefusedWakeWaitsTheRetryTime() =
        runTest {
            world()
            schedule()
            waker.answer = { WakeAnswer.Failed("http 503") }
            assertEquals(RunnerOutcome.WAKE_FAILED, runner.poll()[mine])
            clock += 599
            assertEquals(RunnerOutcome.WOKEN_RECENTLY, runner.poll()[mine])
            clock += 1
            waker.answer = { WakeAnswer.Malformed }
            assertEquals(RunnerOutcome.WAKE_REFUSED, runner.poll()[mine])
            clock += 599
            assertEquals(RunnerOutcome.WOKEN_RECENTLY, runner.poll()[mine])
            clock += 1
            waker.answer = { WakeAnswer.Settled("NOT_DUE") }
            assertEquals(RunnerOutcome.SETTLED, runner.poll()[mine])
            assertEquals(3, waker.wakes.size)
        }

    @Test
    fun aSettledAnswerThatIsntOneIsAFailedWake() =
        runTest {
            world()
            schedule()
            // A 200 whose body names no outcome: not an answer, so it settles nothing.
            waker.answer = { WakeAnswer.Settled(null) }
            assertEquals(RunnerOutcome.WAKE_FAILED, runner.poll()[mine])
            clock += 600
            waker.answer = { WakeAnswer.Accepted }
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
        }

    @Test
    fun aServiceThatFailsThreeWakesInARowIsNotWokenAgainThisPoll() =
        runTest {
            world()
            schedule()
            // Five more owners, all due: the service is down.
            val more = List(5) { newSigner() to newSigner() }
            val dir = FakeDirectory(directory.assistants + more.associate { (o, a) -> o.pubKey to a })
            for ((o, a) in more) {
                relays.put(index, o.signed(10002, arrayOf(arrayOf("r", ownerRelay.url, "write")), createdAt = 1))
                schedule(task(runner = a), by = o)
            }
            waker.answer = { WakeAnswer.Failed("http 503") }
            val r =
                Runner(dir, RunnerNostr(relays, setOf(index), null, now = { clock }), waker, services, now = { clock }, parallelism = 1)
            val outcomes = r.poll()
            assertEquals(3, waker.wakes.size)
            assertEquals(3, outcomes.values.count { it == RunnerOutcome.SERVICE_UNAVAILABLE })
            // The next poll tries again.
            clock += 600
            waker.answer = { WakeAnswer.Accepted }
            assertEquals(6, r.poll().values.count { it == RunnerOutcome.WOKEN })
        }

    @Test
    fun anOwnerOffTheIndexWithAWakePendingIsReadEveryPoll() =
        runTest {
            world()
            // The index doesn't mirror this schedule: it is read on the owner's relays, hourly at most.
            schedule(onIndex = false)
            waker.answer = { WakeAnswer.Failed("http 503") }
            assertEquals(RunnerOutcome.WAKE_FAILED, runner.poll()[mine])
            // Ten minutes on, its retry is due: the owner's relays are read again, not an hour later.
            clock += 600
            waker.answer = { WakeAnswer.Accepted }
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
        }

    @Test
    fun aKeyThatCantBeHadIsReportedAndTheScheduleIsDecryptedOncePerVersion() =
        runTest {
            world()
            schedule()
            directory.failures[owner.pubKey] = KeyFailure.NSEC_UNREADABLE
            assertEquals(RunnerOutcome.NSEC_UNREADABLE, runner.poll()[mine])
            directory.failures[owner.pubKey] = KeyFailure.NSEC_MISMATCH
            assertEquals(RunnerOutcome.NSEC_MISMATCH, runner.poll()[mine])
            directory.failures.clear()
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            val asked = directory.keysAsked.size
            clock += 600
            assertEquals(RunnerOutcome.WOKEN, runner.poll()[mine])
            // The plaintext is kept by event id: the same version isn't decrypted again.
            assertEquals(asked, directory.keysAsked.size)
        }

    @Test
    fun aScheduleTheAssistantCantOpenIsUnreadable() =
        runTest {
            world()
            // It names the Assistant in public, but its private tags were encrypted to the owner alone.
            val private = PrivateTagsInContent.encode(task().privateTags)
            val tags = arrayOf(arrayOf("d", "observer"), arrayOf("p", assistant.pubKey))
            val event = owner.signed(ScheduledTaskEvent.KIND, tags, owner.nip44Encrypt(private, owner.pubKey), createdAt = now - 60)
            relays.put(ownerRelay, event)
            relays.put(index, event)
            assertEquals(RunnerOutcome.UNREADABLE, runner.poll()[mine])
            assertTrue(waker.wakes.isEmpty())
        }

    @Test
    fun noKeyNsecPlaintextOrCiphertextIsLogged() =
        runTest {
            world()
            val secretParam = "sk-or-v1-this-must-never-be-logged-a3f9"
            val event = schedule(task().withParam("key", listOf("https://openrouter.ai/api/v1", secretParam)))
            val unknown = schedule(task(taskName = "hidden-task", d = "h"), d = "h")
            directory.failures[stranger.pubKey] = KeyFailure.NSEC_MISMATCH
            schedule(task(runner = strangersAssistant), by = stranger)
            val (_, logs) =
                capturingLogs {
                    runner.poll()
                    clock += 600
                    runner.poll()
                }
            val secrets =
                listOf(secretParam, "hidden-task", event.content, unknown.content) +
                    listOf(assistant, strangersAssistant).flatMap { listOf(it.keyPair.privKey!!.toHexKey(), it.keyPair.privKey!!.toNsec()) }
            for (secret in secrets) assertFalse(secret in logs, "logged a secret")
            assertTrue("owner=${owner.pubKey}" in logs)
        }

    @Test
    fun theKeysAPollNeedsAreReadInOneBatchAndWipedAfter() =
        runTest {
            world()
            schedule()
            schedule(task(runner = strangersAssistant), by = stranger)
            runner.poll()
            assertEquals(1, directory.batches)
            assertEquals(setOf(owner.pubKey, stranger.pubKey), directory.keysAsked.toSet())
            // Every private key it was handed is zeroed once the poll is done with it.
            assertTrue(directory.handedOut.isNotEmpty())
            assertTrue(directory.handedOut.all { s -> s.keyPair.privKey!!.all { it == 0.toByte() } })
            // Nothing new to decrypt: no batch at all.
            clock += 600
            runner.poll()
            assertEquals(1, directory.batches)
        }

    @Test
    fun aScheduleThatWontDecryptIsNotTriedAgain() =
        runTest {
            world()
            val private = PrivateTagsInContent.encode(task().privateTags)
            val tags = arrayOf(arrayOf("d", "observer"), arrayOf("p", assistant.pubKey))
            val event = owner.signed(ScheduledTaskEvent.KIND, tags, owner.nip44Encrypt(private, owner.pubKey), createdAt = now - 60)
            relays.put(ownerRelay, event)
            relays.put(index, event)
            assertEquals(RunnerOutcome.UNREADABLE, runner.poll()[mine])
            val asked = directory.keysAsked.size
            clock += 600
            assertEquals(RunnerOutcome.UNREADABLE, runner.poll()[mine])
            assertEquals(asked, directory.keysAsked.size)
        }
}
