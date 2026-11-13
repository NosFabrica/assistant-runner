package com.nosfabrica.assistant.runner

import com.nosfabrica.assistant.core.MemoryRunStore
import com.nosfabrica.assistant.core.RunStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTask
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTaskEvent
import java.time.Instant

/**
 * One owner with an Assistant (and a stranger with theirs), their relays and lists, and a runner
 * over fakes: what every runner test starts from. Ported from the-nostr-observer's.
 */
abstract class RunnerWorld {
    protected val owner = newSigner()
    protected val assistant = newSigner()
    protected val stranger = newSigner()
    protected val strangersAssistant = newSigner()

    protected val index = relay("wss://index.example")
    protected val ownerRelay = relay("wss://owner.example")

    // 06:30 UTC on 12 November 2026: London's 06:00 run is due.
    protected val now = Instant.parse("2026-11-12T06:30:00Z").epochSecond

    /** That morning's occurrence. */
    protected val six = Instant.parse("2026-11-12T06:00:00Z").epochSecond

    /** The runner's clock: [now] unless a test moves it. */
    protected var clock = now

    protected val observerService = "http://observer:8080"

    protected val relays = FakeRelays()
    protected val runs: RunStore = MemoryRunStore()
    protected val directory = FakeDirectory(mapOf(owner.pubKey to assistant, stranger.pubKey to strangersAssistant))
    protected val waker = FakeWaker()
    protected val services = TaskServices(mapOf("observer" to observerService))

    protected val runner by lazy { runnerWith() }

    protected fun runnerWith(
        access: RelayAccess = relays,
        accepting: () -> Boolean = { true },
        parallelism: Int = Runner.PARALLELISM,
        wakeRetrySeconds: Long = Runner.DEFAULT_WAKE_RETRY_SECONDS,
    ) = Runner(
        directory = directory,
        nostr = RunnerNostr(access, setOf(index), null, now = { clock }),
        runs = runs,
        waker = waker,
        services = services,
        now = { clock },
        wakeRetrySeconds = wakeRetrySeconds,
        parallelism = parallelism,
        accepting = accepting,
    )

    protected suspend fun world() {
        relays.put(
            index,
            owner.signed(10002, arrayOf(arrayOf("r", ownerRelay.url, "write")), createdAt = 1),
            stranger.signed(10002, arrayOf(arrayOf("r", ownerRelay.url, "write")), createdAt = 1),
        )
    }

    protected fun task(
        taskName: String = "observer",
        d: String = "observer",
        runner: NostrSignerInternal? = assistant,
    ): ScheduledTask {
        val t = ScheduledTask.create(d, taskName, "0 6 * * *", "Europe/London", setAt = 0)
        return if (runner == null) t else t.withRunner(runner.pubKey, 0)
    }

    protected suspend fun schedule(
        task: ScheduledTask = task(),
        by: NostrSignerInternal = owner,
        d: String = "observer",
        onIndex: Boolean = true,
        createdAt: Long = now - 3_600,
    ): Event {
        val event = typed(by.sign(ScheduledTaskEvent.update(task, by, d, createdAt = createdAt)))
        relays.put(ownerRelay, event)
        // Brainstorm's index relay mirrors it: the runner finds schedules there first.
        if (onIndex) relays.put(index, event)
        return event
    }
}
