package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.DONE_REASON_EOSE
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllWithHooks
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nipFBScheduledTasks.ScheduledTaskEvent
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.TimeUtils
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap

/**
 * The search relay's token for an unranked read: it CLOSEs a REQ whose `search` names neither
 * `observer:<hex>` nor this (the-nostr-observer's `INCLUDE_SPAM`).
 */
const val INCLUDE_SPAM = "include:spam"

/** The newest of [events] by `created_at`, then lowest id, as NIP-01 picks a replaceable. */
internal fun newestOf(events: Collection<Event>): Event? =
    events.maxWithOrNull(compareBy<Event> { it.createdAt }.thenByDescending { it.id })

/** What a read across some relays returned, and which of them answered it in full. */
class RelayRead(
    val events: List<Event>,
    /** The relays that reached EOSE: on those, a missing event is missing, not unknown. */
    val served: Set<NormalizedRelayUrl>,
    /** The relays that answered with a CLOSED (not an auth wall): the REQ itself, maybe its size, was refused. */
    val refused: Set<NormalizedRelayUrl> = emptySet(),
    /** The relays the read's event cap cut off: they sent more than it allows (at least the `limit`), so there is more to page. */
    val capped: Set<NormalizedRelayUrl> = emptySet(),
    /** Of [refused], those whose CLOSED said `rate-limited:` or `blocked:`: asking again, smaller, only costs more. */
    val throttled: Set<NormalizedRelayUrl> = emptySet(),
    /** How many events the relays sent, before any was checked: what says a page was full. */
    val sent: Int = events.size,
) {
    companion object {
        /** A CLOSED that says the relay won't serve this client now, whatever it asks: NIP-01's `rate-limited:` and `blocked:`. */
        fun isFinalRefusal(message: String): Boolean = message.startsWith("rate-limited:") || message.startsWith("blocked:")
    }

    /** At least one relay reached EOSE: an empty [events] then means "none", not "unknown". */
    val anyServed: Boolean get() = served.isNotEmpty()

    /** Whether any of [relays] answered: a read judged by the relays that hold the data, not by any that happen to be asked too. */
    fun servedAny(relays: Set<NormalizedRelayUrl>): Boolean = served.any { it in relays }
}

/**
 * The runner's whole use of relays: read with a filter. It never publishes (the-nostr-observer's
 * copy also publishes; the runner signs nothing). Tests fake this.
 */
interface RelayAccess {
    suspend fun fetch(
        relays: Set<NormalizedRelayUrl>,
        filter: Filter,
    ): RelayRead
}

/**
 * [RelayAccess] through Quartz's client. Every relay is asked on its own subscription, in
 * parallel, and each one is bounded twice: by the wall clock ([deadlineMs]) and by how many events
 * it may send ([maxEvents]). Quartz's own window is an idle one, reset by every event, so a relay
 * streaming junk would otherwise hold the read, and the poll, forever. A relay cut off by either
 * bound didn't serve the read: what it sent that verifies is kept, but it never counts toward a
 * quorum. One relay on its own subscription also means a misbehaving one costs only its own
 * answer, never its neighbours'.
 *
 * What bounds a single WebSocket message is not here: OkHttp has no inbound message limit, and
 * buffers a whole message before handing it on. The relay client's sockets are bounded instead
 * ([ByteBudgetSocketFactory]).
 */
class QuartzRelayAccess(
    private val client: INostrClient,
    private val idleMs: Long = 10_000,
    private val deadlineMs: Long = FETCH_DEADLINE_MS,
    private val maxEvents: Int = MAX_EVENTS_PER_RELAY,
) : RelayAccess {
    override suspend fun fetch(
        relays: Set<NormalizedRelayUrl>,
        filter: Filter,
    ): RelayRead {
        if (relays.isEmpty()) return RelayRead(emptyList(), emptySet())
        val reads = coroutineScope { relays.map { relay -> async { fetchOne(relay, filter) } }.awaitAll() }
        // Every copy, unchecked and not deduplicated: RunnerNostr verifies, then deduplicates, so a
        // forged copy under a real event's id can't hide it.
        return RelayRead(
            reads.flatMap { it.events },
            reads.flatMapTo(HashSet()) { it.served },
            reads.flatMapTo(HashSet()) { it.refused },
            capped = reads.flatMapTo(HashSet()) { it.capped },
            throttled = reads.flatMapTo(HashSet()) { it.throttled },
            sent = reads.sumOf { it.sent },
        )
    }

    /** Thrown from the event hook to end a relay's read: the hook's exception unsubscribes it. */
    private class TooManyEvents : Exception()

    private suspend fun fetchOne(
        relay: NormalizedRelayUrl,
        filter: Filter,
    ): RelayRead {
        // Collected here, not from the result: a read cut off by the deadline returns nothing.
        val kept = ArrayList<Event>()
        var sent = 0
        // Never under the REQ's own limit: a relay may send as many events as it was asked for.
        val cap = maxOf(maxEvents, filter.limit ?: 0)
        var capped = false
        val result =
            try {
                withTimeoutOrNull(deadlineMs) {
                    client.fetchAllWithHooks(mapOf(relay to listOf(filter)), idleTimeoutMs = idleMs) { _, e ->
                        if (++sent > cap) throw TooManyEvents()
                        // Kept unchecked: RunnerNostr verifies each copy once. The cap bounds what this holds.
                        synchronized(kept) { kept += e }
                        false
                    }
                }
            } catch (e: TooManyEvents) {
                capped = true
                null
            }
        if (result == null) Log.d(TAG) { "relay=${relay.url} cut off: " + if (capped) "more than $cap events" else "the deadline" }
        val reason = result?.doneReasons?.get(relay)
        val closed = reason?.takeIf { it.startsWith("closed:") }?.removePrefix("closed:")
        return RelayRead(
            synchronized(kept) { kept.toList() },
            if (reason == DONE_REASON_EOSE) setOf(relay) else emptySet(),
            if (closed != null) setOf(relay) else emptySet(),
            capped = if (capped) setOf(relay) else emptySet(),
            throttled = if (closed != null && RelayRead.isFinalRefusal(closed.trim())) setOf(relay) else emptySet(),
            sent = sent,
        )
    }

    companion object {
        private const val TAG = "QuartzRelayAccess"

        /** The longest one relay may take to answer one read, whatever it sends meanwhile. */
        const val FETCH_DEADLINE_MS = 20_000L

        /**
         * The most events one relay may send for one read, and never under the read's `limit`.
         * A page is at most [RunnerNostr.PAGE_LIMIT] events, so this is four pages: past it, a relay
         * is sending junk, or more than it was asked for. A relay cut off here is [RelayRead.capped],
         * and its read is paged on from the oldest event it sent.
         */
        const val MAX_EVENTS_PER_RELAY = 4 * RunnerNostr.PAGE_LIMIT
    }
}

/**
 * What a read of someone's NIP-65 list found: their write relays, and whether that answer is one a
 * quorum can be judged on.
 */
class WriteRelays(
    /** Their write relays, vetted (`wss`, no internal host); empty unless [state] is [State.LISTED]. */
    val relays: Set<NormalizedRelayUrl>,
    val state: State,
) {
    enum class State {
        /** Their newest list was read, and names [relays]. */
        LISTED,

        /** An index relay answered for them in full and holds no list, or one naming no write relay: the index relays stand in. */
        NONE,

        /** No index relay answered for them in full (CLOSED, a timeout, a cap): their relays are unknown, not none. */
        UNKNOWN,

        /** Their list names write relays and every one was refused (`ws`, an internal host): there is nowhere to ask. */
        REFUSED,
    }

    companion object {
        val UNKNOWN = WriteRelays(emptySet(), State.UNKNOWN)
    }
}

/**
 * The newest version of one of an owner's schedules (one `d`), as read, and whether enough of
 * their relays answered to trust that it is the newest.
 */
class ScheduleRead(
    val event: ScheduledTaskEvent,
    /**
     * More than half of the owner's write relays (the index relays, when they are known to list
     * none) reached EOSE. Never when their list couldn't be read or names only refused relays.
     */
    val quorum: Boolean,
)

/**
 * The reads the runner makes, on top of [RelayAccess]. A person's lists come from their own
 * NIP-65 write relays, found through the [index] relays (the search relay mirrors replaceable
 * kinds, but answers only a `search` that says `include:spam`).
 *
 * Nothing a relay sends is taken as read until its id and signature verify and it isn't dated
 * more than [MAX_FUTURE_SECONDS] ahead ([acceptable]): anyone can hand a relay an event with the
 * owner's or the Assistant's pubkey on it.
 */
class RunnerNostr(
    val access: RelayAccess,
    private val index: Set<NormalizedRelayUrl>,
    private val searchRelay: NormalizedRelayUrl?,
    private val now: () -> Long = { TimeUtils.now() },
    /** How long a person's NIP-65 list is reused before it is read again. */
    private val relayListTtlSeconds: Long = RELAY_LIST_TTL_SECONDS,
    /**
     * The longest one read may take, whatever [access] does: a backstop over its own per-relay
     * bound ([QuartzRelayAccess.FETCH_DEADLINE_MS]). Past it, nothing was served.
     */
    private val deadlineMs: Long = READ_DEADLINE_MS,
    /** How many relays a batch read asks at once ([schedules], [writeRelaysOf]). */
    private val relayParallelism: Int = RELAY_PARALLELISM,
    /** The authors a batch read asks one relay for in one REQ ([byAuthors]). */
    private val authorsPerReq: Int = AUTHORS_PER_REQ,
    /** The smallest batch a relay that CLOSEs one is asked again with. */
    private val minChunk: Int = MIN_CHUNK,
    /** The schedules one owner is assumed to hold on one relay ([SCHEDULES_PER_OWNER]). */
    private val schedulesPerOwner: Int = SCHEDULES_PER_OWNER,
) {
    private class CachedRelays(
        val relays: WriteRelays,
        val at: Long,
    )

    private val relayLists = ConcurrentHashMap<HexKey, CachedRelays>()

    /** When each owner with no schedule on the index relays last had their own relays asked. */
    private val offIndexCheckedAt = ConcurrentHashMap<HexKey, Long>()

    /** A filter as one relay must be asked it: the search relay closes a REQ without a token. */
    private fun forRelay(
        relay: NormalizedRelayUrl,
        filter: Filter,
    ) = if (relay == searchRelay) filter.copy(search = INCLUDE_SPAM) else filter

    private suspend fun fetch(
        relays: Set<NormalizedRelayUrl>,
        filter: Filter,
    ): RelayRead {
        val at = now()
        val raw =
            withTimeoutOrNull(deadlineMs) {
                if (searchRelay == null || searchRelay !in relays) {
                    access.fetch(relays, filter)
                } else {
                    coroutineScope {
                        val others = async { access.fetch(relays - searchRelay, filter) }
                        val search = async { access.fetch(setOf(searchRelay), forRelay(searchRelay, filter)) }
                        val (o, s) = others.await() to search.await()
                        RelayRead(
                            o.events + s.events,
                            o.served + s.served,
                            o.refused + s.refused,
                            capped = o.capped + s.capped,
                            throttled = o.throttled + s.throttled,
                            sent = o.sent + s.sent,
                        )
                    }
                }
            } ?: RelayRead(emptyList(), emptySet()).also { Log.d(TAG) { "a read of ${relays.size} relays ran past ${deadlineMs}ms" } }
        // Checked first, then deduplicated: a forged copy under a real event's id must not hide it.
        return RelayRead(
            raw.events.filter { acceptable(it, at) }.distinctBy { it.id },
            raw.served,
            raw.refused,
            capped = raw.capped,
            throttled = raw.throttled,
            sent = raw.sent,
        )
    }

    /** [pubkey]'s NIP-65 write relays, as `wss` URLs only; empty when they published none or they couldn't be read. Cached ([relayListTtlSeconds]). */
    suspend fun writeRelays(pubkey: HexKey): Set<NormalizedRelayUrl> = writeRelaysOf(listOf(pubkey))[pubkey]?.relays.orEmpty()

    /**
     * Each of [pubkeys]' NIP-65 write relays, read from the index relays ([byAuthors]), and
     * reused for [relayListTtlSeconds]: [WriteRelays.State.UNKNOWN] for one no index relay
     * answered for in full, which isn't cached (the next call asks again). Lists past their time
     * are dropped, so people the runner no longer asks about don't stay in memory.
     */
    suspend fun writeRelaysOf(pubkeys: Collection<HexKey>): Map<HexKey, WriteRelays> {
        val at = now()
        relayLists.entries.removeIf { at - it.value.at >= relayListTtlSeconds }
        val out = HashMap<HexKey, WriteRelays>()
        val stale = ArrayList<HexKey>()
        for (key in pubkeys.distinct()) {
            val cached = relayLists[key]
            if (cached != null) out[key] = cached.relays else stale += key
        }
        if (stale.isEmpty()) return out
        val reads =
            onEach(index) { relay -> byAuthors(relay, stale) { Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = it) } }
        val newest =
            reads
                .flatMap { it.events }
                .filterIsInstance<AdvertisedRelayListEvent>()
                .groupBy { it.pubKey }
                .mapValues { (_, lists) -> newestOf(lists) as AdvertisedRelayListEvent }
        val covered = reads.flatMapTo(HashSet()) { it.covered }
        for (key in stale) {
            val list = newest[key]
            val relays =
                when {
                    list != null -> vetted(list)
                    key in covered -> WriteRelays(emptySet(), WriteRelays.State.NONE)
                    else -> WriteRelays.UNKNOWN
                }
            out[key] = relays
            if (relays.state != WriteRelays.State.UNKNOWN) relayLists[key] = CachedRelays(relays, at)
        }
        return out
    }

    /**
     * `writeRelays()` doesn't vet schemes; and a relay that names an internal host is refused
     * here. A list whose every write relay is refused is [WriteRelays.State.REFUSED], not "none":
     * the owner's newest schedule is on relays the runner won't ask, so the index can't stand in.
     */
    private fun vetted(list: AdvertisedRelayListEvent): WriteRelays {
        val listed = list.writeRelays().orEmpty()
        if (listed.isEmpty()) return WriteRelays(emptySet(), WriteRelays.State.NONE)
        val relays =
            listed
                .filter { UrlGuard.refusal(it, setOf("wss")) == null }
                .mapNotNull { RelayUrlNormalizer.normalizeOrNull(it) }
                .toSet()
        return WriteRelays(relays, if (relays.isEmpty()) WriteRelays.State.REFUSED else WriteRelays.State.LISTED)
    }

    /** What [byAuthors] read on one relay: the events, and the authors it answered for in full. */
    private class AuthorsRead(
        val events: List<Event>,
        val covered: Set<HexKey>,
    )

    /**
     * [authors]' events of one kind on [relay], in chunks of [authorsPerReq] with `limit` set to
     * the chunk's size times [perAuthor] (the events one author may hold for the filter: one for a
     * replaceable kind, more for an addressable one, which an author holds one of per `d`), at
     * most [PAGE_LIMIT], and the authors the relay answered for in full.
     *
     * A relay answered for a chunk when it reached EOSE on a page that wasn't full. A full page (as
     * many events sent as the limit, or [FULL_PAGE_FLOOR], the smallest page a relay is assumed to
     * cut to without saying so, counted before any is checked) and a page the read's event cap cut
     * off ([RelayRead.capped]) are paged on: the same chunk asked again with `until` at the oldest
     * event sent (inclusive; copies are deduplicated by id), until a page isn't full. A page that
     * brings nothing new can't be paged past: its chunk is asked again in two halves. With one
     * event an author ([perAuthor] 1), a full page answers for the authors who sent theirs, and the
     * rest are asked again without them.
     *
     * A CLOSED (a REQ too large for the relay) is asked again in two halves of that chunk, down
     * to [minChunk]; a single author is never asked again, and nothing more is asked of a relay
     * whose CLOSED says `rate-limited:` or `blocked:`. A relay that doesn't answer (down, slow) isn't
     * asked again: it answered for nobody.
     *
     * At most [MAX_REQS_PER_RELAY] REQs for each [authorsPerReq] authors: the cap scales with the
     * owners, so a large poll isn't cut short of quorum by its size alone.
     */
    private suspend fun byAuthors(
        relay: NormalizedRelayUrl,
        authors: Collection<HexKey>,
        perAuthor: Int = 1,
        filterOf: (List<HexKey>) -> Filter,
    ): AuthorsRead {
        val events = LinkedHashMap<String, Event>()
        val covered = HashSet<HexKey>()
        val all = authors.distinct().sorted()
        val queue = ArrayDeque(all.chunked(authorsPerReq))
        var asked = 0
        val maxReqs = MAX_REQS_PER_RELAY * ((all.size + authorsPerReq - 1) / authorsPerReq).coerceAtLeast(1)
        chunks@ while (queue.isNotEmpty()) {
            var chunk = queue.removeFirst()
            var until: Long? = null
            while (true) {
                if (asked++ >= maxReqs) {
                    Log.d(TAG) { "relay=${relay.url} asked $maxReqs REQs: the rest isn't answered for" }
                    break@chunks
                }
                val wanted = chunk.toHashSet()
                val limit = minOf(chunk.size * perAuthor, PAGE_LIMIT)
                val read = fetch(setOf(relay), filterOf(chunk).copy(limit = limit, until = until))
                var fresh = 0
                for (e in read.events) if (e.pubKey in wanted && events.putIfAbsent(e.id, e) == null) fresh++
                val served = relay in read.served
                when {
                    served && read.sent < minOf(limit, FULL_PAGE_FLOOR) -> {
                        covered += chunk
                        break
                    }

                    (served || relay in read.capped) && perAuthor == 1 -> {
                        // One event an author: who sent theirs is answered for; the rest are asked again.
                        val sent = read.events.mapNotNullTo(HashSet()) { e -> e.pubKey.takeIf { it in wanted } }
                        covered += sent
                        if (sent.isEmpty()) break
                        chunk = chunk.filter { it !in sent }
                        if (chunk.isEmpty()) break
                    }

                    served || relay in read.capped -> {
                        // A full page, or one the cap cut: page on from the oldest event it sent.
                        val oldest = read.events.minOfOrNull { it.createdAt }
                        if (oldest != null && (until == null || fresh > 0)) {
                            until = oldest
                        } else {
                            // Nothing new past this page (more events in one second than a page holds): smaller, or given up.
                            split(chunk, queue)
                            break
                        }
                    }

                    relay in read.throttled -> {
                        break@chunks
                    }

                    relay in read.refused -> {
                        if (chunk.size > minChunk) split(chunk, queue)
                        break
                    }

                    else -> {
                        break
                    }
                }
            }
        }
        return AuthorsRead(events.values.toList(), covered)
    }

    /** [chunk] asked again as two halves, first; a single author is never asked again. */
    private fun split(
        chunk: List<HexKey>,
        queue: ArrayDeque<List<HexKey>>,
    ) {
        if (chunk.size <= 1) return
        val half = (chunk.size + 1) / 2
        queue.addFirst(chunk.drop(half))
        queue.addFirst(chunk.take(half))
    }

    /** [block] on each of [relays], [relayParallelism] at a time. */
    private suspend fun <T> onEach(
        relays: Collection<NormalizedRelayUrl>,
        block: suspend (NormalizedRelayUrl) -> T,
    ): List<T> {
        val gate = Semaphore(relayParallelism)
        return coroutineScope { relays.map { relay -> async { gate.withPermit { block(relay) } } }.awaitAll() }
    }

    /**
     * The newest version of every schedule (kind 34817, one per `d`) of every owner, and whether
     * it was read with a quorum. Asked by author, never by `#p`: an owner who took a task back
     * published a newer version without the `p`, and a `#p` read would never see it, so a relay
     * still holding the older version would keep the runner waking. Nor by `#d`: the runner runs
     * any task a service is configured for, and a schedule's `d` is only its id. The caller checks
     * that the newest names exactly this owner's Assistant.
     *
     * The index relays are asked first, for every owner; only the owners with a schedule there
     * (and, hourly, those without: [OFF_INDEX_RECHECK_SECONDS]) have their own write relays asked
     * too. Every relay is asked on its own, [relayParallelism] at a time ([byAuthors]), so one
     * slow relay costs its own deadline, not the sum of everyone's.
     */
    suspend fun schedules(owners: Collection<HexKey>): List<ScheduleRead> {
        if (owners.isEmpty()) return emptyList()
        val found = HashMap<HexKey, MutableList<Event>>()
        val servedBy = HashMap<HexKey, MutableSet<NormalizedRelayUrl>>()

        suspend fun ask(byRelay: Map<NormalizedRelayUrl, List<HexKey>>) {
            val reads =
                onEach(byRelay.keys) { relay ->
                    relay to byAuthors(relay, byRelay.getValue(relay), schedulesPerOwner, ::scheduleFilter)
                }
            // Merged here, on one coroutine: the reads ran in parallel.
            for ((relay, read) in reads) {
                read.events.forEach { found.getOrPut(it.pubKey) { ArrayList() } += it }
                read.covered.forEach { servedBy.getOrPut(it) { HashSet() } += relay }
            }
        }

        fun schedulesOf(owner: HexKey) = found[owner].orEmpty().filterIsInstance<ScheduledTaskEvent>().filter { it.pubKey == owner }

        val all = owners.distinct()
        ask(index.associateWith { all })
        val at = now()
        // An owner the index shows no schedule for is still looked for on their own relays, but
        // only every [OFF_INDEX_RECHECK_SECONDS]: a schedule the index missed is found late, never lost.
        val scheduled =
            all.filter { owner ->
                schedulesOf(owner).isNotEmpty() ||
                    (offIndexCheckedAt[owner]?.let { at - it >= OFF_INDEX_RECHECK_SECONDS } ?: true).also {
                        if (it) offIndexCheckedAt[owner] = at
                    }
            }
        offIndexCheckedAt.keys.retainAll(all.toSet())
        val ownRelays = writeRelaysOf(scheduled)
        val byRelay = HashMap<NormalizedRelayUrl, MutableList<HexKey>>()
        for (owner in scheduled) {
            ownRelays[owner]
                ?.relays
                .orEmpty()
                .filter { it !in index }
                .forEach { byRelay.getOrPut(it) { ArrayList() } += owner }
        }
        ask(byRelay)

        return scheduled.flatMap { owner ->
            val quorum = hasQuorum(ownRelays[owner] ?: WriteRelays.UNKNOWN, servedBy[owner].orEmpty())
            schedulesOf(owner)
                .groupBy { it.dTag() }
                .values
                .mapNotNull { versions -> (newestOf(versions) as? ScheduledTaskEvent)?.let { ScheduleRead(it, quorum) } }
        }
    }

    /**
     * More than half of [list]'s relays are among [served]: the index relays only when the
     * owner is known to list none. Their list unread, or naming only refused relays, is never a
     * quorum: the index alone would then vouch for a schedule a newer version may have replaced.
     */
    private fun hasQuorum(
        list: WriteRelays,
        served: Set<NormalizedRelayUrl>,
    ): Boolean {
        val asked =
            when (list.state) {
                WriteRelays.State.LISTED -> list.relays
                WriteRelays.State.NONE -> index
                WriteRelays.State.UNKNOWN, WriteRelays.State.REFUSED -> return false
            }
        return asked.isNotEmpty() && asked.count { it in served } * 2 > asked.size
    }

    private fun scheduleFilter(authors: List<HexKey>) = Filter(kinds = listOf(ScheduledTaskEvent.KIND), authors = authors)

    companion object {
        private const val TAG = "RunnerNostr"

        /**
         * The schedules (and stale versions of them) one owner is assumed to hold on one relay: a
         * schedule read's `limit` is this many per author. An owner past it on a relay isn't
         * answered for there, which costs a quorum, never a wrong decision.
         */
        const val SCHEDULES_PER_OWNER = 16

        /** One 64-hex author is 67 bytes of JSON: 500 is far under the search relay's 262,144-byte REQ frame. */
        const val AUTHORS_PER_REQ = 500

        /**
         * A page this long may be a relay's own cut, made without saying so (strfry cuts at 500,
         * others lower): it is paged on with `until` ([byAuthors]).
         */
        const val FULL_PAGE_FLOOR = 100

        /** The largest `limit` asked for: strfry's own cap, and four of them make [QuartzRelayAccess.MAX_EVENTS_PER_RELAY]. */
        const val PAGE_LIMIT = 500

        /** The smallest chunk a relay that CLOSEs a batch is asked again with. */
        const val MIN_CHUNK = 25

        /** The most REQs [byAuthors] sends one relay for each [AUTHORS_PER_REQ] authors of one batch read. */
        const val MAX_REQS_PER_RELAY = 64

        /** How many relays a batch read asks at once: with the read budget, what bounds a poll's memory ([RELAY_WORST_CASE_BYTES]). */
        const val RELAY_PARALLELISM = 8

        /** [QuartzRelayAccess.FETCH_DEADLINE_MS] and a little more: the backstop over any [RelayAccess]. */
        const val READ_DEADLINE_MS = QuartzRelayAccess.FETCH_DEADLINE_MS + 5_000

        /** An hour: a person's relays rarely move, and a stale list only costs a missed read. */
        const val RELAY_LIST_TTL_SECONDS = 3_600L

        /** How often an owner the index shows no schedule for has their own relays asked anyway. */
        const val OFF_INDEX_RECHECK_SECONDS = 3_600L

        /** How far ahead of the runner's clock an event may be dated and still be read. */
        const val MAX_FUTURE_SECONDS = 600L

        /**
         * Whether [event] may be read at all: its id and signature verify and its `created_at` is
         * at most [MAX_FUTURE_SECONDS] ahead. A forged site dated next year would otherwise win
         * every newest-of read, and `sign` would refuse every publish after it.
         */
        fun acceptable(
            event: Event,
            at: Long = TimeUtils.now(),
        ): Boolean = event.createdAt <= at + MAX_FUTURE_SECONDS && event.verify()

        fun relaysOf(urls: List<String>): Set<NormalizedRelayUrl> =
            urls.mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }.toSet()
    }
}
