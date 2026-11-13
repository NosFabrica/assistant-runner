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
) {
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
        return RelayRead(
            reads.flatMap { it.events }.distinctBy { it.id },
            reads.flatMapTo(HashSet()) { it.served },
            reads.flatMapTo(HashSet()) { it.refused },
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
        val result =
            try {
                withTimeoutOrNull(deadlineMs) {
                    client.fetchAllWithHooks(mapOf(relay to listOf(filter)), idleTimeoutMs = idleMs) { _, e ->
                        if (++sent > maxEvents) throw TooManyEvents()
                        // Checked again by RunnerNostr; dropping a forgery here keeps it out of memory.
                        if (RunnerNostr.acceptable(e)) synchronized(kept) { kept += e }
                        false
                    }
                }
            } catch (e: TooManyEvents) {
                null
            }
        if (result == null) Log.d(TAG) { "relay=${relay.url} cut off: deadline or more than $maxEvents events" }
        val reason = result?.doneReasons?.get(relay)
        return RelayRead(
            synchronized(kept) { kept.toList() },
            if (reason == DONE_REASON_EOSE) setOf(relay) else emptySet(),
            if (reason?.startsWith("closed:") == true) setOf(relay) else emptySet(),
        )
    }

    companion object {
        private const val TAG = "QuartzRelayAccess"

        /** The longest one relay may take to answer one read, whatever it sends meanwhile. */
        const val FETCH_DEADLINE_MS = 20_000L

        /**
         * The most events one relay may send for one read. A page the runner can use is under
         * [RunnerNostr.FULL_PAGE_FLOOR] events (a fuller one is asked again in smaller chunks), and
         * the largest chunk is [RunnerNostr.AUTHORS_PER_REQ] owners: four times that is a relay
         * holding old versions too, and anything past it is a relay sending junk.
         */
        const val MAX_EVENTS_PER_RELAY = 4 * RunnerNostr.AUTHORS_PER_REQ
    }
}

/**
 * The newest version of one of an owner's schedules (one `d`), as read, and whether enough of
 * their relays answered to trust that it is the newest.
 */
class ScheduleRead(
    val event: ScheduledTaskEvent,
    /** More than half of the owner's write relays (the index relays, when they list none) reached EOSE. */
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
        val relays: Set<NormalizedRelayUrl>,
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
                        RelayRead(o.events + s.events, o.served + s.served, o.refused + s.refused)
                    }
                }
            } ?: RelayRead(emptyList(), emptySet()).also { Log.d(TAG) { "a read of ${relays.size} relays ran past ${deadlineMs}ms" } }
        return RelayRead(raw.events.distinctBy { it.id }.filter { acceptable(it, at) }, raw.served, raw.refused)
    }

    /** [pubkey]'s NIP-65 write relays, as `wss` URLs only; empty when they published none. Cached ([relayListTtlSeconds]). */
    suspend fun writeRelays(pubkey: HexKey): Set<NormalizedRelayUrl> = writeRelaysOf(listOf(pubkey))[pubkey].orEmpty()

    /**
     * Each of [pubkeys]' NIP-65 write relays, read from the index relays ([byAuthors]), and
     * reused for [relayListTtlSeconds]. A list no index relay answered for in full isn't cached:
     * the next call asks again. Lists past their time are dropped, so people the runner no
     * longer asks about don't stay in memory.
     */
    suspend fun writeRelaysOf(pubkeys: Collection<HexKey>): Map<HexKey, Set<NormalizedRelayUrl>> {
        val at = now()
        relayLists.entries.removeIf { at - it.value.at >= relayListTtlSeconds }
        val out = HashMap<HexKey, Set<NormalizedRelayUrl>>()
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
            val relays = newest[key]?.let(::vetted).orEmpty()
            out[key] = relays
            if (key in covered || key in newest) relayLists[key] = CachedRelays(relays, at)
        }
        return out
    }

    /** `writeRelays()` doesn't vet schemes; and a relay that names an internal host is refused here. */
    private fun vetted(list: AdvertisedRelayListEvent): Set<NormalizedRelayUrl> =
        list
            .writeRelays()
            .orEmpty()
            .filter { UrlGuard.refusal(it, setOf("wss")) == null }
            .mapNotNull { RelayUrlNormalizer.normalizeOrNull(it) }
            .toSet()

    /** What [byAuthors] read on one relay: the events, and the authors it answered for in full. */
    private class AuthorsRead(
        val events: List<Event>,
        val covered: Set<HexKey>,
    )

    /**
     * [authors]' events of one kind on [relay], in chunks of [authorsPerReq] with `limit` set to
     * the chunk's size times [perAuthor] (the events one author may hold for the filter: one for a
     * replaceable kind, more for an addressable one, which an author holds one of per `d`), and
     * the authors the relay answered for in full.
     *
     * A relay answered for an author when it reached EOSE on a page that wasn't full, or, for a
     * replaceable kind ([perAuthor] 1), when it sent that author's event. A full page (as many
     * events as the limit, or [FULL_PAGE_FLOOR], the smallest page a relay is assumed to cut to
     * without saying so) may have left authors out: those are asked again in chunks half the size,
     * until the page isn't full. With several events an author, every author of a full page is
     * asked again, since one who sent a schedule may hold another the cut left out; one author
     * alone on a full page isn't answered for. A CLOSED (a REQ too large for the relay) is asked
     * again the same way, down to [minChunk]. A relay that doesn't answer (down, slow, cut off)
     * isn't asked again: it answered for nobody.
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
        val events = ArrayList<Event>()
        val covered = HashSet<HexKey>()
        var left = authors.distinct().sorted()
        var size = authorsPerReq
        var asked = 0
        val maxReqs = MAX_REQS_PER_RELAY * ((left.size + authorsPerReq - 1) / authorsPerReq).coerceAtLeast(1)
        while (left.isNotEmpty() && asked < maxReqs) {
            val again = ArrayList<HexKey>()
            for (chunk in left.chunked(size)) {
                if (asked++ >= maxReqs) break
                val wanted = chunk.toHashSet()
                val limit = chunk.size * perAuthor
                val read = fetch(setOf(relay), filterOf(chunk).copy(limit = limit))
                val mine = read.events.filter { it.pubKey in wanted }
                events += mine
                when {
                    relay in read.served -> {
                        val full = read.events.size >= minOf(limit, FULL_PAGE_FLOOR)
                        when {
                            !full -> {
                                covered += chunk
                            }

                            perAuthor == 1 -> {
                                // One event an author: who sent theirs is answered for, the rest asked again.
                                val sent = mine.mapTo(HashSet()) { it.pubKey }
                                covered += sent
                                if (chunk.size > 1) again += chunk.filter { it !in sent } else covered += chunk
                            }

                            chunk.size > 1 -> {
                                again += chunk
                            }

                            else -> {
                                Log.d(TAG) { "relay=${relay.url} holds $limit or more events for one author: not answered for" }
                            }
                        }
                    }

                    relay in read.refused && size > minChunk -> {
                        again += chunk
                    }
                }
            }
            if (again.isEmpty() || size <= 1) break
            left = again
            size = (size / 2).coerceAtLeast(1)
        }
        return AuthorsRead(events, covered)
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
            ownRelays[owner].orEmpty().filter { it !in index }.forEach { byRelay.getOrPut(it) { ArrayList() } += owner }
        }
        ask(byRelay)

        return scheduled.flatMap { owner ->
            val quorum = hasQuorum(ownRelays[owner].orEmpty(), servedBy[owner].orEmpty())
            schedulesOf(owner)
                .groupBy { it.dTag() }
                .values
                .mapNotNull { versions -> (newestOf(versions) as? ScheduledTaskEvent)?.let { ScheduleRead(it, quorum) } }
        }
    }

    /** More than half of [relays] (the index relays when there are none) are among [served]. */
    private fun hasQuorum(
        relays: Set<NormalizedRelayUrl>,
        served: Set<NormalizedRelayUrl>,
    ): Boolean {
        val asked = relays.ifEmpty { index }
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
         * others lower): its missing authors are asked again in smaller chunks ([byAuthors]).
         */
        const val FULL_PAGE_FLOOR = 100

        /** The smallest chunk a relay that CLOSEs a batch is asked again with. */
        const val MIN_CHUNK = 25

        /** The most REQs [byAuthors] sends one relay for each [AUTHORS_PER_REQ] authors of one batch read. */
        const val MAX_REQS_PER_RELAY = 64

        /** How many relays a batch read asks at once. */
        const val RELAY_PARALLELISM = 16

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
