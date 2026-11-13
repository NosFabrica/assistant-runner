package com.nosfabrica.assistant.runner

import com.nosfabrica.assistant.core.AssistantDirectory
import com.nosfabrica.assistant.core.AssistantKey
import com.nosfabrica.assistant.core.AssistantPair
import com.nosfabrica.assistant.core.KeyFailure
import com.nosfabrica.assistant.core.WakeRequest
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.utils.EventFactory

fun relay(url: String): NormalizedRelayUrl = RelayUrlNormalizer.normalizeOrNull(url)!!

/** Parsed again through Quartz's factory, as a relay would deliver it: the typed class. */
fun typed(event: Event): Event =
    EventFactory.create(event.id, event.pubKey, event.createdAt, event.kind, event.tags, event.content, event.sig)

suspend fun NostrSignerInternal.signed(
    kind: Int,
    tags: Array<Array<String>>,
    content: String = "",
    createdAt: Long,
): Event = typed(sign(EventTemplate<Event>(createdAt, kind, tags, content)))

fun newSigner() = NostrSignerInternal(KeyPair())

/**
 * Relays in memory: each answers a filter with what it holds, by Quartz's own matcher, newest
 * first and cut at the filter's `limit` (or at the relay's [defaultLimit] when it gives none), as a relay does.
 */
class FakeRelays : RelayAccess {
    val held = HashMap<NormalizedRelayUrl, MutableList<Event>>()
    val asked = mutableListOf<Pair<Set<NormalizedRelayUrl>, Filter>>()
    var down: Set<NormalizedRelayUrl> = emptySet()

    /** What a relay sends for a filter with no `limit`: many relays cut there without saying so. */
    var defaultLimit: (NormalizedRelayUrl) -> Int? = { null }

    /** Relays that answer a filter with a CLOSED instead (a REQ too large for them, say). */
    var refuses: (NormalizedRelayUrl, Filter) -> Boolean = { _, _ -> false }

    fun put(
        relay: NormalizedRelayUrl,
        vararg events: Event,
    ) {
        synchronized(held) { held.getOrPut(relay) { mutableListOf() }.addAll(events) }
    }

    override suspend fun fetch(
        relays: Set<NormalizedRelayUrl>,
        filter: Filter,
    ): RelayRead {
        synchronized(asked) { asked += relays to filter }
        val up = relays - down
        val refused = up.filter { refuses(it, filter) }.toSet()
        val answering = up - refused
        val events =
            answering.flatMap { relay ->
                val matching =
                    synchronized(
                        held,
                    ) { held[relay].orEmpty().toList() }.filter { filter.match(it) }.sortedByDescending { it.createdAt }
                (filter.limit ?: defaultLimit(relay))?.let { matching.take(it) } ?: matching
            }
        return RelayRead(events.distinctBy { it.id }, answering, refused)
    }
}

/** brainstorm_server's view and key file, faked: each owner's Assistant, or the failure its key gives. */
class FakeDirectory(
    val assistants: Map<HexKey, NostrSignerInternal>,
) : AssistantDirectory {
    /** A failure an owner's key gives instead of the key (a lost Fernet key, a mismatched row). */
    val failures = HashMap<HexKey, KeyFailure>()

    /** Owners whose key was asked for, in order: a key is asked for only to decrypt a new schedule. */
    val keysAsked = mutableListOf<HexKey>()

    override suspend fun page(
        after: HexKey?,
        limit: Int,
    ): List<AssistantPair> =
        assistants.entries
            .sortedBy { it.key }
            .filter { after == null || it.key > after }
            .take(limit)
            .map { AssistantPair(it.key, it.value.pubKey) }

    override suspend fun key(owner: HexKey): AssistantKey {
        synchronized(keysAsked) { keysAsked += owner }
        failures[owner]?.let { return AssistantKey.Failed(owner, it) }
        return assistants[owner]?.let { AssistantKey.Ready(owner, it) } ?: AssistantKey.Failed(owner, KeyFailure.NO_ASSISTANT)
    }
}

/** The task services, faked: records each wake and answers with [answer]. */
class FakeWaker(
    var answer: (WakeRequest) -> WakeAnswer = { WakeAnswer.Accepted },
) : TaskWaker {
    val wakes = mutableListOf<Pair<String, WakeRequest>>()

    override suspend fun wake(
        service: String,
        request: WakeRequest,
    ): WakeAnswer {
        synchronized(wakes) { wakes += service to request }
        return answer(request)
    }
}
