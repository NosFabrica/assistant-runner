package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * The trigger contract, v1 (PLAN.md, "The trigger contract"; `contract/v1/`): the runner's wake,
 * `POST {service}/v1/run` with `{"v": 1, "task", "owner", "occurrence"}`. Built only from valid
 * fields, so the runner never sends what a service would answer `400`. The service parses it with
 * code of its own: the two sides share the vectors, not this class.
 *
 * [occurrence] is a hint: the service decides the occurrence itself from the schedule it reads.
 */
class WakeRequest(
    val task: String,
    val owner: HexKey,
    val occurrence: Long,
) {
    init {
        require(isTaskName(task)) { TASK_RULE }
        require(isPubKeyHex(owner)) { OWNER_RULE }
        require(occurrence >= 0) { OCCURRENCE_RULE }
    }

    fun toJson(): JsonObject =
        buildJsonObject {
            put("v", VERSION)
            put("task", task)
            put("owner", owner)
            put("occurrence", occurrence)
        }

    override fun toString() = toJson().toString()

    override fun equals(other: Any?) = other is WakeRequest && other.task == task && other.owner == owner && other.occurrence == occurrence

    override fun hashCode() = (task.hashCode() * 31 + owner.hashCode()) * 31 + occurrence.hashCode()

    companion object {
        const val VERSION = 1

        /** The path a service serves the wake on, under its base URL. */
        const val PATH = "/v1/run"

        // The contract's rule names, as the `wake-invalid-*` vectors' `why` spells them.
        const val TASK_RULE = "task must be a task name"
        const val OWNER_RULE = "owner must be 64 lowercase hex"
        const val OCCURRENCE_RULE = "occurrence must be unix seconds"

        private val TASK_NAME = Regex("[a-z0-9][a-z0-9_-]{0,63}")

        /** A task's name as the contract carries it: lowercase letters, digits, `_` and `-`, at most 64. */
        fun isTaskName(s: String?) = s != null && TASK_NAME.matches(s)
    }
}

/**
 * A service's `200` body: settled, nothing to do, with its own outcome name (`NOT_DUE`,
 * `ATTEMPTED`, `NOT_NAMED`, …). The name is the service's enum, so it is safe to log; anything
 * that isn't shaped like one is no outcome.
 */
object SettledAnswer {
    private val OUTCOME = Regex("[A-Z][A-Z0-9_]{0,63}")

    /** The outcome a `200` body names, or null when the body isn't an answer. */
    fun outcomeOf(body: String): String? {
        val json =
            try {
                Json.parseToJsonElement(body) as? JsonObject
            } catch (e: SerializationException) {
                null
            } catch (e: IllegalArgumentException) {
                null
            }
        return (json?.get("outcome") as? JsonPrimitive)
            ?.takeIf { it.isString }
            ?.content
            ?.takeIf(OUTCOME::matches)
    }
}
