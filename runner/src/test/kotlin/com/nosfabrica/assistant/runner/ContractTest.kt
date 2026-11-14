package com.nosfabrica.assistant.runner

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * contract/v1/vectors, read where they live: the runner writes every wake vector exactly, can't
 * write an invalid one, and reads every answer. The service parses them with code of its own.
 */
class ContractTest {
    private val dir = File(System.getProperty("contract.vectors") ?: "../contract/v1/vectors")
    private val vectors = dir.listFiles { f -> f.name.endsWith(".json") }!!.sortedBy { it.name }

    private fun json(f: File): JsonObject = Json.parseToJsonElement(f.readText()).jsonObject

    private fun JsonObject.str(key: String) = getValue(key).jsonPrimitive.content

    @Test
    fun everyVectorIsCovered() {
        assertTrue(vectors.isNotEmpty(), "no vectors in $dir")
        vectors.forEach { assertTrue(it.name.startsWith("wake-") || it.name.startsWith("answer-"), it.name) }
    }

    @Test
    fun aWakeIsWrittenAsTheVector() {
        val wakes = vectors.filter { it.name.startsWith("wake-") && !it.name.startsWith("wake-invalid-") }
        assertTrue(wakes.isNotEmpty())
        for (f in wakes) {
            val v = json(f)
            val wake = WakeRequest(v.str("task"), v.str("owner"), v.getValue("occurrence").jsonPrimitive.long)
            assertEquals(v, wake.toJson(), f.name)
        }
    }

    @Test
    fun anInvalidWakeCantBeWritten() {
        val invalid = vectors.filter { it.name.startsWith("wake-invalid-") }
        assertTrue(invalid.isNotEmpty())
        for (f in invalid) {
            val v = json(f)
            if (v.getValue("v").jsonPrimitive.int != WakeRequest.VERSION) {
                // The runner writes only its own version.
                assertEquals("unknown version", v.str("why"), f.name)
                continue
            }
            val refused =
                assertFailsWith<IllegalArgumentException>(f.name) {
                    WakeRequest(v.str("task"), v.str("owner"), v.getValue("occurrence").jsonPrimitive.long)
                }
            assertEquals(v.str("why"), refused.message, f.name)
        }
    }

    @Test
    fun anAnswerIsRead() {
        val answers = vectors.filter { it.name.startsWith("answer-") }
        assertTrue(answers.isNotEmpty())
        for (f in answers) assertEquals(json(f).str("outcome"), SettledAnswer.outcomeOf(f.readText()), f.name)
    }

    @Test
    fun whatIsNotAnAnswerOrAWakeIsRefused() {
        val owner = "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
        assertNull(SettledAnswer.outcomeOf("""{"outcome":"not an enum, maybe an echo"}"""))
        assertNull(SettledAnswer.outcomeOf("""{"outcome":1}"""))
        assertNull(SettledAnswer.outcomeOf("[]"))
        assertNull(SettledAnswer.outcomeOf("<html>"))
        assertEquals(WakeRequest.TASK_RULE, assertFailsWith<IllegalArgumentException> { WakeRequest("Observer", owner, 1) }.message)
        assertEquals(
            WakeRequest.OWNER_RULE,
            assertFailsWith<IllegalArgumentException> { WakeRequest("observer", owner.uppercase(), 1) }.message,
        )
        assertEquals(WakeRequest.OCCURRENCE_RULE, assertFailsWith<IllegalArgumentException> { WakeRequest("observer", owner, -1) }.message)
        assertNotEquals(WakeRequest("observer", owner, 1), WakeRequest("observer", owner, 2))
    }
}
