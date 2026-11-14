package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip19Bech32.decodePrivateKeyAsHexOrNull
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.quartz.utils.Log
import java.nio.file.attribute.FileTime
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

/** An owner and the Assistant brainstorm_server holds for them, as its table lists them: nothing decrypted. */
data class AssistantPair(
    val owner: HexKey,
    val assistant: HexKey,
)

/** Why an owner's Assistant key can't be had. Logged by name, with the owner. */
enum class KeyFailure {
    /** `brainstorm_nsec` has no row for the owner, or one with no `assistant_pubkey`: no Assistant (or it was removed). */
    NO_ASSISTANT,

    /** No key in the key file opens the token, even after reading it again, or the plaintext isn't an nsec. */
    NSEC_UNREADABLE,

    /** The nsec opens, but its public key isn't the stored `assistant`: a row that doesn't say what it holds. */
    NSEC_MISMATCH,
}

/** An owner's Assistant key, or why there is none. [toString] never shows the key. */
sealed interface AssistantKey {
    class Ready(
        val owner: HexKey,
        val signer: NostrSignerInternal,
    ) : AssistantKey {
        override fun toString() = "AssistantKey.Ready(owner=$owner, assistant=${signer.pubKey})"
    }

    class Failed(
        val owner: HexKey,
        val failure: KeyFailure,
    ) : AssistantKey {
        override fun toString() = "AssistantKey.Failed(owner=$owner, $failure)"
    }
}

/** Where the owners, their Assistants and the Assistants' keys come from. [AssistantKeys] reads Postgres; tests fake it. */
interface AssistantDirectory {
    /** Up to [limit] pairs with an owner after [after] (all when null), by owner. */
    suspend fun page(
        after: HexKey?,
        limit: Int = PAGE,
    ): List<AssistantPair>

    /** Every pair, page by page. */
    suspend fun pairs(): List<AssistantPair> {
        val all = ArrayList<AssistantPair>()
        var after: HexKey? = null
        while (true) {
            val page = page(after, PAGE)
            all += page
            if (page.size < PAGE) return all
            after = page.last().owner
        }
    }

    /** [owner]'s Assistant key, decrypted now; held by the caller in memory only. */
    suspend fun key(owner: HexKey): AssistantKey

    /** Each of [owners]' keys, as [key] gives them: a poll asks once for every owner it decrypts for. */
    suspend fun keys(owners: Collection<HexKey>): Map<HexKey, AssistantKey> = owners.distinct().associateWith { key(it) }

    companion object {
        const val PAGE = 500
    }
}

/**
 * brainstorm_server's `brainstorm_nsec` table, read as its own repo (`app/repos/brainstorm_nsec.py`)
 * reads it, and its Fernet key file. Only four columns ([COLUMNS]), the ones ops grants the role:
 * the runner is coupled to them, so a server migration that renames or drops one breaks it. The
 * task services read the same columns with code of their own: nothing here is shared with them.
 *
 * - [page] is the server's `select_owner_assistant_pairs_on_db`: `pubkey` and `assistant_pubkey`
 *   where an Assistant is set, by `pubkey`; nothing is decrypted.
 * - [key] reads one owner's `assistant_pubkey`, `encrypted_nsec` and `nsec`, and opens them as the
 *   server's `_plaintext_nsec` does: `encrypted_nsec` when it is non-empty (a value that is
 *   already an `nsec1` is taken as it is, as `decrypt_nsec` does), `nsec` only otherwise. A stale
 *   plaintext `nsec` beside a token is never used: the token wins.
 *
 * Neither the nsec, the token nor the plaintext is ever logged, and no exception that could carry
 * one escapes: a failure is a [KeyFailure].
 */
class AssistantKeys(
    private val db: Db,
    private val keyFile: FernetKeyFile,
    private val table: String = TABLE,
    private val nowMs: () -> Long = { System.nanoTime() / 1_000_000 },
) : AssistantDirectory {
    init {
        require(TABLE_NAME.matches(table)) { "not a table name" }
    }

    override suspend fun page(
        after: HexKey?,
        limit: Int,
    ): List<AssistantPair> =
        db.use { c ->
            // Two statements rather than `? IS NULL OR pubkey > ?`: Postgres can't type an untyped null parameter.
            // A row the server wrote wrong is left out by the query, not after it: a page short of
            // LIMIT is what ends [pairs], so one bad row filtered here would drop every owner after it.
            val sql =
                if (after == null) {
                    "SELECT pubkey, assistant_pubkey FROM $table WHERE $VALID ORDER BY pubkey LIMIT ?"
                } else {
                    "SELECT pubkey, assistant_pubkey FROM $table WHERE $VALID AND pubkey > ? ORDER BY pubkey LIMIT ?"
                }
            c.prepareStatement(sql).use { st ->
                var i = 1
                if (after != null) st.setString(i++, after)
                st.setInt(i, limit)
                st.executeQuery().use { rs ->
                    buildList {
                        while (rs.next()) {
                            val owner = rs.getString(1)
                            val assistant = rs.getString(2)
                            // [VALID] already says so; checked again so a pair is never built from anything else.
                            if (isPubKeyHex(owner) && isPubKeyHex(assistant)) add(AssistantPair(owner, assistant))
                        }
                    }
                }
            }
        }

    override suspend fun key(owner: HexKey): AssistantKey = keys(listOf(owner)).getValue(owner)

    /**
     * Each of [owners]' keys, their rows read in one query on one connection. A key that didn't
     * open (`NSEC_UNREADABLE`, `NSEC_MISMATCH`) is remembered, and neither read nor tried again,
     * while the key file's modification time is the same and for at most [FAILURE_TTL_MS]: a
     * rotation, or an hour, is a new chance.
     */
    override suspend fun keys(owners: Collection<HexKey>): Map<HexKey, AssistantKey> {
        val wanted = owners.distinct()
        val out = HashMap<HexKey, AssistantKey>()
        val version = keyFile.version()
        val at = nowMs()
        val toRead =
            wanted.filter { owner ->
                val known = failures[owner]?.takeIf { it.version == version && at - it.at < FAILURE_TTL_MS }
                if (known != null) out[owner] = AssistantKey.Failed(owner, known.failure)
                known == null
            }
        if (toRead.isEmpty()) return out
        val rows = db.use { c -> rows(c, toRead) }
        for (owner in toRead) {
            val key = open(owner, rows[owner])
            out[owner] = key
            if (key is AssistantKey.Failed && key.failure != KeyFailure.NO_ASSISTANT) {
                failures[owner] = KnownFailure(key.failure, version, at)
            } else {
                failures.remove(owner)
            }
        }
        failures.entries.removeIf { at - it.value.at >= FAILURE_TTL_MS }
        return out
    }

    private fun open(
        owner: HexKey,
        row: Row?,
    ): AssistantKey {
        if (row == null) return failed(owner, KeyFailure.NO_ASSISTANT)
        // The server's `if assistant_pubkey:`: a blank one is no Assistant, not a mismatch.
        val assistant = row.assistant?.takeIf { it.isNotBlank() } ?: return failed(owner, KeyFailure.NO_ASSISTANT)
        val hex = plaintextOf(row)?.let { decodePrivateKeyAsHexOrNull(it.trim()) }?.takeIf { it.length == 64 }
        val keyPair = hex?.let(::keyPairOrNull) ?: return failed(owner, KeyFailure.NSEC_UNREADABLE)
        if (keyPair.pubKey.toHexKey() != assistant.lowercase()) {
            keyPair.privKey?.fill(0)
            return failed(owner, KeyFailure.NSEC_MISMATCH)
        }
        return AssistantKey.Ready(owner, NostrSignerInternal(keyPair))
    }

    private class KnownFailure(
        val failure: KeyFailure,
        val version: FileTime?,
        val at: Long,
    )

    private val failures = ConcurrentHashMap<HexKey, KnownFailure>()

    private fun failed(
        owner: HexKey,
        failure: KeyFailure,
    ): AssistantKey.Failed {
        Log.d(TAG) { "owner=$owner key=$failure" }
        return AssistantKey.Failed(owner, failure)
    }

    /** One owner's row. Strings can't be wiped, so it lives only as long as [keys] runs. */
    private class Row(
        val assistant: String?,
        val encrypted: String?,
        val nsec: String?,
    )

    private fun rows(
        c: Connection,
        owners: List<HexKey>,
    ): Map<HexKey, Row> =
        c.prepareStatement("SELECT pubkey, assistant_pubkey, encrypted_nsec, nsec FROM $table WHERE pubkey = ANY(?)").use { st ->
            st.setArray(1, c.createArrayOf("varchar", owners.toTypedArray()))
            st.executeQuery().use { rs ->
                buildMap { while (rs.next()) put(rs.getString(1), Row(rs.getString(2), rs.getString(3), rs.getString(4))) }
            }
        }

    /** The nsec's text, or null when nothing opens it: the server's `_plaintext_nsec`. */
    private fun plaintextOf(row: Row): String? {
        val token = row.encrypted
        // The server's `if encrypted_nsec:` reads an empty token as none too.
        if (token.isNullOrEmpty()) return row.nsec
        // The server's decrypt_nsec: a value that was never encrypted is used as it is.
        if (token.startsWith("nsec1")) return token
        val bytes = keyFile.decrypt(token) ?: return null
        return try {
            bytes.decodeToString()
        } finally {
            bytes.fill(0)
        }
    }

    private fun keyPairOrNull(hex: String): KeyPair? =
        try {
            val priv = Hex.decode(hex)
            try {
                KeyPair(priv.copyOf())
            } finally {
                priv.fill(0)
            }
        } catch (e: Exception) {
            // Zero, or past the curve's order: not a key. The exception is dropped, not logged.
            null
        }

    companion object {
        private const val TAG = "AssistantKeys"

        /** The longest a key that didn't open is remembered without a rotation: a row the server fixed is read again within it. */
        const val FAILURE_TTL_MS = 3_600_000L

        /** brainstorm_server's table (its `BrainstormNsec` model). */
        const val TABLE = "brainstorm_nsec"

        /** The only columns read, and the only ones ops grants the role (PLAN.md, "Roles and grants"). */
        val COLUMNS = listOf("pubkey", "assistant_pubkey", "encrypted_nsec", "nsec")

        private val TABLE_NAME = Regex("[a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)?")

        /** A pair that can be matched to a schedule: both keys 64 lowercase hex ([isPubKeyHex]); a null fails the match. */
        private const val VALID = "pubkey ~ '^[0-9a-f]{64}$' AND assistant_pubkey ~ '^[0-9a-f]{64}$'"
    }
}
