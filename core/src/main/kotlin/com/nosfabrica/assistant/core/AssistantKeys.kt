package com.nosfabrica.assistant.core

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip19Bech32.decodePrivateKeyAsHexOrNull
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.quartz.utils.Log
import java.sql.Connection

/** An owner and the Assistant brainstorm_server holds for them, as the view lists them: nothing decrypted. */
data class AssistantPair(
    val owner: HexKey,
    val assistant: HexKey,
)

/** Why an owner's Assistant key can't be had. Logged by name, with the owner. */
enum class KeyFailure {
    /** The view has no row for the owner: no Assistant (or it was removed). */
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

    companion object {
        const val PAGE = 500
    }
}

/**
 * brainstorm_server's `assistant_keys_v1` view (owner, assistant, encrypted_nsec, legacy_nsec)
 * and its Fernet key file. Listing pairs decrypts nothing; [key] reads one owner's row and opens
 * it as the server's `_plaintext_nsec` does: `encrypted_nsec` when it is there (a value that is
 * already an `nsec1` is taken as it is, as the server's `decrypt_nsec` does), `legacy_nsec` only
 * when it is null.
 *
 * Neither the nsec, the token nor the plaintext is ever logged, and no exception that could carry
 * one escapes: a failure is a [KeyFailure].
 */
class AssistantKeys(
    private val db: Db,
    private val keyFile: FernetKeyFile,
    private val view: String = VIEW,
) : AssistantDirectory {
    init {
        require(VIEW_NAME.matches(view)) { "not a view name" }
    }

    override suspend fun page(
        after: HexKey?,
        limit: Int,
    ): List<AssistantPair> =
        db.use { c ->
            // Two statements rather than `? IS NULL OR owner > ?`: Postgres can't type an untyped null parameter.
            val sql =
                if (after == null) {
                    "SELECT owner, assistant FROM $view ORDER BY owner LIMIT ?"
                } else {
                    "SELECT owner, assistant FROM $view WHERE owner > ? ORDER BY owner LIMIT ?"
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
                            // A row the server wrote wrong is skipped: it can't be matched to a schedule.
                            if (isPubKeyHex(owner) && isPubKeyHex(assistant)) add(AssistantPair(owner, assistant))
                        }
                    }
                }
            }
        }

    override suspend fun key(owner: HexKey): AssistantKey {
        val row = db.use { c -> row(c, owner) } ?: return failed(owner, KeyFailure.NO_ASSISTANT)
        val hex = plaintextOf(row)?.let { decodePrivateKeyAsHexOrNull(it.trim()) }?.takeIf { it.length == 64 }
        val keyPair = hex?.let(::keyPairOrNull) ?: return failed(owner, KeyFailure.NSEC_UNREADABLE)
        if (keyPair.pubKey.toHexKey() != row.assistant.lowercase()) return failed(owner, KeyFailure.NSEC_MISMATCH)
        return AssistantKey.Ready(owner, NostrSignerInternal(keyPair))
    }

    private fun failed(
        owner: HexKey,
        failure: KeyFailure,
    ): AssistantKey.Failed {
        Log.d(TAG) { "owner=$owner key=$failure" }
        return AssistantKey.Failed(owner, failure)
    }

    /** One owner's row. Strings can't be wiped, so it lives only as long as [key] runs. */
    private class Row(
        val assistant: String,
        val encrypted: String?,
        val legacy: String?,
    )

    private fun row(
        c: Connection,
        owner: HexKey,
    ): Row? =
        c.prepareStatement("SELECT assistant, encrypted_nsec, legacy_nsec FROM $view WHERE owner = ?").use { st ->
            st.setString(1, owner)
            st.executeQuery().use { rs -> if (rs.next()) Row(rs.getString(1), rs.getString(2), rs.getString(3)) else null }
        }

    /** The nsec's text, or null when nothing opens it. */
    private fun plaintextOf(row: Row): String? {
        val token = row.encrypted
        // The server's `if encrypted_nsec:` reads an empty token as none too.
        if (token.isNullOrEmpty()) return row.legacy
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

        /** brainstorm_server's view, by its versioned name. */
        const val VIEW = "assistant_keys_v1"

        private val VIEW_NAME = Regex("[a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)?")
    }
}
