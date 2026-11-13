package com.nosfabrica.assistant.runner

import com.vitorpamplona.quartz.utils.Log
import java.io.IOException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.FileTime

/**
 * brainstorm_server's Fernet key file (`/run/secrets/nsec_encryption_keys`): comma-separated
 * keys, tried in order ([MultiFernet]). The server writes and rotates it; every reader mounts it.
 *
 * It is read again when its modification time changes, and once more, whatever the time says,
 * when no key opens a token: a rotation writes `[new, old]` and then re-encrypts, and a rewrite
 * within the file system's time granularity would otherwise go unseen. A missing file is no keys:
 * every token is unreadable until it appears.
 */
class FernetKeyFile(
    val path: Path = Paths.get(DEFAULT_PATH),
    private val nowMs: () -> Long = { System.nanoTime() / 1_000_000 },
) {
    private var keys = MultiFernet(emptyList())
    private var loadedAt: FileTime? = null
    private var loaded = false
    private var forcedAt: Long? = null

    /** How many times the file was read: for tests. */
    var reads = 0
        private set

    /**
     * [token]'s plaintext, or null when no key opens it, even after reading the file again. The
     * file is read again for a token no key opens at most once a second ([FORCED_RELOAD_MS]):
     * a poll full of tokens a lost key made would otherwise read it once per owner.
     */
    @Synchronized
    fun decrypt(token: String): ByteArray? {
        val modified = modifiedTime()
        if (!loaded || modified != loadedAt) reload(modified)
        keys.decrypt(token)?.let { return it }
        val at = nowMs()
        if (forcedAt?.let { at - it < FORCED_RELOAD_MS } == true) return null
        forcedAt = at
        reload(modifiedTime())
        return keys.decrypt(token)
    }

    /** The file's modification time as last seen: what a failure to open a token is remembered by. */
    @Synchronized
    fun version(): FileTime? = modifiedTime()

    /** How many keys the file held when last read: for the startup log, never the keys. */
    @Synchronized
    fun keyCount(): Int {
        if (!loaded) reload(modifiedTime())
        return keys.keys.size
    }

    private fun modifiedTime(): FileTime? =
        try {
            Files.getLastModifiedTime(path)
        } catch (e: IOException) {
            null
        }

    private fun reload(modified: FileTime?) {
        val text =
            try {
                Files.readString(path)
            } catch (e: NoSuchFileException) {
                ""
            } catch (e: IOException) {
                // The class only: the path is ours, but an IO message is no place to look for a key.
                Log.w(TAG) { "could not read the key file: ${e.javaClass.simpleName}" }
                ""
            }
        reads++
        val retired = keys
        keys = MultiFernet.parse(text) { i -> Log.w(TAG) { "key ${i + 1} in the key file is not a Fernet key; skipped" } }
        // The keys read before are zeroed: the new ones are their own copies.
        retired.keys.forEach { it.wipe() }
        loadedAt = modified
        loaded = true
    }

    override fun toString() = "FernetKeyFile($path)"

    companion object {
        private const val TAG = "FernetKeyFile"

        /** The least time between two reads of the file forced by a token no key opens. */
        const val FORCED_RELOAD_MS = 1_000L

        /** brainstorm_server's `KEY_FILE_PATH`. */
        const val DEFAULT_PATH = "/run/secrets/nsec_encryption_keys"
    }
}
