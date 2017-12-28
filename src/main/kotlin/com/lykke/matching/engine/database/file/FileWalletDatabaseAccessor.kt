package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.outgoing.database.WalletsSaveService
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.nustaq.serialization.FSTConfiguration
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.HashMap
import java.util.concurrent.LinkedBlockingQueue

class FileWalletDatabaseAccessor(private val walletsDirectory: String,
                                 /** accessor to save wallets to another db async; null, if don't need it */
                                 walletDatabaseAccessor: WalletDatabaseAccessor? = null) : WalletDatabaseAccessor {

    companion object {
        private val LOGGER = Logger.getLogger(FileWalletDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()
    private val saveToAnotherDb = walletDatabaseAccessor != null
    private val updatedWalletsQueue = LinkedBlockingQueue<List<Wallet>>()

    init {
        if (saveToAnotherDb) {
            WalletsSaveService(walletDatabaseAccessor!!, updatedWalletsQueue).start()
            // synchronize wallets with another db
            updatedWalletsQueue.put(loadWallets().values.toList())
        }
    }

    override fun loadBalances(): HashMap<String, MutableMap<String, AssetBalance>> {
        val result = HashMap<String, MutableMap<String, AssetBalance>>()
        val wallets = loadWallets()
        var balancesCount = 0
        try {
            wallets.values.forEach { wallet ->
                val map = result.getOrPut(wallet.clientId) { HashMap() }
                wallet.balances.values.forEach { balance ->
                    map.put(balance.asset, AssetBalance(balance.asset, balance.timestamp, balance.balance, balance.reserved))
                    balancesCount++
                }
            }
        } catch (e: Exception) {
            val message = "Unable to load balances"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
        LOGGER.info("Loaded $balancesCount balances for ${result.size} clients")
        return result
    }

    override fun loadWallets(): HashMap<String, Wallet> {
        val result = HashMap<String, Wallet>()
        try {
            val dir = File(walletsDirectory)
            if (dir.exists()) {
                dir.listFiles().filter { it.isFile }.forEach { file ->
                    if (!file.name.startsWith("_prev_")) {
                        try {
                            readAndAddWallet(file, result)
                        } catch (e: Exception) {
                            LOGGER.error("Unable to read wallet file ${file.name}. Trying to load previous one", e)
                            try {
                                readAndAddWallet(File("$dir/_prev_${file.name}"), result)
                            } catch (e: Exception) {
                                LOGGER.error("Unable to read previous wallet file ${file.name}.", e)
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            val message = "Unable to load wallets"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
        LOGGER.info("Loaded ${result.size} wallets")
        return result
    }

    private fun readAndAddWallet(file: File, wallets: MutableMap<String, Wallet>) {
        val wallet = loadFile(file)
        if (wallet != null) {
            wallets.put(wallet.clientId, wallet)
        } else {
            LOGGER.error("File '${file.name}' has invalid data format")
        }
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        if (saveToAnotherDb) {
            updatedWalletsQueue.put(wallets)
        }
        wallets.forEach { insertOrUpdateWallet(it, false) }
    }

    override fun insertOrUpdateWallet(wallet: Wallet) {
        insertOrUpdateWallet(wallet, saveToAnotherDb)
    }

    private fun insertOrUpdateWallet(wallet: Wallet, saveToAnotherDb: Boolean) {
        try {
            val fileName = wallet.clientId
            archiveAndDeleteFile(fileName)
            saveFile(fileName, wallet)
            if (saveToAnotherDb) {
                updatedWalletsQueue.put(listOf(wallet))
            }
        } catch (e: Exception) {
            val message = "Unable to save wallet, clientId: ${wallet.clientId}"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }

    private fun loadFile(file: File): Wallet? {
        val fileLocation = file.toPath()
        val bytes = Files.readAllBytes(fileLocation)
        val readCase = conf.asObject(bytes)
        return readCase as? Wallet
    }

    private fun archiveAndDeleteFile(fileName: String) {
        try {
            val newFile = FileSystems.getDefault().getPath("$walletsDirectory/_prev_$fileName")
            val oldFile = FileSystems.getDefault().getPath("$walletsDirectory/$fileName")
            Files.move(oldFile, newFile, StandardCopyOption.REPLACE_EXISTING)
        } catch (e: NoSuchFileException) {
            val message = "There is no file to archive and delete: ${e.message}"
            LOGGER.error(message)
        } catch (e: Exception) {
            val message = "Unable to archive and delete, name: $fileName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }

    private fun saveFile(fileName: String, wallet: Wallet) {
        try {
            wallet.balances.values.removeIf { it.balance == 0.0 }
            val file = File("$walletsDirectory/$fileName")
            if (!file.exists()) {
                file.createNewFile()
            }
            val bytes = conf.asByteArray(wallet)
            Files.write(FileSystems.getDefault().getPath("$walletsDirectory/$fileName"), bytes, StandardOpenOption.CREATE)
        } catch (e: Exception) {
            val message = "Unable to save, name: $fileName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }
}