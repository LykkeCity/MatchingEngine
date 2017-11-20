package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import org.apache.log4j.Logger
import org.nustaq.serialization.FSTConfiguration
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.HashMap

class FileWalletDatabaseAccessor(private val dir: String, private val delegateDatabaseAccessor: WalletDatabaseAccessor) : WalletDatabaseAccessor {

    companion object {
        private val LOGGER = Logger.getLogger(FileWalletDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    init {
        if (delegateDatabaseAccessor is FileWalletDatabaseAccessor) {
            throw IllegalArgumentException("delegateDatabaseAccessor must not be FileWalletDatabaseAccessor instance")
        }
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()

    override fun loadBalances(): HashMap<String, MutableMap<String, AssetBalance>> {
        val result = HashMap<String, MutableMap<String, AssetBalance>>()
        val wallets = loadWallets()
        var balancesCount = 0
        try {
            wallets.values.forEach { wallet ->
                val map = result.getOrPut(wallet.clientId) { HashMap() }
                wallet.balances.values.forEach { balance ->
                    map.put(balance.asset, AssetBalance(balance.asset, balance.balance, balance.reserved))
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
            val dir = File(dir)
            if (dir.exists()) {
                dir.listFiles().forEach { file ->
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
        wallet?.let {
            wallets.put(it.clientId, it)
        }
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        wallets.forEach { insertOrUpdateWallet(it) }
    }

    override fun insertOrUpdateWallet(wallet: Wallet) {
        try {
            val fileName = wallet.clientId
            archiveAndDeleteFile(fileName)
            saveFile(fileName, wallet)
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
            val newFile = FileSystems.getDefault().getPath("$dir/_prev_$fileName")
            val oldFile = FileSystems.getDefault().getPath("$dir/$fileName")
            Files.move(oldFile, newFile, StandardCopyOption.REPLACE_EXISTING)
        } catch (e: NoSuchFileException) {
            val message = "There are no file to archive and delete: ${e.message}"
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
            val file = File("$dir/$fileName")
            if (!file.exists()) {
                file.createNewFile()
            }
            val bytes = conf.asByteArray(wallet)
            Files.write(FileSystems.getDefault().getPath("$dir/$fileName"), bytes, StandardOpenOption.CREATE)
        } catch (e: Exception) {
            val message = "Unable to save, name: $fileName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }

    override fun insertExternalCashOperation(operation: ExternalCashOperation) {
        delegateDatabaseAccessor.insertExternalCashOperation(operation)
    }

    override fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation? {
        return delegateDatabaseAccessor.loadExternalCashOperation(clientId, operationId)
    }

    override fun insertOperation(operation: WalletOperation) {
        delegateDatabaseAccessor.insertOperation(operation)
    }

    override fun insertTransferOperation(operation: TransferOperation) {
        delegateDatabaseAccessor.insertTransferOperation(operation)
    }

    override fun insertSwapOperation(operation: SwapOperation) {
        delegateDatabaseAccessor.insertSwapOperation(operation)
    }

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        return delegateDatabaseAccessor.loadAssetPairs()
    }

    override fun loadAssetPair(assetId: String): AssetPair? {
        return delegateDatabaseAccessor.loadAssetPair(assetId)
    }

}