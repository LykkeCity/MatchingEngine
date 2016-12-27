package com.lykke.matching.engine.utils.config

import com.google.gson.FieldNamingPolicy.UPPER_CAMEL_CASE
import com.google.gson.GsonBuilder
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlob
import java.io.ByteArrayOutputStream
import javax.naming.ConfigurationException

class AzureConfigParser {
    companion object {
        fun initConfig(storageConnectionString: String, blobContainer: String, fileName: String): AzureConfig {
            val storageAccount = CloudStorageAccount.parse(storageConnectionString)
            val blobClient = storageAccount.createCloudBlobClient()
            val container = blobClient.getContainerReference(blobContainer)
            container.listBlobs().forEach { blobItem ->
                // If the item is a blob, not a virtual directory.
                if (blobItem is CloudBlob && blobItem.name == fileName) {
                    val stream = ByteArrayOutputStream()
                    blobItem.download(stream)
                    val gson = GsonBuilder().setFieldNamingPolicy(UPPER_CAMEL_CASE).create()
                    return gson.fromJson(stream.toString(), AzureConfig::class.java)
                }
            }

            throw ConfigurationException()
        }
    }
}