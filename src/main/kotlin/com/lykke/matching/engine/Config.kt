package com.lykke.matching.engine

import com.google.gson.Gson
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlob
import java.io.ByteArrayOutputStream
import java.io.FileInputStream
import java.util.HashMap
import java.util.Properties

fun loadLocalConfig(path: String): Properties {
    val props = Properties()
    props.load(FileInputStream(path))
    return props
}

fun Properties.getInt(key: String): Int? {
    return this.getProperty(key)?.toInt()
}

fun loadConfig(config: Properties): HashMap<String, Any> {
    val map = HashMap<String, Any>()
    val storageConnectionString =
            "DefaultEndpointsProtocol=${config.getProperty("azure.default.endpoints.protocol")};" +
                    "AccountName=${config.getProperty("azure.account.name")};" +
                    "AccountKey=${config.getProperty("azure.account.key")}"

    val storageAccount = CloudStorageAccount.parse(storageConnectionString)
    val blobClient = storageAccount.createCloudBlobClient()
    val container = blobClient.getContainerReference(config.getProperty("azure.blob.container"))

    container.listBlobs().forEach { blobItem ->
        // If the item is a blob, not a virtual directory.
        if (blobItem is CloudBlob && blobItem.name == config.getProperty("azure.blob.config.fileName")) {
            val stream = ByteArrayOutputStream()
            blobItem.download(stream)
            map.putAll(Gson().fromJson(stream.toString(), Map::class.java) as Map<String, Any>)
        }
    }

    return map
}