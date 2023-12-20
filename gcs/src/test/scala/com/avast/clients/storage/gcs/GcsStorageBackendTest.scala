package com.avast.clients.storage.gcs

import better.files.File
import cats.effect.Blocker
import com.avast.clients.storage.gcs.TestImplicits._
import com.avast.clients.storage.{GetResult, HeadResult}
import com.avast.scala.hashes.Sha256
import com.github.luben.zstd.Zstd
import com.google.cloud.storage.{Blob, BlobId, Storage}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar

import java.io.OutputStream
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.MapHasAsJava

@RunWith(classOf[JUnitRunner])
class GcsStorageBackendTest extends FunSuite with ScalaFutures with MockitoSugar {
  test("head") {
    val fileSize = 1001100
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()
    val bucketName = "bucket-tst"

    val blob = mock[Blob]
    when(blob.getSize).thenReturn(fileSize.toLong)
    when(blob.getMetadata).thenReturn(null)

    val storageClient = mock[Storage]
    when(storageClient.get(any[BlobId]())).thenAnswer { call =>
      val blobId = call.getArgument[BlobId](0)
      val blobPath = blobId.getName
      assertResult(bucketName)(blobId.getBucket)
      assertResult {
        List(
          shaStr.substring(0, 2),
          shaStr.substring(2, 4),
          shaStr.substring(4, 6),
          shaStr,
        )
      }(blobPath.split("/").toList)
      blob
    }

    val result = composeTestBackend(storageClient, bucketName).head(sha).runSyncUnsafe(10.seconds)

    assertResult(Right(HeadResult.Exists(fileSize)))(result)
  }

  test("head-zstd") {
    val fileSize = 1001100
    val originalContent = randomString(fileSize).getBytes()
    val compressedContent = Zstd.compress(originalContent, 9)
    val sha = originalContent.sha256
    val shaStr = sha.toString()
    val bucketName = "bucket-tst"

    val blob = mock[Blob]
    when(blob.getSize).thenReturn(compressedContent.length.toLong)
    when(blob.getMetadata).thenReturn {
      Map(
        GcsStorageBackend.CompressionTypeHeader -> "zstd",
        GcsStorageBackend.OriginalSizeHeader -> originalContent.length.toString
      ).asJava
    }

    val storageClient = mock[Storage]
    when(storageClient.get(any[BlobId]())).thenAnswer { call =>
      val blobId = call.getArgument[BlobId](0)
      val blobPath = blobId.getName
      assertResult(bucketName)(blobId.getBucket)
      assertResult {
        List(
          shaStr.substring(0, 2),
          shaStr.substring(2, 4),
          shaStr.substring(4, 6),
          shaStr,
        )
      }(blobPath.split("/").toList)
      blob
    }

    val result = composeTestBackend(storageClient, bucketName).head(sha).runSyncUnsafe(10.seconds)

    assertResult(Right(HeadResult.Exists(fileSize)))(result)
  }

  test("get") {
    val fileSize = 1001200
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()
    val bucketName = "bucket-tst"

    val blob = mock[Blob]
    when(blob.getMetadata).thenReturn(null)
    when(blob.downloadTo(any[OutputStream]())).thenAnswer { call =>
      val outputStream = call.getArgument[OutputStream](0)
      outputStream.write(content.getBytes())
    }

    val storageClient = mock[Storage]
    when(storageClient.get(any[BlobId]())).thenAnswer { call =>
      val blobId = call.getArgument[BlobId](0)
      val blobPath = blobId.getName
      assertResult(bucketName)(blobId.getBucket)
      assertResult {
        List(
          shaStr.substring(0, 2),
          shaStr.substring(2, 4),
          shaStr.substring(4, 6),
          shaStr,
        )
      }(blobPath.split("/").toList)
      blob
    }

    File.usingTemporaryFile() { file =>
      val result = composeTestBackend(storageClient, bucketName).get(sha, file).runSyncUnsafe(10.seconds)
      assertResult(Right(GetResult.Downloaded(file, fileSize)))(result)
      assertResult(sha.toString.toLowerCase)(file.sha256.toLowerCase)
      assertResult(fileSize)(file.size)
    }
  }

  test("get-zstd") {
    val fileSize = 1024 * 1024
    val originalContent = randomString(fileSize).getBytes()
    val compressedContent = Zstd.compress(originalContent, 9)
    val sha = originalContent.sha256
    val shaStr = sha.toString()
    val bucketName = "bucket-tst"

    val blob = mock[Blob]
    when(blob.getMetadata).thenReturn {
      Map(GcsStorageBackend.CompressionTypeHeader -> "zstd").asJava
    }
    when(blob.downloadTo(any[OutputStream]())).thenAnswer { call =>
      val outputStream = call.getArgument[OutputStream](0)
      outputStream.write(compressedContent)
    }

    val storageClient = mock[Storage]
    when(storageClient.get(any[BlobId]())).thenAnswer { call =>
      val blobId = call.getArgument[BlobId](0)
      val blobPath = blobId.getName
      assertResult(bucketName)(blobId.getBucket)
      assertResult {
        List(
          shaStr.substring(0, 2),
          shaStr.substring(2, 4),
          shaStr.substring(4, 6),
          shaStr,
        )
      }(blobPath.split("/").toList)
      blob
    }

    File.usingTemporaryFile() { file =>
      val result = composeTestBackend(storageClient, bucketName).get(sha, file).runSyncUnsafe(10.seconds)
      assertResult(Right(GetResult.Downloaded(file, fileSize)))(result)
      assertResult(sha.toString.toLowerCase)(file.sha256.toLowerCase)
      assertResult(fileSize)(file.size)
    }
  }

  test("composeObjectPath") {
    val sha = Sha256("d05af9a8494696906e8eec79843ca1e4bf408c280616a121ed92f9e92e2de831")
    assertResult("d0/5a/f9/d05af9a8494696906e8eec79843ca1e4bf408c280616a121ed92f9e92e2de831")(GcsStorageBackend.composeBlobPath(sha))
  }

  private def composeTestBackend(storageClient: Storage, bucketName: String): GcsStorageBackend[Task] = {
    val blocker = Blocker.liftExecutionContext(monix.execution.Scheduler.io())
    new GcsStorageBackend[Task](storageClient, bucketName)(blocker)
  }
}
