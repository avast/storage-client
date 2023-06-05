package com.avast.clients.storage.gcs

import better.files.File
import cats.effect.Blocker
import com.avast.clients.storage.gcs.TestImplicits.{randomString, StringOps}
import com.avast.clients.storage.{GetResult, HeadResult}
import com.avast.scala.hashes.Sha256
import com.google.cloud.storage.{Blob, Bucket}
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

@RunWith(classOf[JUnitRunner])
class GcsStorageBackendTest extends FunSuite with ScalaFutures with MockitoSugar {
  test("head") {
    val fileSize = 1001100
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()

    val blob = mock[Blob]
    when(blob.getSize).thenReturn(fileSize.toLong)

    val bucket = mock[Bucket]
    when(bucket.get(any[String]())).thenAnswer { call =>
      val blobPath = call.getArgument[String](0)
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

    val result = composeTestBackend(bucket).head(sha).runSyncUnsafe(10.seconds)

    assertResult(Right(HeadResult.Exists(fileSize)))(result)
  }

  test("get") {
    val fileSize = 1001200
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()

    val blob = mock[Blob]
    when(blob.getSize).thenReturn(fileSize.toLong)
    when(blob.downloadTo(any[OutputStream]())).thenAnswer { call =>
      val outputStream = call.getArgument[OutputStream](0)
      outputStream.write(content.getBytes())
    }

    val bucket = mock[Bucket]
    when(bucket.get(any[String]())).thenAnswer { call =>
      val blobPath = call.getArgument[String](0)
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
      val result = composeTestBackend(bucket).get(sha, file).runSyncUnsafe(10.seconds)
      assertResult(Right(GetResult.Downloaded(file, fileSize)))(result)
      assertResult(sha.toString.toLowerCase)(file.sha256.toLowerCase)
      assertResult(fileSize)(file.size)
    }
  }

  test("composeObjectPath") {
    val sha = Sha256("d05af9a8494696906e8eec79843ca1e4bf408c280616a121ed92f9e92e2de831")
    assertResult("d0/5a/f9/d05af9a8494696906e8eec79843ca1e4bf408c280616a121ed92f9e92e2de831")(GcsStorageBackend.composeBlobPath(sha))
  }

  private def composeTestBackend(bucket: Bucket): GcsStorageBackend[Task] = {
    val blocker = Blocker.liftExecutionContext(monix.execution.Scheduler.io())
    new GcsStorageBackend[Task](bucket)(blocker)
  }
}
