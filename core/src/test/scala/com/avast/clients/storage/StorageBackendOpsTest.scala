package com.avast.clients.storage

import better.files.File
import com.avast.clients.storage.StorageException.InvalidResponseException
import com.avast.clients.storage.TestImplicits._
import com.avast.scala.hashes.Sha256
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StorageBackendOpsTest extends FunSuite with ScalaFutures {
  test("withFallbackIfError - fallback not used") {
    val first = new StorageBackend[Task] {
      override def head(sha256: Sha256): Task[Either[StorageException, HeadResult]] = Task.now(Right(HeadResult.Exists(42)))

      override def get(sha256: Sha256, dest: File): Task[Either[StorageException, GetResult]] = {
        Task.now(Right(GetResult.Downloaded(dest, 42)))
      }
      override def close(): Unit = ()
    }

    val second = new StorageBackend[Task] {
      override def head(sha256: Sha256): Task[Either[StorageException, HeadResult]] = Task.raiseError(new RuntimeException)

      override def get(sha256: Sha256, dest: File): Task[Either[StorageException, GetResult]] = Task.raiseError(new RuntimeException)

      override def close(): Unit = ()
    }

    val merged = first.withFallbackIfError(second)

    assertResult(Right(HeadResult.Exists(42)))(merged.head(randomSha).runToFuture.futureValue)
    val dest = File.newTemporaryFile()
    assertResult(Right(GetResult.Downloaded(dest, 42)))(merged.get(randomSha, dest).runToFuture.futureValue)
  }

  test("withFallbackIfError - fallback used") {
    val first = new StorageBackend[Task] {
      override def head(sha256: Sha256): Task[Either[StorageException, HeadResult]] = Task.now(Left(InvalidResponseException(500, "", "")))

      override def get(sha256: Sha256, dest: File): Task[Either[StorageException, GetResult]] = {
        Task.now(Left(InvalidResponseException(500, "", "")))
      }
      override def close(): Unit = ()
    }

    val second = new StorageBackend[Task] {
      override def head(sha256: Sha256): Task[Either[StorageException, HeadResult]] = Task.now(Right(HeadResult.Exists(42)))

      override def get(sha256: Sha256, dest: File): Task[Either[StorageException, GetResult]] = {
        Task.now(Right(GetResult.Downloaded(dest, 42)))
      }
      override def close(): Unit = ()
    }

    val merged = first.withFallbackIfError(second)

    assertResult(Right(HeadResult.Exists(42)))(merged.head(randomSha).runToFuture.futureValue)
    val dest = File.newTemporaryFile()
    assertResult(Right(GetResult.Downloaded(dest, 42)))(merged.get(randomSha, dest).runToFuture.futureValue)
  }
}
