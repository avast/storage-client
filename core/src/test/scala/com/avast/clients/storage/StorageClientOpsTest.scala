package com.avast.clients.storage

import better.files.File
import com.avast.scala.hashes.Sha256
import monix.eval.Task
import org.scalatest.FunSuite
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future
import TestImplicits._
import org.scalatest.concurrent.ScalaFutures

class StorageClientOpsTest extends FunSuite with ScalaFutures {
  test("mapK") {

    val backend = new StorageBackend[Task] {
      override def head(sha256: Sha256): Task[Either[StorageException, HeadResult]] = Task.now(Right(HeadResult.Exists(42)))

      override def get(sha256: Sha256, dest: File): Task[Either[StorageException, GetResult]] = {
        Task.now(Right(GetResult.Downloaded(dest, 42)))
      }

      override def close(): Unit = ()
    }

    val client: StorageClient[Task] = new DefaultStorageClient[Task](backend)
    val futureClient: StorageClient[Future] = client.mapK[Future]

    val sha = randomSha

    assertResult(client.head(sha).runAsync.futureValue)(futureClient.head(sha).futureValue)
  }
}
