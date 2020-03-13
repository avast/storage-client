package com.avast.clients.storage

import java.io.{ByteArrayInputStream, InputStream}
import java.security.MessageDigest

import cats.arrow.FunctionK
import cats.~>
import com.avast.scala.hashes.Sha256
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/* Utils for testing. */
object TestImplicits {
  def randomSha: Sha256 = Sha256(Stream.continually(Random.nextInt(9)).take(64).mkString)

  def randomString(length: Int = 100): String = Random.alphanumeric.take(length).mkString

  implicit class StringOps(val s: String) extends AnyVal {
    def sha256: Sha256 = {
      val digest = MessageDigest.getInstance("SHA-256")
      Sha256(digest.digest(s.getBytes))
    }

    def newInputStream: InputStream = new ByteArrayInputStream(s.getBytes)
  }
}
