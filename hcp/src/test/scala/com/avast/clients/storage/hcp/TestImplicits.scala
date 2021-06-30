package com.avast.clients.storage.hcp

import java.io.{ByteArrayInputStream, InputStream}
import java.security.MessageDigest

import com.avast.scala.hashes.Sha256

import scala.util.Random

/* Utils for testing. */
object TestImplicits {
  def randomSha: Sha256 = Sha256(LazyList.continually(Random.nextInt(9)).take(64).mkString)

  def randomString(length: Int = 100): String = Random.alphanumeric.take(length).mkString

  implicit class StringOps(val s: String) extends AnyVal {
    def sha256: Sha256 = {
      val digest = MessageDigest.getInstance("SHA-256")
      Sha256(digest.digest(s.getBytes))
    }

    def newInputStream: InputStream = new ByteArrayInputStream(s.getBytes)
  }
}
