package com.avast.clients.storage.gcs

import com.avast.scala.hashes.Sha256

import java.security.MessageDigest
import scala.util.Random

/* Utils for testing. */
object TestImplicits {
  def randomString(length: Int = 100): String = Random.alphanumeric.take(length).mkString

  implicit class StringOps(val s: String) extends AnyVal {
    def sha256: Sha256 = {
      val digest = MessageDigest.getInstance("SHA-256")
      Sha256(digest.digest(s.getBytes))
    }
  }
}
