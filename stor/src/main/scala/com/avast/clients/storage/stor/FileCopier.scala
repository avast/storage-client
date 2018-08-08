package com.avast.clients.storage.stor

import java.io.{InputStream, OutputStream}
import java.security.MessageDigest

import com.avast.scala.hashes.Sha256
import org.apache.commons.io.IOUtils

class FileCopier {

  private val lock = new Object

  private val digest: MessageDigest = Sha256Provider.get()

  private var finalHash: Option[Sha256] = None // scalastyle:ignore

  def copy(is: InputStream, os: OutputStream): Int = lock.synchronized {
    val fis = new ProxyInputStream(is)(digest.update(_))

    IOUtils.copy(fis, os)
  }

  def finalSha256: Sha256 = lock.synchronized {
    finalHash match {
      case Some(hash) => hash
      case None =>
        val hash = Sha256(digest.digest())
        finalHash = Some(hash)
        hash
    }
  }
}

private class ProxyInputStream(is: InputStream)(handle: Array[Byte] => Unit) extends InputStream {

  override def read(): Int = {
    val b = is.read()

    if (b != -1) handle(Array(b.toByte))

    b
  }

  override def read(b: Array[Byte]): Int = {
    val bytes = is.read(b)

    handle(b.slice(0, bytes))

    bytes
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val bytes = is.read(b, off, len)

    handle(b.slice(off, off + bytes))

    bytes
  }

  override def markSupported(): Boolean = is.markSupported()

  override def available(): Int = is.available()

  override def skip(n: Long): Long = is.skip(n)

  override def reset(): Unit = is.reset()

  override def close(): Unit = is.close()

  override def mark(readlimit: Int): Unit = is.mark(readlimit)
}
