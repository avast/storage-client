package com.avast.clients.storage.compression

import com.avast.scala.hashes.Sha256
import com.github.luben.zstd.Zstd
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.security.MessageDigest

@RunWith(classOf[JUnitRunner])
class ZstdDecompressOutputStreamTest extends FunSuite {
  private def computeSha256(data: Array[Byte]): Sha256 = {
    Sha256(MessageDigest.getInstance("SHA-256").digest(data))
  }

  private def generateData(size: Int): Array[Byte] = {
    1.to(size).map(i => (i % 256).toByte).toArray // generates compressible data, random data wouldn't compress well
  }

  test("decompress zstd stream") {
    val chunkSize = 4 * 1024

    val lengths = Seq(0, 1, chunkSize, 10 * 1024 * 1024)
    val levels = Seq(Zstd.minCompressionLevel(), Zstd.defaultCompressionLevel(), 9, Zstd.maxCompressionLevel())

    val testCases = for {
      size <- lengths
      level <- levels
    } yield (size, level)

    for ((size, level) <- testCases) {
      val original_data = generateData(size)
      val original_sha256 = computeSha256(original_data)

      println(s"Original data size: ${original_data.length}, level: $level")
      val compressed_data = Zstd.compress(original_data, level)
      println(s"Compressed data size: ${compressed_data.length}, level: $level")

      val sourceStream = ByteBuffer.wrap(compressed_data)
      val targetStream = new ByteArrayOutputStream()

      val decompressStream = new ZstdDecompressOutputStream(targetStream)

      while (sourceStream.hasRemaining) {
        val chunkSize = math.min(sourceStream.remaining(), 4 * 1024)
        val chunk = new Array[Byte](chunkSize)
        sourceStream.get(chunk)
        decompressStream.write(chunk)
      }

      decompressStream.close()

      val result = targetStream.toByteArray

      assert(original_sha256 == computeSha256(result))
    }
  }
}
