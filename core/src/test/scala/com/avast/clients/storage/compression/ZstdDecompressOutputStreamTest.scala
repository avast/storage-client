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
    1.to(size).map(i => (i % 256).toByte).toArray
  }

  test("decompress zstd stream") {
    val chunkSize = 4 * 1024
    val testCases = Seq(0, 1, chunkSize, 10 * 1024 * 1024)

    for (testCase <- testCases) {
      val original_data = generateData(testCase)
      val original_sha256 = computeSha256(original_data)

      println(s"Original data size: ${original_data.length}")
      val compressed_data = Zstd.compress(original_data, 9)
      println(s"Compressed data size: ${compressed_data.length}")

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
