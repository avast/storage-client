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
    val builder = Array.newBuilder[Byte]
    var i = 0
    while (i < size) {
      builder += (i % 256).toByte
      i += 1
    }
    builder.result()
  }

  test("decompress zstd stream") {
    val chunkSize = 4 * 1024
    val testCases = Seq(0, 1, chunkSize, 10 * 1024 * 1024)

    for (testCase <- testCases) {
      println(s"Test case: $testCase")

      val original_data = generateData(testCase)
      val original_sha256 = computeSha256(original_data)

      val compressed_data = Zstd.compress(original_data, 9)

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
