package com.avast.clients.storage.compression

import com.github.luben.zstd.{ZstdDecompressCtx, ZstdInputStream}

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels

class ZstdDecompressOutputStream(outputStream: OutputStream) extends OutputStream {
  private val decompressCtx = new ZstdDecompressCtx()
  private val outputChannel = Channels.newChannel(outputStream)
  private val closed = false

  override def write(chunk: Array[Byte]): Unit = {
    if (closed) {
      throw new IllegalStateException("Stream is closed")
    }

    val inputBuffer = ByteBuffer.allocateDirect(chunk.length)
    val outputBuffer = ByteBuffer.allocateDirect(ZstdInputStream.recommendedDOutSize().toInt)

    inputBuffer.put(chunk)
    inputBuffer.flip()

    while (inputBuffer.hasRemaining) {
      decompressCtx.decompressDirectByteBufferStream(outputBuffer, inputBuffer)

      outputBuffer.flip()

      while (outputBuffer.hasRemaining) {
        outputChannel.write(outputBuffer)
      }

      outputBuffer.clear()
    }
  }

  override def write(chunk: Array[Byte], offset: Int, length: Int): Unit = {
    write(chunk.slice(offset, offset + length))
  }

  override def write(b: Int): Unit = {
    write(Array(b.toByte))
  }

  override def close(): Unit = {
    if (!closed) {
      decompressCtx.close()
      outputChannel.close()
    }
  }
}
