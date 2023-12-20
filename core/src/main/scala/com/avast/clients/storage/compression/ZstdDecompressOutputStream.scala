package com.avast.clients.storage.compression

import com.github.luben.zstd.{ZstdDecompressCtx, ZstdInputStream}

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels

class ZstdDecompressOutputStream(outputStream: OutputStream) extends OutputStream {
  private val decompressCtx = new ZstdDecompressCtx()
  private val outputChannel = Channels.newChannel(outputStream)
  private val outputBuffer = ByteBuffer.allocateDirect(ZstdInputStream.recommendedDOutSize().toInt)

  private var closed = false

  override def write(chunk: Array[Byte]): Unit = {
    if (closed) {
      throw new IllegalStateException("Stream is closed")
    }

    val inputBuffer = ByteBuffer.allocateDirect(chunk.length) // ByteBuffer.wrap(chunk) does not work, we need direct buffer
    inputBuffer.put(chunk)
    inputBuffer.rewind()

    while (inputBuffer.hasRemaining) {
      outputBuffer.clear()

      decompressCtx.decompressDirectByteBufferStream(outputBuffer, inputBuffer)

      outputBuffer.flip()

      while (outputBuffer.hasRemaining) {
        outputChannel.write(outputBuffer)
      }
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
      closed = true
      decompressCtx.close()
      outputBuffer.clear()
      outputChannel.close()
    }
  }
}
