package com.avast.clients.storage

import better.files.File
import cats.effect.Effect
import com.avast.scala.hashes.Sha256

import scala.language.higherKinds

trait StorageClient[F[_]] extends AutoCloseable {
  def head(sha256: Sha256): F[Either[StorageException, HeadResult]]

  def get(sha256: Sha256, dest: File = File.newTemporaryFile(prefix = "stor")): F[Either[StorageException, GetResult]]
}

class DefaultStorageClient[F[_]: Effect](backend: StorageBackend[F]) extends StorageClient[F] {
  override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = backend.head(sha256)

  override def get(sha256: Sha256, dest: File): F[Either[StorageException, GetResult]] = backend.get(sha256, dest)

  override def close(): Unit = backend.close()
}
