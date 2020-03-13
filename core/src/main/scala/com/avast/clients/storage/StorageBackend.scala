package com.avast.clients.storage

import better.files.File
import com.avast.scala.hashes.Sha256

import scala.language.higherKinds

trait StorageBackend[F[_]] extends AutoCloseable {
  def head(sha256: Sha256): F[Either[StorageException, HeadResult]]

  def get(sha256: Sha256, dest: File = File.newTemporaryFile(prefix = "stor")): F[Either[StorageException, GetResult]]
}
