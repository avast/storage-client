package com.avast.clients

import better.files.File
import cats.Monad
import cats.data.EitherT
import com.avast.scala.hashes.Sha256

import scala.language.higherKinds

package object storage {

  implicit class StorageBackendOps[F[_]: Monad](val backend: StorageBackend[F]) {
    def withFallbackIfError(fallback: StorageBackend[F]): StorageBackend[F] = new StorageBackend[F] {
      override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = {
        EitherT(backend.head(sha256))
          .orElse(EitherT(fallback.head(sha256)))
          .value
      }

      override def get(sha256: Sha256, dest: File): F[Either[StorageException, GetResult]] = {
        EitherT(backend.get(sha256, dest))
          .orElse(EitherT(fallback.get(sha256, dest)))
          .value
      }

      override def close(): Unit = {
        backend.close()
        fallback.close()
      }
    }

    def withFallbackIfNotFound(fallback: StorageBackend[F]): StorageBackend[F] = new StorageBackend[F] {
      override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = {
        EitherT(backend.head(sha256))
          .flatMap[StorageException, HeadResult] {
            case r: HeadResult.Exists => EitherT.rightT(r)
            case HeadResult.NotFound => EitherT(fallback.head(sha256))
          }
          .value
      }

      override def get(sha256: Sha256, dest: File): F[Either[StorageException, GetResult]] = {
        EitherT(backend.get(sha256, dest))
          .flatMap[StorageException, GetResult] {
            case r: GetResult.Downloaded => EitherT.rightT(r)
            case GetResult.NotFound => EitherT(fallback.get(sha256, dest))
          }
          .value
      }

      override def close(): Unit = {
        backend.close()
        fallback.close()
      }
    }
  }
}
