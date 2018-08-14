package com.avast.clients

import better.files.File
import cats.data.EitherT
import cats.{~>, Monad}
import com.avast.scala.hashes.Sha256
import mainecoon.FunctorK

import scala.language.higherKinds

package object storage {

  implicit class StorageClientOps[F[_]](val client: StorageClient[F]) extends AnyVal {
    def mapK[G[_]](implicit fToG: F ~> G): StorageClient[G] = {
      storageClientFunctorK.mapK(client)(fToG)
    }
  }

  implicit class StorageBackendOps[F[_]: Monad](val backend: StorageBackend[F]) {
    def withFallbackTo(fallback: StorageBackend[F]): StorageBackend[F] = new StorageBackend[F] {
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
  }

  implicit val storageBackendFunctorK: FunctorK[StorageBackend] = new FunctorK[StorageBackend] {
    override def mapK[F[_], G[_]](backend: StorageBackend[F])(fToG: F ~> G): StorageBackend[G] = new StorageBackend[G] {
      override def head(sha256: Sha256): G[Either[StorageException, HeadResult]] = fToG {
        backend.head(sha256)
      }

      override def get(sha256: Sha256, dest: File): G[Either[StorageException, GetResult]] = fToG {
        backend.get(sha256, dest)
      }

      override def close(): Unit = backend.close()
    }
  }

  implicit val storageClientFunctorK: FunctorK[StorageClient] = new FunctorK[StorageClient] {
    override def mapK[F[_], G[_]](client: StorageClient[F])(fToG: F ~> G): StorageClient[G] = new StorageClient[G] {
      override def head(sha256: Sha256): G[Either[StorageException, HeadResult]] = fToG {
        client.head(sha256)
      }

      override def get(sha256: Sha256, dest: File): G[Either[StorageException, GetResult]] = fToG {
        client.get(sha256, dest)
      }

      override def close(): Unit = client.close()
    }
  }
}
