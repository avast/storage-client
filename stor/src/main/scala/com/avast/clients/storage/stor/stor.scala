package com.avast.clients.storage

import better.files.File
import cats.arrow.FunctionK
import cats.effect.{Effect, IO, Sync}
import cats.~>
import com.avast.scala.hashes.Sha256
import mainecoon.FunctorK
import monix.eval.Task
import monix.execution.Scheduler

import scala.language.higherKinds
import scala.util.control.NonFatal

package object stor {
  type FromTask[A[_]] = FunctionK[Task, A]

  implicit val fkTask: FromTask[Task] = FunctionK.id

  implicit val producerFunctorK: FunctorK[StorClient] = new FunctorK[StorClient] {
    override def mapK[F[_], G[_]](client: StorClient[F])(fToG: ~>[F, G]): StorClient[G] = new StorClient[G] {
      override def head(sha256: Sha256): G[Either[StorException, HeadResult]] = fToG {
        client.head(sha256)
      }

      override def get(sha256: Sha256, dest: File): G[Either[StorException, GetResult]] = fToG {
        client.get(sha256, dest)
      }
    }

  }


}

