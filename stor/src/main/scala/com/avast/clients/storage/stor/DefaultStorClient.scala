package com.avast.clients.storage.stor

import java.io.ByteArrayInputStream

import better.files.File
import com.avast.scala.hashes.Sha256
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers.`Content-Length`

import scala.util.control.NonFatal

class DefaultStorClient(rootUri: Uri, httpClient: Client[Task])(implicit sch: Scheduler) extends StorClient[Task] with StrictLogging {

  override def head(sha256: Sha256): Task[Either[StorException, HeadResult]] = {
    logger.debug(s"Checking presence of file $sha256 in Stor")

    try {
      val request = Request[Task](
        Method.HEAD,
        rootUri / sha256.toString
      )

      httpClient.fetch(request) { resp =>
        resp.status match {
          case Status.Ok =>
            `Content-Length`.from(resp.headers) match {
              case Some(`Content-Length`(length)) => Task.now(Right(HeadResult.Exists(length)))
              case None =>
                resp.bodyAsText.compile.last.map { body =>
                  Left(StorException.InvalidResponseException(resp.status.code, body.toString, "Missing Content-Length header"))
                }
            }
          case Status.NotFound =>
            Task.now(Right(HeadResult.NotFound))

          case _ =>
            resp.bodyAsText.compile.last.map { body =>
              Left(StorException.InvalidResponseException(resp.status.code, body.toString, "Unexpected status"))
            }
        }
      }
    } catch {
      case NonFatal(e) => Task.raiseError(e)
    }
  }

  override def get(sha256: Sha256, dest: File): Task[Either[StorException, GetResult]] = {
    logger.debug(s"Getting file $sha256 from Stor")

    try {
      val request = Request[Task](
        Method.GET,
        rootUri / sha256.toString
      )

      httpClient.fetch(request) { resp =>
        resp.status match {
          case Status.Ok => receiveStreamedFile(sha256, dest, resp)
          case Status.NotFound => Task.now(Right(GetResult.NotFound))

          case _ =>
            resp.bodyAsText.compile.last.map { body =>
              Left(StorException.InvalidResponseException(resp.status.code, body.toString, "Unexpected status"))
            }
        }

      }
    } catch {
      case NonFatal(e) => Task.raiseError(e)
    }
  }

  private def receiveStreamedFile(sha256: Sha256,
                                  dest: File,
                                  resp: Response[Task]): Task[Either[StorException.InvalidResponseException, GetResult]] = {
    `Content-Length`.from(resp.headers) match {
      case Some(clh) =>
        val fileCopier = new FileCopier
        val fileOs = dest.newOutputStream

        resp.body.chunks
          .map { bytes =>
            val bis = new ByteArrayInputStream(bytes.toArray)
            val copied = fileCopier.copy(bis, fileOs)
            bis.close()
            copied
          }
          .compile
          .toVector
          .map { chunksSizes =>
            val transferred = chunksSizes.sum

            fileOs.close() // all data has been transferred

            if (clh.length != transferred) {
              Left(StorException.InvalidResponseException(resp.status.code, "-stream-", s"Expected ${clh.length} B but got $transferred B"))
            } else {
              val transferredSha = fileCopier.finalSha256

              if (transferredSha != sha256) {
                Left(
                  StorException.InvalidResponseException(resp.status.code, "-stream-", s"Expected SHA256 $sha256 but got $transferredSha"))
              } else {
                Right(GetResult.Downloaded(dest, transferred))
              }
            }
          }

      case None => Task.now(Left(StorException.InvalidResponseException(resp.status.code, "-stream-", "Missing Content-Length header")))
    }
  }
}

case class BasicAuth(username: String, password: String)
