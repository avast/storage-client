package com.avast.clients.storage.hcp
import java.io.ByteArrayInputStream

import better.files.File
import cats.data.NonEmptyList
import cats.effect.Effect
import cats.syntax.all._
import com.avast.clients.storage.hcp.HcpRestStorageBackend._
import com.avast.clients.storage.{ConfigurationException, FileCopier, GetResult, HeadResult, StorageBackend, StorageException}
import com.avast.scala.hashes.Sha256
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.http4s.client.Client
import org.http4s.client.blaze.{BlazeClientConfig, Http1Client}
import org.http4s.headers.`Content-Length`
import org.http4s.{Method, Request, Response, Status, _}
import pureconfig._
import pureconfig.error.ConfigReaderException
import pureconfig.modules.http4s.uriReader

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.control.NonFatal
class HcpRestStorageBackend[F[_]](rootUri: Uri, httpClient: Client[F])(implicit F: Effect[F]) extends StorageBackend[F] with StrictLogging {
  override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = {
    logger.debug(s"Checking presence of file $sha256 in HCP")

    val finalUri: Uri = splitFileName(sha256).foldLeft(rootUri)(_ / _)

    try {
      val request = Request[F](
        Method.HEAD,
        finalUri
      )

      httpClient.fetch(request) { resp =>
        resp.status match {
          case Status.Ok =>
            `Content-Length`.from(resp.headers) match {
              case Some(`Content-Length`(length)) => F.pure(Right(HeadResult.Exists(length)))
              case None =>
                resp.bodyAsText.compile.last.map { body =>
                  Left(StorageException.InvalidResponseException(resp.status.code, body.toString, "Missing Content-Length header"))
                }
            }
          case Status.NotFound =>
            F.pure(Right(HeadResult.NotFound))

          case _ =>
            resp.bodyAsText.compile.last.map { body =>
              Left(StorageException.InvalidResponseException(resp.status.code, body.toString, "Unexpected status"))
            }
        }
      }
    } catch {
      case NonFatal(e) => F.raiseError(e)
    }
  }

  override def get(sha256: Sha256, dest: File): F[Either[StorageException, GetResult]] = {
    logger.debug(s"Getting file $sha256 from HCP")

    val finalUri: Uri = splitFileName(sha256).foldLeft(rootUri)(_ / _)

    try {
      val request = Request[F](
        Method.GET,
        finalUri
      )

      httpClient.fetch(request) { resp =>
        resp.status match {
          case Status.Ok => receiveStreamedFile(sha256, dest, resp)
          case Status.NotFound => F.pure(Right(GetResult.NotFound))

          case _ =>
            resp.bodyAsText.compile.last.map { body =>
              Left(StorageException.InvalidResponseException(resp.status.code, body.toString, "Unexpected status"))
            }
        }

      }
    } catch {
      case NonFatal(e) => F.raiseError(e)
    }
  }

  override def close(): Unit = httpClient.shutdownNow()

  private def receiveStreamedFile(sha256: Sha256, dest: File, resp: Response[F]): F[Either[StorageException, GetResult]] = {
    `Content-Length`.from(resp.headers) match {
      case Some(clh) =>
        val fileCopier = new FileCopier
        val fileOs = dest.newOutputStream

        resp.body.chunks
          .map { bytes =>
            val bis = new ByteArrayInputStream(bytes.toArray)
            try {
              fileCopier.copy(bis, fileOs)
            } finally {
              bis.close()
            }
          }
          .compile
          .toVector
          .onError {
            case NonFatal(_) => F.delay(fileOs.close())
          }
          .map { chunksSizes =>
            val transferred = chunksSizes.sum

            fileOs.close() // all data has been transferred

            if (clh.length != transferred) {
              Left(StorageException.InvalidDataException(resp.status.code, "-stream-", s"Expected ${clh.length} B but got $transferred B"))
            } else {
              val transferredSha = fileCopier.finalSha256

              if (transferredSha != sha256) {
                Left {
                  StorageException.InvalidDataException(resp.status.code, "-stream-", s"Expected SHA256 $sha256 but got $transferredSha")
                }
              } else {
                Right(GetResult.Downloaded(dest, transferred))
              }
            }
          }

      case None => F.pure(Left(StorageException.InvalidResponseException(resp.status.code, "-stream-", "Missing Content-Length header")))
    }
  }
}

object HcpRestStorageBackend {
  private val RootConfigKey = "hcpRestBackendDefaults"
  private val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  // configure pureconfig:
  private implicit val ph: ProductHint[HcpRestBackendConfiguration] = ProductHint[HcpRestBackendConfiguration](
    fieldMapping = ConfigFieldMapping(CamelCase, CamelCase)
  )

  def fromConfig[F[_]: Effect](config: Config)(
      implicit ec: ExecutionContext): Either[ConfigurationException, F[HcpRestStorageBackend[F]]] = {

    def assembleUri(repository: String, namespace: String, tenant: String, protocol: String): Either[ConfigurationException, Uri] = {
      Uri
        .fromString(s"$protocol://$namespace.$tenant.$repository/rest")
        .leftMap(ConfigurationException("Could not assemble final URI", _))

    }

    def loadConfig: Either[ConfigurationException, HcpRestBackendConfiguration] = {
      pureconfig
        .loadConfig[HcpRestBackendConfiguration](config.withFallback(DefaultConfig))
        .leftMap { failures =>
          ConfigurationException("Could not load config", new ConfigReaderException[HcpRestBackendConfiguration](failures))
        }
    }

    for {
      conf <- loadConfig
      uri <- {
        import conf._
        assembleUri(repository, namespace, tenant, protocol)
      }
    } yield {
      Http1Client[F](conf.toBlazeConfig.copy(executionContext = ec))
        .map(new HcpRestStorageBackend[F](uri, _))
    }
  }

  private[storage] def splitFileName(sha256: Sha256): NonEmptyList[String] = {
    val str = sha256.toString()

    NonEmptyList.of(str.substring(0, 2), str.substring(2, 4), str.substring(4, 6), str)
  }
}

private case class HcpRestBackendConfiguration(repository: String,
                                               namespace: String,
                                               tenant: String,
                                               protocol: String,
                                               requestTimeout: Duration,
                                               socketTimeout: Duration,
                                               responseHeaderTimeout: Duration,
                                               maxConnections: Int,
                                               userAgent: Option[String]) {
  def toBlazeConfig: BlazeClientConfig = BlazeClientConfig.defaultConfig.copy(
    requestTimeout = requestTimeout,
    maxTotalConnections = maxConnections,
    responseHeaderTimeout = responseHeaderTimeout,
    idleTimeout = socketTimeout,
    userAgent = userAgent.map {
      org.http4s.headers.`User-Agent`.parse(_).getOrElse(throw new IllegalArgumentException("Unsupported format of user-agent provided"))
    }
  )
}
