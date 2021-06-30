package com.avast.clients.storage.hcp
import better.files.File
import cats.data.NonEmptyList
import cats.effect.implicits._
import cats.effect.{Async, Blocker, ConcurrentEffect, ContextShift, Resource, Sync}
import cats.syntax.all._
import com.avast.clients.storage.hcp.HcpRestStorageBackend._
import com.avast.clients.storage.{ConfigurationException, GetResult, HeadResult, StorageBackend, StorageException}
import com.avast.scala.hashes
import com.avast.scala.hashes.Sha256
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.{Client, RequestKey}
import org.http4s.headers.{`Content-Length`, `User-Agent`, AgentProduct}
import org.http4s.{Method, Request, Response, Status, _}
import pureconfig._
import pureconfig.error.ConfigReaderException
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption
import java.security.{DigestOutputStream, MessageDigest}
import java.util.Base64
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

import scala.util.control.NonFatal

class HcpRestStorageBackend[F[_]: Sync: ContextShift](baseUrl: Uri, username: String, password: String, httpClient: Client[F])(
    blocker: Blocker)(implicit F: Async[F])
    extends StorageBackend[F]
    with StrictLogging {

  private val authenticationHeader = composeAuthenticationHeader(username, password)

  override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = {
    logger.debug(s"Checking presence of file $sha256 in HCP")

    val relativeUrl: Uri = composeFileUrl(sha256)

    try {
      val request = prepareRequest(Method.HEAD, relativeUrl)

      httpClient.run(request).use { resp =>
        resp.status match {
          case Status.Ok =>
            `Content-Length`.from(resp.headers) match {
              case Some(`Content-Length`(length)) => F.pure(Right(HeadResult.Exists(length)))
              case None =>
                resp.bodyText.compile.toList.map { body =>
                  Left(StorageException.InvalidResponseException(resp.status.code, body.mkString, "Missing Content-Length header"))
                }
            }
          case Status.NotFound =>
            F.pure(Right(HeadResult.NotFound))

          case _ =>
            resp.bodyText.compile.toList.map { body =>
              Left(StorageException.InvalidResponseException(resp.status.code, body.mkString, "Unexpected status"))
            }
        }
      }

    } catch {
      case NonFatal(e) => F.raiseError(e)
    }
  }

  override def get(sha256: Sha256, dest: File): F[Either[StorageException, GetResult]] = {
    logger.debug(s"Getting file $sha256 from HCP")

    val relativeUrl: Uri = composeFileUrl(sha256)

    try {
      val request = prepareRequest(Method.GET, relativeUrl)

      httpClient
        .run(request).use { resp =>
          resp.status match {
            case Status.Ok => receiveStreamedFile(resp, dest, sha256)
            case Status.NotFound => F.pure(Right(GetResult.NotFound))

            case _ =>
              resp.bodyText.compile.toList.map { body =>
                Left(StorageException.InvalidResponseException(resp.status.code, body.mkString, "Unexpected status"))
              }
          }
        }
    } catch {
      case NonFatal(e) => F.raiseError(e)
    }
  }

  override def close(): Unit = ()

  private def composeFileUrl(sha256: Sha256): Uri = {
    splitFileName(sha256).foldLeft(RelativeUrlBase)(_ / _)
  }

  private def prepareRequest(method: Method, relative: Uri): Request[F] = {
    Request[F](method, baseUrl.copy(path = relative.path), headers = Headers.of(Header("Connection", "close"), authenticationHeader))
  }

  private def receiveStreamedFile(response: Response[F],
                                  destination: File,
                                  expectedHash: Sha256): F[Either[StorageException, GetResult]] = {
    logger.debug(s"Downloading streamed data to $destination")

    val openOptions = Seq(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    blocker
      .blockOn {
        F.delay(destination.newOutputStream(openOptions))
          .bracket { fileStream =>
            F.delay(new DigestOutputStream(fileStream, MessageDigest.getInstance("SHA-256")))
              .bracket { stream =>
                response.body.chunks
                  .map { ch =>
                    stream.write(ch.toArray)
                    ch.size
                  }
                  .compile
                  .toList
                  .map { transferred =>
                    val totalSize = transferred.map(_.toLong).sum
                    (totalSize, Sha256(stream.getMessageDigest.digest))
                  }
              }(stream => F.delay(stream.close()))
          }(fileStream => F.delay(fileStream.close()))
      }
      .map {
        case (transferred, sha256) =>
          verifyResult(destination, transferred, response.contentLength, sha256, expectedHash, response.status.code)
      }
  }

  private def verifyResult(file: File,
                           transferred: Long,
                           expectedSize: Option[Long],
                           hash: Sha256,
                           expectedHash: Sha256,
                           statusCode: Int): Either[StorageException, GetResult] = {

    expectedSize match {
      case Some(contentLength) =>
        if (contentLength != transferred) {
          Left {
            StorageException.InvalidDataException(statusCode, "-stream-", s"Expected $contentLength B but got $transferred B")
          }
        } else if (expectedHash != hash) {
          Left {
            StorageException.InvalidDataException(statusCode, "-stream-", s"Expected SHA256 $expectedHash but got $hash")
          }
        } else {
          Right {
            GetResult.Downloaded(file, transferred)
          }
        }
      case None =>
        Left {
          StorageException.InvalidResponseException(statusCode, "-stream-", "Missing Content-Length header")
        }
    }
  }
}

object HcpRestStorageBackend {
  private val RelativeUrlBase = Uri(path = "/rest")

  def fromConfig[F[_]: ConcurrentEffect: ContextShift](config: Config, blocker: Blocker)(
      implicit ec: ExecutionContext): Either[ConfigurationException, Resource[F, HcpRestStorageBackend[F]]] = {

    def assembleUri(protocol: String, namespace: String, tenant: String, repository: String): Either[ConfigurationException, Uri] = {
      Uri
        .fromString(s"$protocol://$namespace.$tenant.$repository")
        .leftMap(ConfigurationException("Could not assemble final URI", _))
    }

    def composeConfig: Either[ConfigurationException, HcpRestBackendConfiguration] = {
      val DefaultConfig = ConfigFactory.defaultReference().getConfig("hcpRestBackendDefaults")
      pureconfig.ConfigSource
        .fromConfig(config.withFallback(DefaultConfig))
        .load[HcpRestBackendConfiguration]
        .leftMap { failures =>
          ConfigurationException("Could not load config", new ConfigReaderException[HcpRestBackendConfiguration](failures))
        }
    }

    for {
      conf <- composeConfig
      baseUri <- {
        import conf._
        assembleUri(protocol = protocol, namespace = namespace, tenant = tenant, repository = repository)
      }
      clientBuilder = conf.toBlazeClientBuilder[F](ec)
    } yield {
      clientBuilder.resource.map { httpClient =>
        new HcpRestStorageBackend(baseUri, username = conf.username, password = conf.password, httpClient)(blocker)
      }
    }
  }

  private[hcp] def splitFileName(sha256: Sha256): NonEmptyList[String] = {
    val str = sha256.toString()
    NonEmptyList.of(str.substring(0, 2), str.substring(2, 4), str.substring(4, 6), str)
  }

  private def composeAuthenticationHeader(username: String, password: String): Header.Raw = {
    val encodedUserName = Base64.getEncoder.encodeToString(username.getBytes)
    val encodedPassword = {
      val stringBytes = password.getBytes(StandardCharsets.UTF_8)
      hashes.bytes2hex {
        MessageDigest.getInstance("MD5").digest(stringBytes)
      }
    }

    Header("Authorization", s"HCP $encodedUserName:$encodedPassword")
  }
}

case class HcpRestBackendConfiguration(protocol: String,
                                       namespace: String,
                                       tenant: String,
                                       repository: String,
                                       username: String,
                                       password: String,
                                       responseHeaderTimeout: Duration,
                                       requestTimeout: Duration,
                                       idleTimeout: Duration,
                                       maxConnections: Int,
                                       maxConnectionsPerNode: Int,
                                       maxWainQueueLimit: Int,
                                       userAgent: Option[String]) {

  def toBlazeClientBuilder[F[_]: ConcurrentEffect](executionContext: ExecutionContext,
                                                   dnsResolver: RequestKey => Either[Throwable, InetSocketAddress] =
                                                     BalancingDnsResolver.getAddress): BlazeClientBuilder[F] = {
    BlazeClientBuilder
      .apply[F](executionContext)
      .withResponseHeaderTimeout(responseHeaderTimeout)
      .withRequestTimeout(requestTimeout)
      .withIdleTimeout(idleTimeout)
      .withMaxTotalConnections(maxConnections)
      .withMaxConnectionsPerRequestKey(_ => maxConnectionsPerNode)
      .withUserAgentOption(userAgent.map(v => `User-Agent`(AgentProduct(v))))
      .withMaxWaitQueueLimit(maxWainQueueLimit)
      .withCustomDnsResolver(dnsResolver)
  }
}

object HcpRestBackendConfiguration {
  // configure pureconfig:
  implicit val productHint: ProductHint[HcpRestBackendConfiguration] = ProductHint[HcpRestBackendConfiguration](
    fieldMapping = ConfigFieldMapping(CamelCase, CamelCase)
  )
}
