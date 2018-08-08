package com.avast.clients.storage.stor

import better.files.File
import com.avast.scala.hashes.Sha256
import com.typesafe.config.{Config, ConfigFactory}
import mainecoon.FunctorK
import monix.eval.Task
import monix.execution.Scheduler
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.client.blaze.{BlazeClientConfig, Http1Client}
import pureconfig.modules.http4s.uriReader
import pureconfig.{CamelCase, ConfigFieldMapping, ProductHint}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.higherKinds

trait StorClient[F[_]] {
  def head(sha256: Sha256): F[Either[StorException, HeadResult]]

  def get(sha256: Sha256, dest: File = File.newTemporaryFile(prefix = "stor")): F[Either[StorException, GetResult]]
}

object StorClient {
  private val RootConfigKey = "storClientDefaults"
  private val DefaultConfig = ConfigFactory.defaultReference().getConfig(RootConfigKey)

  // configure pureconfig:
  private implicit val ph: ProductHint[StorClientConfiguration] = ProductHint[StorClientConfiguration](
    fieldMapping = ConfigFieldMapping(CamelCase, CamelCase)
  )

  def fromConfig[F[_]: FromTask](config: Config)(implicit sch: Scheduler): StorClient[F] = {
    val conf = pureconfig.loadConfigOrThrow[StorClientConfiguration](config.withFallback(DefaultConfig))
    val httpClient: Client[Task] = Await.result(Http1Client[Task](conf.toBlazeConfig.copy(executionContext = sch)).runAsync, Duration.Inf)

    FunctorK[StorClient].mapK {
      new DefaultStorClient(conf.uri, httpClient)
    }(implicitly[FromTask[F]])
  }
}

private case class StorClientConfiguration(uri: Uri,
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
