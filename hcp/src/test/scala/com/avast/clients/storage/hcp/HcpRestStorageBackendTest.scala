package com.avast.clients.storage.hcp

import cats.data.NonEmptyList
import cats.effect.{Blocker, Resource}
import com.avast.clients.storage.hcp.TestImplicits._
import com.avast.clients.storage.{GetResult, HeadResult}
import com.avast.scala.hashes.Sha256
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Length`
import org.http4s.implicits._
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.{HttpRoutes, Uri}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class HcpRestStorageBackendTest extends FunSuite with ScalaFutures with MockitoSugar with Http4sDsl[Task] {
  implicit val p: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds))

  test("head") {
    val fileSize = 1000000
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()

    val service = HttpRoutes.of[Task] {
      case request @ HEAD -> urlPath =>
        request
          .as[String]
          .flatMap { body =>
            assertResult {
              List(
                "rest",
                shaStr.substring(0, 2),
                shaStr.substring(2, 4),
                shaStr.substring(4, 6),
                shaStr,
              )
            }(urlPath.segments.map(_.encoded).toList)

            assertResult(0)(body.length)

            Ok().map(_.putHeaders(`Content-Length`.unsafeFromLong(fileSize)))
          }
    }

    val Right(HeadResult.Exists(size)) = composeTestEnv(service)
      .use { target =>
        target.head(sha)
      }
      .runSyncUnsafe(10.seconds)

    assertResult(fileSize)(size)
  }

  test("get") {
    val fileSize = 1000000
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()

    val service = HttpRoutes.of[Task] {
      case request @ GET -> urlPath =>
        request
          .as[String]
          .flatMap { body =>
            assertResult {
              List(
                "rest",
                shaStr.substring(0, 2),
                shaStr.substring(2, 4),
                shaStr.substring(4, 6),
                shaStr,
              )
            }(urlPath.segments.map(_.encoded).toList)

            assertResult(0)(body.length)

            Ok(content)
          }

    }

    val Right(GetResult.Downloaded(file, size)) = composeTestEnv(service)
      .use { target =>
        target.get(sha)
      }
      .runSyncUnsafe(10.seconds)

    assertResult(sha.toString.toLowerCase)(file.sha256.toLowerCase)
    assertResult(fileSize)(size)
    assertResult(fileSize)(file.size)
  }

  test("splitFileName") {
    val sha = Sha256("d05af9a8494696906e8eec79843ca1e4bf408c280616a121ed92f9e92e2de831")

    assertResult {
      NonEmptyList("d0", List("5a", "f9", "d05af9a8494696906e8eec79843ca1e4bf408c280616a121ed92f9e92e2de831"))
    }(HcpRestStorageBackend.splitFileName(sha))
  }

  private def composeTestEnv(service: HttpRoutes[Task]): Resource[Task, HcpRestStorageBackend[Task]] = {
    for {
      server <- BlazeServerBuilder[Task](monix.execution.Scheduler.Implicits.global).bindHttp(port = 0).withHttpApp(service.orNotFound).resource
      httpClient <- BlazeClientBuilder[Task](monix.execution.Scheduler.Implicits.global).resource
      blocker = Blocker.liftExecutionContext(monix.execution.Scheduler.io())
      storageBackend = new HcpRestStorageBackend(
        Uri.unsafeFromString(s"http://localhost:${server.address.getPort}"),
        "N/A",
        "N/A",
        httpClient
      )(blocker)
    } yield storageBackend
  }
}
