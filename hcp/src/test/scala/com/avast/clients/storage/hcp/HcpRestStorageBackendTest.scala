package com.avast.clients.storage.hcp
import cats.data.NonEmptyList
import cats.effect.IO
import com.avast.clients.storage.hcp.TestImplicits._
import com.avast.clients.storage.{GetResult, HeadResult}
import com.avast.scala.hashes.Sha256
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.http4s.client.blaze.Http1Client
import org.http4s.dsl.io._
import org.http4s.headers.`Content-Length`
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.{HttpService, Uri}
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Seconds, Span}

class HcpRestStorageBackendTest extends FunSuite with ScalaFutures with MockitoSugar {
  implicit val p: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds))

  test("head") {
    val fileSize = 1000000
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()

    val service = HttpService[IO] {
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
            }(urlPath.toList)

            assertResult(0)(body.length)

            Ok().map(_.putHeaders(`Content-Length`.unsafeFromLong(fileSize)))
          }

    }

    val server = BlazeBuilder[IO].bindHttp(port = 0).mountService(service).start.unsafeRunSync()

    val httpClient = Http1Client[Task]().runAsync.futureValue

    val client = new HcpRestStorageBackend(
      Uri.fromString(s"http://localhost:${server.address.getPort}/rest").getOrElse(fail()),
      httpClient
    )

    val Right(HeadResult.Exists(size)) = client.head(sha).runAsync.futureValue

    assertResult(fileSize)(size)
  }

  test("get") {
    val fileSize = 1000000
    val content = randomString(fileSize)
    val sha = content.sha256
    val shaStr = sha.toString()

    val service = HttpService[IO] {
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
            }(urlPath.toList)

            assertResult(0)(body.length)

            Ok(content)
          }

    }

    val server = BlazeBuilder[IO].bindHttp(port = 0).mountService(service).start.unsafeRunSync()

    val httpClient = Http1Client[Task]().runAsync.futureValue

    val client = new HcpRestStorageBackend(
      Uri.fromString(s"http://localhost:${server.address.getPort}/rest").getOrElse(fail()),
      httpClient
    )

    val Right(GetResult.Downloaded(file, size)) = client.get(sha).runAsync.futureValue

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
}
