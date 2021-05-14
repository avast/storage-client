package com.avast.clients.storage.hcp

import cats.implicits._
import org.http4s.Uri
import org.http4s.client.RequestKey

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.ThreadLocalRandom

object BalancingDnsResolver {
  def getAddress(requestKey: RequestKey): Either[Throwable, InetSocketAddress] =
    requestKey match {
      case RequestKey(s, auth) =>
        val port = auth.port.getOrElse(if (s == Uri.Scheme.https) 443 else 80)
        val hostname = auth.host.value

        Either
          .catchNonFatal {
            // Using JVM DNS cache, see: https://javaeesupportpatterns.blogspot.com/2011/03/java-dns-cache-reference-guide.html
            val addresses = InetAddress.getAllByName(hostname)
            addresses(ThreadLocalRandom.current().nextInt(addresses.size))
          }
          .map { address =>
            new InetSocketAddress(address, port)
          }
    }
}
