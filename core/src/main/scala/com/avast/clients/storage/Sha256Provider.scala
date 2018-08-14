package com.avast.clients.storage

import java.security.MessageDigest

object Sha256Provider extends ThreadLocal[MessageDigest] {
  override def initialValue(): MessageDigest = MessageDigest.getInstance("SHA-256")
}
