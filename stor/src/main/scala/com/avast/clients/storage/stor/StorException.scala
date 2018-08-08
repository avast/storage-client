package com.avast.clients.storage.stor

abstract class StorException(msg: String, cause: Throwable = null) extends Exception(msg, cause)

object StorException {

  case class InvalidResponseException(status: Int, body: String, desc: String, cause: Throwable = null)
      extends StorException(s"Invalid response with status $status: $desc", cause)

}
