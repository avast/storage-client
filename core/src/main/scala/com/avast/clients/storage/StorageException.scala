package com.avast.clients.storage

sealed abstract class StorageException(msg: String, cause: Throwable = null) extends Exception(msg, cause)

object StorageException {

  case class InvalidResponseException(status: Int, body: String, desc: String, cause: Throwable = null)
      extends StorageException(s"Invalid response with status $status: $desc", cause)

}
