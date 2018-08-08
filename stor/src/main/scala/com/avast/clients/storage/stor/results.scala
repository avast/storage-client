package com.avast.clients.storage.stor

import better.files.File

sealed trait HeadResult

object HeadResult {

  case class Exists(length: Long) extends HeadResult

  case object NotFound extends HeadResult

}

sealed trait GetResult

object GetResult {

  case class Downloaded(file: File, fileSize: Long) extends GetResult

  case object NotFound extends GetResult

}

sealed trait PostResult

object PostResult {

  case object AlreadyExists extends PostResult

  case object Created extends PostResult

  case object Unauthorized extends PostResult

  case object ShaMismatch extends PostResult

  case object InsufficientStorage extends PostResult

}
