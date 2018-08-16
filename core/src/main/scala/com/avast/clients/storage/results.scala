package com.avast.clients.storage

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
