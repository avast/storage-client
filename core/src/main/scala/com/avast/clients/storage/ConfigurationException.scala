package com.avast.clients.storage

case class ConfigurationException(desc: String, cause: Throwable = null) extends IllegalArgumentException(desc, cause)
