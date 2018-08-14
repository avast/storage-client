package com.avast.clients.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest.FunSuite

class FileCopierTest extends FunSuite {
  test("basic") {
    val bis = new ByteArrayInputStream("helloworld".getBytes)
    val bos = new ByteArrayOutputStream()

    val copier = new FileCopier

    copier.copy(bis, bos)

    assertResult("helloworld")(bos.toString)
    assertResult("936a185caaa266bb9cbe981e9e05cb78cd732b0b3280eb944412bb6f8f8f07af")(copier.finalSha256.toString())
  }

  test("basic2") {
    val bis = new ByteArrayInputStream("936a185caaa266bb9cbe981e9e05cb78cd732b0b3280eb944412bb6f8f8f07af".getBytes)
    val bos = new ByteArrayOutputStream()

    val copier = new FileCopier

    copier.copy(bis, bos)

    assertResult("936a185caaa266bb9cbe981e9e05cb78cd732b0b3280eb944412bb6f8f8f07af")(bos.toString)
    assertResult("cda6fa38611c153780f3a234cd986c51eb8de001aeab5fb5ca1c66506f0c83b4")(copier.finalSha256.toString)
  }
}
