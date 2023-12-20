package com.avast.clients.storage.gcs

import better.files.File
import cats.effect.implicits.catsEffectSyntaxBracket
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.syntax.all._
import com.avast.clients.storage.compression.ZstdDecompressOutputStream
import com.avast.clients.storage.gcs.GcsStorageBackend.{composeBlobPath, getMetadataValue, CompressionTypeHeader, OriginalSizeHeader}
import com.avast.clients.storage.{ConfigurationException, GetResult, HeadResult, StorageBackend, StorageException}
import com.avast.scala.hashes.Sha256
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.ServiceOptions
import com.google.cloud.storage.{Blob, BlobId, Storage, StorageOptions, StorageException => GcStorageException}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import pureconfig.error.ConfigReaderException
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping}

import java.io.{ByteArrayInputStream, FileInputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption
import java.security.{DigestOutputStream, MessageDigest}

class GcsStorageBackend[F[_]: Sync: ContextShift](storageClient: Storage, bucketName: String)(blocker: Blocker)
    extends StorageBackend[F]
    with StrictLogging {
  private val FileStreamOpenOptions = Seq(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

  override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = {
    {
      for {
        _ <- Sync[F].delay(logger.debug(s"Checking presence of file $sha256 in GCS"))
        blob <- getBlob(sha256)
        result = blob match {
          case Some(blob) =>
            getMetadataValue(blob, OriginalSizeHeader) match {
              case None =>
                HeadResult.Exists(blob.getSize)
              case Some(originalSize) =>
                HeadResult.Exists(originalSize.toLong)
            }
          case None =>
            HeadResult.NotFound
        }
      } yield Either.right[StorageException, HeadResult](result)
    }.recover {
      case e: GcStorageException =>
        logger.error(s"Error while checking presence of file $sha256 in GCS", e)
        Either.left[StorageException, HeadResult] {
          StorageException.InvalidResponseException(e.getCode, e.getMessage, e.getReason)
        }
    }
  }

  override def get(sha256: Sha256, dest: File): F[Either[StorageException, GetResult]] = {
    {
      for {
        _ <- Sync[F].delay(logger.debug(s"Downloading file $sha256 from GCS"))
        blob <- getBlob(sha256)
        result <- blob match {
          case Some(blob) =>
            receiveStreamedFile(blob, dest, sha256)
          case None =>
            Sync[F].pure[Either[StorageException, GetResult]] {
              Right(GetResult.NotFound)
            }
        }
      } yield result
    }.recover {
      case e: GcStorageException =>
        logger.error(s"Error while downloading file $sha256 from GCS", e)
        Either.left[StorageException, GetResult] {
          StorageException.InvalidResponseException(e.getCode, e.getMessage, e.getReason)
        }
    }
  }

  private def getBlob(sha256: Sha256): F[Option[Blob]] = {
    for {
      objectPath <- Sync[F].delay(composeBlobPath(sha256))
      result <- blocker.delay {
        Option(storageClient.get(BlobId.of(bucketName, objectPath)))
      }
    } yield result
  }

  private def receiveStreamedFile(blob: Blob, destination: File, expectedHash: Sha256): F[Either[StorageException, GetResult]] = {
    Sync[F].delay(logger.debug(s"Downloading streamed data to $destination")) >>
      blocker
        .delay(destination.newOutputStream(FileStreamOpenOptions))
        .bracket { fileStream =>
          downloadBlobToFile(blob, fileStream)
        }(fileStream => blocker.delay(fileStream.close()))
        .map[Either[StorageException, GetResult]] {
          case (size, hash) =>
            if (expectedHash != hash) {
              Left {
                StorageException.InvalidDataException(200, "-stream-", s"Expected SHA256 $expectedHash but got $hash")
              }
            } else {
              Right {
                GetResult.Downloaded(destination, size)
              }
            }
        }
  }

  private def downloadBlobToFile(blob: Blob, fileStream: OutputStream): F[(Long, Sha256)] = {

    Sync[F]
      .delay {
        val countingStream = new GcsStorageBackend.CountingOutputStream(fileStream)
        val hashingStream = new DigestOutputStream(countingStream, MessageDigest.getInstance("SHA-256"))
        (countingStream, hashingStream)
      }
      .bracket {
        case (countingStream, hashingStream) => {
          getMetadataValue(blob, CompressionTypeHeader) match {
            case None =>
              downloadBlobToStream(blob, hashingStream)
            case Some("zstd") =>
              decodeZstdBlobToStream(blob, hashingStream)
            case Some(unknown) =>
              throw new IllegalArgumentException(s"Unknown compression type $unknown")
          }
        }.flatMap { _ =>
          Sync[F].delay {
            (countingStream.length, Sha256(hashingStream.getMessageDigest.digest))
          }
        }
      } {
        case (hashingStream, countingStream) =>
          Sync[F].delay {
            hashingStream.close()
            countingStream.close()
          }
      }
  }

  private def decodeZstdBlobToStream(blob: Blob, targetStream: DigestOutputStream): F[Unit] = {
    Sync[F]
      .delay(new ZstdDecompressOutputStream(targetStream))
      .bracket { decompressionStream =>
        downloadBlobToStream(blob, decompressionStream)
      }(decompressionStream => Sync[F].delay(decompressionStream.close()))
  }

  private def downloadBlobToStream(blob: Blob, targetStream: OutputStream): F[Unit] = {
    blocker.delay(blob.downloadTo(targetStream))
  }

  override def close(): Unit = {
    ()
  }
}

object GcsStorageBackend {
  private val DefaultConfig = ConfigFactory.defaultReference().getConfig("gcsBackendDefaults")

  private[gcs] val CompressionTypeHeader = "comp-type"
  private[gcs] val OriginalSizeHeader = "original-size"

  def fromConfig[F[_]: Sync: ContextShift](config: Config,
                                           blocker: Blocker): Either[ConfigurationException, Resource[F, GcsStorageBackend[F]]] = {

    def composeConfig: Either[ConfigurationException, GcsBackendConfiguration] = {
      pureconfig.ConfigSource
        .fromConfig(config.withFallback(DefaultConfig))
        .load[GcsBackendConfiguration]
        .leftMap { failures =>
          ConfigurationException("Could not load config", new ConfigReaderException[GcsBackendConfiguration](failures))
        }
    }

    for {
      conf <- composeConfig
      storageClient <- prepareStorageClient(conf, blocker)
    } yield {
      Resource
        .fromAutoCloseable {
          Sync[F].pure(storageClient)
        }
        .map { storageClient =>
          new GcsStorageBackend[F](storageClient, conf.bucketName)(blocker)
        }
    }
  }

  private[gcs] def composeBlobPath(sha256: Sha256): String = {
    val sha256Hex = sha256.toHexString
    String.join("/", sha256Hex.substring(0, 2), sha256Hex.substring(2, 4), sha256Hex.substring(4, 6), sha256Hex)
  }

  private[gcs] def getMetadataValue(blob: Blob, key: String): Option[String] = {
    Option(blob.getMetadata).flatMap(m => Option(m.get(key)))
  }

  private[gcs] class CountingOutputStream(target: OutputStream) extends OutputStream {
    private var count: Long = 0

    def length: Long = count

    override def write(b: Int): Unit = {
      target.write(b)
      count += 1
    }

    override def write(b: Array[Byte]): Unit = {
      target.write(b)
      count += b.length
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      target.write(b, off, len)
      count += len
    }

    override def flush(): Unit = {
      target.flush()
    }

    override def close(): Unit = {
      target.close()
    }
  }

  def prepareStorageClient[F[_]: Sync: ContextShift](conf: GcsBackendConfiguration,
                                                     blocker: Blocker): Either[ConfigurationException, Storage] = {
    Either
      .catchNonFatal {
        val credentialsFileContent = conf.credentialsFile
          .map { credentialsFilePath =>
            new FileInputStream(credentialsFilePath)
          }
          .orElse {
            sys.env.get("GOOGLE_APPLICATION_CREDENTIALS_RAW").map { credentialFileRaw =>
              new ByteArrayInputStream(credentialFileRaw.getBytes(StandardCharsets.UTF_8))
            }
          }

        val builder = credentialsFileContent match {
          case Some(inputStream) =>
            StorageOptions.newBuilder
              .setCredentials(ServiceAccountCredentials.fromStream(inputStream))
          case None =>
            StorageOptions.getDefaultInstance.toBuilder
        }

        builder
          .setProjectId(conf.projectId)
          .setRetrySettings(ServiceOptions.getNoRetrySettings)

        builder.build.getService
      }
      .leftMap { e =>
        ConfigurationException("Could not create GCS client", e)
      }
  }
}

case class GcsBackendConfiguration(projectId: String, bucketName: String, credentialsFile: Option[String] = None)

object GcsBackendConfiguration {
  // configure pureconfig:
  implicit val productHint: ProductHint[GcsBackendConfiguration] = ProductHint[GcsBackendConfiguration](
    fieldMapping = ConfigFieldMapping(CamelCase, CamelCase)
  )
}
