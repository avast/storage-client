package com.avast.clients.storage.gcs

import better.files.File
import cats.data.EitherT
import cats.effect.implicits.catsEffectSyntaxBracket
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.syntax.all._
import com.avast.clients.storage.gcs.GcsStorageBackend.composeBlobPath
import com.avast.clients.storage.{ConfigurationException, GetResult, HeadResult, StorageBackend, StorageException}
import com.avast.scala.hashes.Sha256
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.ServiceOptions
import com.google.cloud.storage.{Blob, Bucket, Storage, StorageOptions, StorageException => GcStorageException}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import pureconfig.error.ConfigReaderException
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping}

import java.io.FileInputStream
import java.nio.file.StandardOpenOption
import java.security.{DigestOutputStream, MessageDigest}

class GcsStorageBackend[F[_]: Sync: ContextShift](bucket: Bucket)(blocker: Blocker) extends StorageBackend[F] with StrictLogging {
  private val FileStreamOpenOptions = Seq(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

  override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = {
    {
      for {
        _ <- Sync[F].delay(logger.debug(s"Checking presence of file $sha256 in GCS"))
        blob <- getBlob(sha256)
        result = blob match {
          case Some(blob) =>
            HeadResult.Exists(blob.getSize)
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
        Option(bucket.get(objectPath))
      }
    } yield result
  }

  private def receiveStreamedFile(blob: Blob, destination: File, expectedHash: Sha256): F[Either[StorageException, GetResult]] = {
    Sync[F].delay(logger.debug(s"Downloading streamed data to $destination")) >>
      blocker
        .delay(destination.newOutputStream(FileStreamOpenOptions))
        .bracket { fileStream =>
          Sync[F]
            .delay(new DigestOutputStream(fileStream, MessageDigest.getInstance("SHA-256")))
            .bracket { stream =>
              blocker.delay(blob.downloadTo(stream)).flatMap { _ =>
                Sync[F].delay {
                  (blob.getSize, Sha256(stream.getMessageDigest.digest))
                }
              }
            }(stream => blocker.delay(stream.close()))
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

  override def close(): Unit = {
    ()
  }
}

object GcsStorageBackend {
  private val DefaultConfig = ConfigFactory.defaultReference().getConfig("gcsBackendDefaults")

  def fromConfig[F[_]: Sync: ContextShift](config: Config,
                                           blocker: Blocker): EitherT[F, ConfigurationException, Resource[F, GcsStorageBackend[F]]] = {

    def composeConfig: EitherT[F, ConfigurationException, GcsBackendConfiguration] = EitherT {
      Sync[F].delay {
        pureconfig.ConfigSource
          .fromConfig(config.withFallback(DefaultConfig))
          .load[GcsBackendConfiguration]
          .leftMap { failures =>
            ConfigurationException("Could not load config", new ConfigReaderException[GcsBackendConfiguration](failures))
          }
      }
    }

    {
      for {
        conf <- composeConfig
        storageClient <- prepareStorageClient(conf, blocker)
        bucket <- getBucket(conf, storageClient, blocker)
      } yield (storageClient, bucket)
    }.map {
      case (storage, bucket) =>
        Resource
          .fromAutoCloseable {
            Sync[F].pure(storage)
          }
          .map { _ =>
            new GcsStorageBackend[F](bucket)(blocker)
          }
    }
  }

  private[gcs] def composeBlobPath(sha256: Sha256): String = {
    val sha256Hex = sha256.toHexString
    String.join("/", sha256Hex.substring(0, 2), sha256Hex.substring(2, 4), sha256Hex.substring(4, 6), sha256Hex)
  }

  def prepareStorageClient[F[_]: Sync: ContextShift](conf: GcsBackendConfiguration,
                                                     blocker: Blocker): EitherT[F, ConfigurationException, Storage] = {
    EitherT {
      blocker.delay {
        Either
          .catchNonFatal {
            val builder = conf.jsonKeyPath match {
              case Some(jsonKeyPath) =>
                StorageOptions.newBuilder
                  .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(jsonKeyPath)))
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
  }

  def getBucket[F[_]: Sync: ContextShift](conf: GcsBackendConfiguration,
                                          storageClient: Storage,
                                          blocker: Blocker): EitherT[F, ConfigurationException, Bucket] = {
    EitherT {
      blocker
        .delay {
          Either
            .catchNonFatal {
              Option(storageClient.get(conf.bucketName, Storage.BucketGetOption.userProject(conf.projectId)))
            }
        }
        .map {
          _.leftMap { e =>
            ConfigurationException(s"Attempt to get bucket ${conf.bucketName} failed", e)
          }.flatMap {
            case Some(bucket) =>
              Right(bucket)
            case None =>
              Left {
                ConfigurationException(s"Bucket ${conf.bucketName} does not exist")
              }
          }
        }
    }
  }
}

case class GcsBackendConfiguration(projectId: String, bucketName: String, jsonKeyPath: Option[String] = None)

object GcsBackendConfiguration {
  // configure pureconfig:
  implicit val productHint: ProductHint[GcsBackendConfiguration] = ProductHint[GcsBackendConfiguration](
    fieldMapping = ConfigFieldMapping(CamelCase, CamelCase)
  )
}
