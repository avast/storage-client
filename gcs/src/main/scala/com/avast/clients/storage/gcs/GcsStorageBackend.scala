package com.avast.clients.storage.gcs

import better.files.File
import cats.effect.implicits.catsEffectSyntaxBracket
import cats.effect.{Async, Blocker, ConcurrentEffect, ContextShift, Resource, Sync}
import cats.syntax.all._
import com.avast.clients.storage.gcs.GcsStorageBackend.composeObjectPath
import com.avast.clients.storage.{ConfigurationException, GetResult, HeadResult, StorageBackend, StorageException}
import com.avast.scala.hashes.Sha256
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{Blob, Bucket, Storage, StorageOptions, StorageRetryStrategy, StorageException => GcStorageException}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import pureconfig.error.ConfigReaderException
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping}

import java.io.FileInputStream
import java.nio.file.StandardOpenOption
import java.security.{DigestOutputStream, MessageDigest}

class GcsStorageBackend[F[_]: Sync: ContextShift](bucket: Bucket)(blocker: Blocker)(implicit F: Async[F])
    extends StorageBackend[F]
    with StrictLogging {

  override def head(sha256: Sha256): F[Either[StorageException, HeadResult]] = {
    logger.debug(s"Checking presence of file $sha256 in GCS")

    val objectPath: String = composeObjectPath(sha256)

    blocker
      .blockOn {
        F.delay {
          Either.right[StorageException, HeadResult] {
            Option {
              bucket.get(objectPath)
            } match {
              case Some(blob) =>
                HeadResult.Exists(blob.getSize)
              case None =>
                HeadResult.NotFound
            }
          }
        }
      }
      .handleError {
        case e: GcStorageException =>
          logger.error(s"Error while checking presence of file $sha256 in GCS", e)
          Either.left[StorageException, HeadResult] {
            StorageException.InvalidResponseException(e.getCode, e.getMessage, e.getReason)
          }
      }
  }

  override def get(sha256: Sha256, dest: File): F[Either[StorageException, GetResult]] = {
    logger.debug(s"Downloading file $sha256 from GCS")

    val objectPath: String = composeObjectPath(sha256)

    {
      Option {
        bucket.get(objectPath)
      } match {
        case Some(blob) =>
          receiveStreamedFile(blob, dest, sha256)
        case None =>
          F.pure[Either[StorageException, GetResult]](Right(GetResult.NotFound))
      }
    }.handleError {
      case e: GcStorageException =>
        logger.error(s"Error while downloading file $sha256 from GCS", e)
        Either.left[StorageException, GetResult] {
          StorageException.InvalidResponseException(e.getCode, e.getMessage, e.getReason)
        }
    }
  }

  private def receiveStreamedFile(blob: Blob, destination: File, expectedHash: Sha256): F[Either[StorageException, GetResult]] = {
    logger.debug(s"Downloading streamed data to $destination")

    val openOptions = Seq(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    blocker
      .blockOn {
        F.delay(destination.newOutputStream(openOptions))
          .bracket { fileStream =>
            F.delay(new DigestOutputStream(fileStream, MessageDigest.getInstance("SHA-256")))
              .bracket { stream =>
                F.delay(blob.downloadTo(stream)).map { _ =>
                  (blob.getSize, Sha256(stream.getMessageDigest.digest))
                }
              }(stream => F.delay(stream.close()))
          }(fileStream => F.delay(fileStream.close()))
      }
      .map {
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
  def fromConfig[F[_]: ConcurrentEffect: ContextShift](
      config: Config,
      blocker: Blocker): Either[ConfigurationException, Resource[F, GcsStorageBackend[F]]] = {

    def composeConfig: Either[ConfigurationException, GcsBackendConfiguration] = {
      val DefaultConfig = ConfigFactory.defaultReference().getConfig("gcsBackendDefaults")
      pureconfig.ConfigSource
        .fromConfig(config.withFallback(DefaultConfig))
        .load[GcsBackendConfiguration]
        .leftMap { failures =>
          ConfigurationException("Could not load config", new ConfigReaderException[GcsBackendConfiguration](failures))
        }
    }

    for {
      conf <- composeConfig
      storageClient <- prepareStorageClient(conf)
      bucket <- getBucket(conf, storageClient)
    } yield {
      Resource
        .fromAutoCloseable {
          Sync[F].pure(storageClient)
        }
        .map { _ =>
          new GcsStorageBackend[F](bucket)(blocker)
        }
    }
  }

  private[gcs] def composeObjectPath(sha256: Sha256): String = {
    val str = sha256.toString()
    String.join("/", str.substring(0, 2), str.substring(2, 4), str.substring(4, 6), str)
  }

  def prepareStorageClient(conf: GcsBackendConfiguration): Either[ConfigurationException, Storage] = {
    try {
      val builder = conf.jsonKeyPath match {
        case Some(jsonKeyPath) =>
          StorageOptions.newBuilder
            .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(jsonKeyPath)))
        case None =>
          StorageOptions.getDefaultInstance.toBuilder
      }

      builder
        .setProjectId(conf.projectId)
        .setStorageRetryStrategy(StorageRetryStrategy.getDefaultStorageRetryStrategy)

      Right(builder.build.getService)
    } catch {
      case e: GcStorageException =>
        Left {
          ConfigurationException("Could not create GCS client", e)
        }
    }
  }

  def getBucket(conf: GcsBackendConfiguration, storageClient: Storage): Either[ConfigurationException, Bucket] = {
    try {
      Option {
        storageClient.get(conf.bucketName, Storage.BucketGetOption.userProject(conf.projectId))
      } match {
        case Some(bucket) =>
          Right(bucket)
        case None =>
          Left {
            ConfigurationException(s"Bucket ${conf.bucketName} does not exist")
          }
      }
    } catch {
      case e: GcStorageException =>
        Left {
          ConfigurationException(s"Could not obtain bucket ${conf.bucketName}", e)
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
