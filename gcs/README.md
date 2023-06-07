# GCS (Google Cloud Storage) backend

## Dependency

```groovy
compile "com.avast.clients.storage:storage-client-gcs_2.13:x.x.x"
```

## Usage

### Configuration:

```hocon
projectId = "my-project-id"
bucketName = "bucket-name"
```

### Authentication

GCS backends supports multiple ways of authentication:
* Providing path to the credentials file in the configuration under `credentialsFile` key
* Using `GOOGLE_APPLICATION_CREDENTIALS_RAW` environment variable with the content of the credentials file
* All native ways of authentication provided by Google Cloud SDK
  * Using `GOOGLE_APPLICATION_CREDENTIALS` environment variable
  * Reading credential file from default paths (see https://cloud.google.com/docs/authentication/application-default-credentials#personal)
  * For all options see https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to 


### Client initialization

Example for `monix.eval.Task`:

```scala
import com.avast.clients.storage.gcs.GcsStorageBackend
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import cats.effect.Blocker

implicit val scheduler: Scheduler = ???
val blocker: Blocker = ???
val config: Config = ???

GcsStorageBackend.fromConfig[Task](config, blocker).map{ resource =>
  resource.use { client =>
    client.get(sha256, destinationFile)
  }
}
```