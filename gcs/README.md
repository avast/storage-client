# GCS (Google Cloud Storage) backend

## Dependency

```groovy
compile "com.avast.clients.storage:storage-client-gcs_2.13:x.x.x"
```

## Usage

Configuration:

```hocon
projectId = "my-project-id"
bucketName = "bucket-name"
```

Client init, example for `monix.eval.Task`:

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