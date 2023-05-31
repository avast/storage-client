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

implicit val scheduler: Scheduler = ???
val config: Config = ???

def initClient: Task[GcsStorageBackend[Task]] = {
  GcsStorageBackend.fromConfig[Task](config) match {
    case Right(cl) => cl
    case Left(err) => Task.raiseError(err)
  }
}
```