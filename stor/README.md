# Stor backend

Backend for querying [Stor](https://github.com/avast/stor).

## Dependency

```groovy
compile "com.avast.clients.storage:storage-client-stor_2.12:x.x.x"
```

## Usage

Configuration:

```hocon
uri = "my-stor.uri.com"
```

Client init, example for `monix.eval.Task`:

```scala
import com.avast.clients.storage.stor.StorBackend
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler

implicit val scheduler: Scheduler = ???
val config: Config = ???

def initClient: Task[StorBackend[Task]] = {
  StorBackend.fromConfig[Task](config) match {
    case Right(cl) => cl
    case Left(err) => Task.raiseError(err)
  }
}
```