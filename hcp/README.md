# HCP REST backend

## Dependency

```groovy
compile "com.avast.clients.storage:storage-client-hcp_2.13:x.x.x"
```

## Usage

Configuration:

```hocon
namespace = "files"
tenant = "org"
repository = "hcp-repository-host.com"
```

Client init, example for `monix.eval.Task`:

```scala
import com.avast.clients.storage.hcp.HcpRestStorageBackend
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler

implicit val scheduler: Scheduler = ???
val config: Config = ???

def initClient: Task[HcpRestStorageBackend[Task]] = {
  HcpRestStorageBackend.fromConfig[Task](config) match {
    case Right(cl) => cl
    case Left(err) => Task.raiseError(err)
  }
}
```