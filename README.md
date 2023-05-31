# Scala storage client [![Build](https://github.com/avast/storage-client/actions/workflows/build.yml/badge.svg)](https://github.com/avast/storage-client/actions/workflows/build.yml) [![Version](https://badgen.net/maven/v/maven-central/com.avast.clients.storage/storage-client-core_2.13/)](https://repo1.maven.org/maven2/com/avast/clients/storage/storage-client-core_2.13/)

Finally-tagless implementation of client for misc. storages represented by backends. Supports backends fallbacks.

Currently supported backends:
1. [HCP (Hitachi Content Platform)](hcp/README.md)
2. [GCS (Google Cloud Storage)](gcs/README.md)

## Dependency

Use dependency from README of selected backend:

```groovy
compile "com.avast.clients.storage:storage-client-BACKEND_2.13:x.x.x"
```

## Usage

The library has two levels:
1. Storage client which contains so called backend
1. Storage backend which is actual storage connector

The client has only single backend inside however backends are combinable to provide fallback support:

```scala
val backend1: StorageBackend[F] = ???
val backend2: StorageBackend[F] = ???

val merged: StorageBackend[F] = backend1 withFallbackTo backend2
```

### Retries etc.

Retries and similar functionality is not supported out-of-the-box by the library but you can easily implement them by your own by using some
`F[_]` which is capable of doing it and is familiar to you.

Example for `monix.eval.Task`:

```scala
import better.files.File
import com.avast.clients.storage.{GetResult, HeadResult, StorageBackend, StorageException}
import com.avast.scala.hashes.Sha256
import monix.eval.Task
val backend: StorageBackend[Task] = ???

val retried = new StorageBackend[Task] {
  override def head(sha256: Sha256): Task[Either[StorageException, HeadResult]] = backend.head(sha256).onErrorRestart(3)
  
  override def get(sha256: Sha256, dest: File): Task[Either[StorageException, GetResult]] = backend.get(sha256, dest).onErrorRestart(3)
  
  override def close(): Unit = backend.close()
}
```

### Example

Example usage for `monix.eval.Task`:

```scala
import com.avast.clients.storage.{StorageBackend, StorageClient}
import monix.eval.Task
import monix.execution.Scheduler

implicit val scheduler: Scheduler = ???
val backend: StorageBackend[Task] = ???

val client = StorageClient(backend)
```
