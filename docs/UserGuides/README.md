# Hangfire Go SDK User Guide

These guides show how to create, schedule, and process Hangfire background jobs from Go. They follow the structure of the official Hangfire documentation for [Background Methods](https://docs.hangfire.io/en/latest/background-methods/index.html) and [Background Processing](https://docs.hangfire.io/en/latest/background-processing/index.html). Each page covers the same topic for Go and lists what works differently from .NET Hangfire.

## How the SDK fits into Hangfire

Hangfire has no network API. Clients write jobs as rows into a shared SQL Server database, and servers poll that database for work. The Go SDK reads and writes the same rows as .NET Hangfire 1.8, so Go and .NET clients, servers, and the Hangfire Dashboard can share one database.

| Package | Role |
|---|---|
| `hangfire` | Job types, states, and options. Storage-independent |
| `sqlserver` | Producer client, recurring jobs, monitoring, and the SQL Server storage |
| `server` | Go server that runs Go handlers for Hangfire jobs |
| `hangfiretest` | In-memory fake client for unit tests |

A job names its target as a .NET type and method, such as `MyApp.Jobs.IEmailJobs, MyApp` and `Send`. Either a .NET server runs that method, or a Go server runs the handler registered under the same names.

## Background Methods

Creating jobs from Go.

- [Overview](background-methods/index.md)
- [Calling Methods in Background](background-methods/calling-methods-in-background.md)
- [Calling Methods with Delay](background-methods/calling-methods-with-delay.md)
- [Performing Recurrent Tasks](background-methods/performing-recurrent-tasks.md)
- [Passing Arguments](background-methods/passing-arguments.md)
- [Passing Dependencies](background-methods/passing-dependencies.md)
- [Using IoC Containers](background-methods/using-ioc-containers.md)
- [Using Cancellation Tokens](background-methods/using-cancellation-tokens.md)
- [Writing Unit Tests](background-methods/writing-unit-tests.md)
- [Using Batches](background-methods/using-batches.md)

## Background Processing

Running jobs in Go.

- [Overview](background-processing/index.md)
- [Processing Background Jobs](background-processing/processing-background-jobs.md)
- [Processing Jobs in a Web Application](background-processing/processing-jobs-in-web-app.md)
- [Processing Jobs in a Console Application](background-processing/processing-jobs-in-console-app.md)
- [Processing Jobs in a Windows Service](background-processing/processing-jobs-in-windows-service.md)
- [Dealing with Exceptions](background-processing/dealing-with-exceptions.md)
- [Tracking the Progress](background-processing/tracking-progress.md)
- [Configuring the Degree of Parallelism](background-processing/configuring-degree-of-parallelism.md)
- [Placing Processing into Another Process](background-processing/placing-processing-into-another-process.md)
- [Running Multiple Server Instances](background-processing/running-multiple-server-instances.md)
- [Configuring Job Queues](background-processing/configuring-job-queues.md)
- [Concurrency & Rate Limiting](background-processing/throttling.md)

## Related

- [Repository README](../../README.md): install, quick start, compatibility matrix.
- [Examples](../../examples/README.md): producer, worker, and a .NET app hosting the Hangfire Dashboard.
- [Software Requirements Specification](../SRS/golang-sdk-srs.md): the wire-format contract and its sources in the Hangfire code.
