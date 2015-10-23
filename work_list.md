# Work List

This document is intended to serve as inspiration for people who are looking to contribute to PikaDB.


## 1. Documentation

We need a full architecture guide for people who are interested in contributing to the project.

The initial specification for the DB API should be in the form of a fully written out user guide that demonstrates all features intended for the first release of the database.

A solid and fun sample application is needed both as a source of inspiration, but also to give people a more complete understanding on how the API is intended to be used.

The project needs a web site that can function both as a reference for existing users as well as a teaser / introduction for people who are curious about the project.

The project was named after the American Pika. While we need a solid simple branding - using the Pika as a mascot for the project will be a great way to make the website more lively and an excellent oppertunity for merchandising.

## 2. Refactoring

The current datamodel is Database > Soup > Column > Value. While the concept of a soup perfectly matches what we are trying to do, it is foreign and creates initial confusion to people interested in the project - just change it to Table.

The current jar file is not directly usable by an Android Studio project. There is not reason for this - figure out why this is a problem and fixe the project to build artifacts that can be instantly used for both Android and general Java projects.

There is currently a half assed implementation of sorted pages - make it work in general and make non sortable columns be something that can be configured using the database declaration API.

## 3. Storage Engine

There is a current pull request for a simple LRU based page replacement algorithm - extend this to include a LRU 2Q algorithm.

The current implementation can fail if it is stopped in the middle of a write. The plan for this is to implement a write ahead log with checkpoints. 

There are serious issues with page overflows - this is a high priority issue to debug!

Add support for null values, timestamps and bignums!

Explore value type promotion... that is, support byte > short > int > long > bignum. The check to decide upon type should be fairly quick using bitmasks - but still, this is something we should micro benchmark thoroughly before doing.

Current columns allow any value type to be stored in them. Implement optional pr column constraints.

## 4. Query Engine

While some uses of the database might use the current options to load either a single object by id or all object, we need support for a full predicate tree and a good api for constructing it that will allow us to experiment with different query languages on top of it.

Take a predicate tree and use it to extract a result set from the database.

A beautiful and easy to use user level api is the key to any success with this project. It should be easy to implement on top of a predicate engine and an execution engine based on a predicate tree - but the design of it will be anything but easy.

Add a query planner that considers Cardinality / Indexes / Stats / to create intelligent query execution plans

Implement indexes that work well with our paging strategy - but don't hesitate to suggest changes to the page architecture to support faster indexes!

## 5. Object Mapping

The current prototype has some JSON input support - we need complete JSON in/out for server side API integrations.

We need a way to map result sets to plain java objects - how do we track changes?

We can easily implement fine grained observer pattern notifications in the storage engine - find a good way to make it easy and convenient to tie user interfaces up to it.

## 6. Benchmarks

Benchmarking will be essential for pushing PikaDB into the world... we don't have to be the fastest, but we need to prove that we are a realistic option. We have the luxury of having an actual app that we can easily model data sets after. Create a synthetic data test set based on our actual app data for a big event.

Benchmark based on actual application behavior.

Alternative implementations of test using sqlite, realm and so forth.

Benchmark for both time and memory usage.