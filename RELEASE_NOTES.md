Release Notes
---------------
###Version 0.4-SNAPSHOT

###Version 0.3 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.3%7Cjar))
* add ```Database.run``` overload with ```Charset``` parameter
* [pull 10](https://github.com/davidmoten/rxjava-jdbc/pull/10) Add backpressure suppport for ```QuerySelectOperator```
* upgrade rxjava dependency to 0.20.0 which includes backpressure

###Version 0.2 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.2%7Cjar))
* upgrade rxjava dependency to 0.19.6
* use lazy evaluation in logging
* [pull 4] (https://github.com/davidmoten/rxjava-jdbc/pull/4) Add test scope to mockito dependency
* [pull 6] (https://github.com/davidmoten/rxjava-jdbc/pull/6) add support for null insert/update of clobs and blobs

###Version 0.1.3 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.1.3%7Cjar))
* upgrade rxjava dependency to 0.17.6 (retry operator was broken in 0.17.5)
* upgrade c3p0, h2, slf4j dependencies to latest
* change ```Database.Builder``` method for specifiying connection pool
* add username and password parameters to ```Database.Builder```, ```Database.from()```

###Version 0.1.2 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.1.2%7Cjar))
* [issue #1](https://github.com/davidmoten/rxjava-jdbc/issues/1) use rxjava 0.17.4
* add ```Database.fromContext(jndiResource)``` for JNDI lookup of DataSource 
* [issue #2](https://github.com/davidmoten/rxjava-jdbc/issues/2) queries synchronous by default (scheduled using ```Schedulers.trampoline()```)
* [pull 3](https://github.com/davidmoten/rxjava-jdbc/pull/3) run all database tests sync and async 

###Version 0.1.1 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.1.1%7Cjar))
* replaced use of ```flatMap``` with ```concatMap``` to limit possible async side effects on specifying parameter observables to queries

###Version 0.1 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.1%7Cjar))
* initial release
