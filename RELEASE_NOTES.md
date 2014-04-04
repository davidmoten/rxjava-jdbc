Release Notes
---------------
###Version 0.1.2-SNAPSHOT
* [Issue #1](https://github.com/davidmoten/rxjava-jdbc/issues/1) use rxjava 0.17.4
* add ```Database.fromContext(jndiResource)``` for JNDI lookup of DataSource 
* [Issue #2](https://github.com/davidmoten/rxjava-jdbc/issues/2) queries synchronous by default (scheduled using ```Schedulers.trampoline()```)

###Version 0.1.1 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.1.1%7Cjar))
* replaced use of flatMap with concatMap to limit possible async side effects on specifying parameter observables to queries

###Version 0.1 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-jdbc%7C0.1%7Cjar))
* initial release
