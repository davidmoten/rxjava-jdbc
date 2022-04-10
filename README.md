rxjava-jdbc
============
<a href="https://github.com/davidmoten/rxjava-jdbc/actions/workflows/ci.yml"><img src="https://github.com/davidmoten/rxjava-jdbc/actions/workflows/ci.yml/badge.svg"/></a><br/>
<a href="https://scan.coverity.com/projects/4834"><img src="https://scan.coverity.com/projects/4834/badge.svg?flat=1"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava-jdbc/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava-jdbc)

Efficient execution, concise code, and functional composition of database calls
using JDBC and [RxJava](https://github.com/Netflix/RxJava/wiki) [Observable](http://netflix.github.io/RxJava/javadoc/rx/Observable.html). 

Status: Released to Maven Central

See also [rxjava2-jdbc](https://github.com/davidmoten/rxjava2-jdbc) for RxJava 2.x with non-blocking connection pools!

[Release Notes](https://github.com/davidmoten/rxjava-jdbc/releases)

Features
--------------------------
* Functionally compose database queries run sequentially or in parallel 
* Queries may be only partially run or indeed never run due to subscription cancellations thus improving efficiency
* Concise code
* Queries can depend on completion of other Observables and can be supplied parameters through Observables.
* Method chaining just leads the way (once you are on top of the RxJava api of course!)
* All the RxJava goodness!
* Automatically maps query result rows into typed tuples or your own classes
* CLOB and BLOB handling is simplified greatly

Maven site reports are [here](http://davidmoten.github.io/rxjava-jdbc/index.html) including [javadoc](http://davidmoten.github.io/rxjava-jdbc/apidocs/index.html).

Table of Contents
------------------------

- [rxjava-jdbc](#)
	- [Features](#)
	- [Todo](#todo)
	- [Build instructions](#build-instructions)
	- [Getting started](#getting-started)
	- [Query types](#query-types)
	- [Functional composition of JDBC calls](#functional-composition-of-jdbc-calls)
	- [About toBlocking](#about-toblocking)
	- [Dependencies](#dependencies)
	- [Mixing explicit and Observable parameters](#mixing-explicit-and-observable-parameters)
	- [Passing multiple parameter sets to a query](#passing-multiple-parameter-sets-to-a-query)
	- [Named parameters](#named-parameters)
	- [Processing a ResultSet](#processing-a-resultset)
	- [Mapping](#mapping)
	- [Explicit mapping](#explicit-mapping)
	- [Automap](#automap)
		- [Automap using an interface](#automap-using-an-interface)
		- [Automap using a concrete class](#automap-using-a-concrete-class)
	- [Auto mappings](#auto-mappings)
	- [Tuples](#tuples)
		- [Tuple2](#tuple2)
		- [TupleN](#tuplen)
	- [Returning generated keys](#returning-generated-keys)
	- [Large objects support](#large-objects-support)
		- [Insert a Clob](#insert-a-clob)
		- [Insert a Null Clob](#insert-a-null-clob)
		- [Read a Clob](#read-a-clob)
		- [Insert a Blob](#insert-a-clob)
		- [Insert a Null Blob](#insert-a-null-blob)
		- [Read a Blob](#read-a-blob)
	- [Compose](#compose)
	- [Transactions](#transactions)
		- [Transactions as dependency](#transactions-as-dependency)
		- [onNext Transactions](#onNext-transactions)
	- [Asynchronous queries](#asynchronous-queries)
	- [Backpressure](#backpressure)
	- [Logging](#logging)
	- [Database Connection Pools](#database-connection-pools)
	- [Using a custom connection pool](#using-a-custom-connection-pool)
	- [Use a single Connection](#use-a-single-connection)
	- [Note for SQLite Users](#note-for-sqlite-users)

Todo
------------
* Callable statements

Build instructions
-------------------
```
git clone https://github.com/davidmoten/rxjava-jdbc.git
cd rxjava-jdbc
mvn clean install
```

Getting started
--------------------
Include this maven dependency in your pom (available in Maven Central):
```xml
<dependency>
    <groupId>com.github.davidmoten</groupId>
    <artifactId>rxjava-jdbc</artifactId>
    <version>VERSION_HERE</version>
</dependency>
```

After using [RxJava](https://github.com/Netflix/RxJava/wiki) on a work project and being very impressed with 
it (even without Java 8 lambdas!), I wondered what it could offer for JDBC usage. The answer is lots!

Here's a simple example:
```java
Database db = Database.from(url);
List<String> names = db
		.select("select name from person where name > ? order by name")
		.parameter("ALEX")
		.getAs(String.class)
		.toList().toBlocking().single();
System.out.println(names);
```
output:
```
[FRED, JOSEPH, MARMADUKE]
```
Without using rxjava-jdbc:
```java
String sql = "select name from person where name > ? order by name";
try (Connection con = nextConnection();
     PreparedStatement ps = con.prepareStatement(sql);) {
    ps.setObject(1, "ALEX");
    List<String> list = new ArrayList<String>();
    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
            list.add(rs.getString(1));
        }
    }
    System.out.println(list);
} catch (SQLException e) {
    throw new RuntimeException(e);
}
```

Query types
------------------
The ```Database.select()``` method is used for 
* SQL select queries. 

The ```Database.update()``` method is used for
* update
* insert
* delete
* DDL (like *create table*, etc)

Examples of all of the above methods are found in the sections below. 

Functional composition of JDBC calls
-----------------------------------------
Here's an example, wonderfully brief compared to normal JDBC usage:
 
```java
import com.github.davidmoten.rx.jdbc.Database;
import rx.Observable;

// use composition to find the first person alphabetically with
// a score less than the person with the last name alphabetically
// whose name is not XAVIER. Two threads and connections will be used.

Database db = new Database(connectionProvider);
Observable<Integer> score = db
		.select("select score from person where name <> ? order by name")
		.parameter("XAVIER")
		.getAs(Integer.class)
		.last();
String name = db
		.select("select name from person where score < ? order by name")
		.parameters(score)
		.getAs(String.class)
		.first()
		.toBlocking().single();
assertEquals("FRED", name);
```
or alternatively using the ```Observable.compose()``` method to chain everything in one command:
```java
String name = db
    .select("select score from person where name <> ? order by name")
    .parameter("XAVIER")
    .getAs(Integer.class)
    .last()
    .compose(db.select("select name from person where score < ? order by name")
            .parameterTransformer()
            .getAs(String.class))
    .first()
    .toBlocking().single();
```

About toBlocking
----------------------------
You'll see ```toBlocking()``` used in the examples in this page and in 
the unit tests but in your application code you should try to avoid using it. The most benefit 
from the reactive style is obtained by *not leaving the monad*. That is, stay in Observable land and make 
the most of it. Chain everything together and leave ```toBlocking``` to 
an endpoint or better still just subscribe with a ```Subscriber```.

Dependencies
--------------
You can setup chains of dependencies that will determine the order of running of queries. 

To indicate that a query cannot be run before one or more other Observables
have been completed use the `dependsOn()` method. Here's an example:
```java
Observable<Integer> insert = db
		.update("insert into person(name,score) values(?,?)")
		.parameters("JOHN", 45)
		.count()
		.map(Util.<Integer> delay(500));
int count = db
		.select("select name from person")
		.dependsOn(insert)
		.get()
		.count()
		.toBlocking().single();
assertEquals(4, count);
```

Note that when you pass the output of a query as a parameter to another query there is an implicit dependency established.

Mixing explicit and Observable parameters
------------------------------------------
Example:
```java
String name= db
	.select("select name from person where name > ?  and score < ? order by name")
	.parameter("BARRY")
	.parameters(Observable.just(100))
	.getAs(String.class)
	.first()
	.toBlocking().single();
assertEquals("FRED",name);
```

Passing multiple parameter sets to a query
--------------------------------------------
Given a sequence of parameters, each chunk of parameters will be run with the query and the results appended. 
In the example below there is only one parameter in the sql statement yet two parameters are specified.
This causes the statement to be run twice.

```java
List<Integer> list = 
	db.select("select score from person where name=?")
	    .parameter("FRED").parameter("JOSEPH")
		.getAs(Integer.class).toList().toBlocking().single();
assertEquals(Arrays.asList(21,34),list);
```

Named parameters
----------------------------
Examples:

```java
Observable<String> names = db
    .select("select name from person where score >= :min and score <=:max")
    .parameter("min", 24)
    .parameter("max", 26)
    .getAs(String.class);
```

Using a map of parameters:
```java
Map<String, Integer> map = new HashMap<String, Integer>();
map.put("min", 24);
map.put("max", 26);
Observable<String> names = db
    .select("select name from person where score >= :min and score <=:max")
    .parameters(map)
    .getAs(String.class);
```

Using an Observable of maps:
```java
Observable<String> names = db
    .select("select name from person where score >= :min and score <=:max")
    .parameters(Observable.just(map1, map2))
    .getAs(String.class);
```

Processing a ResultSet
-----------------------------
Many operators in rxjava process items pushed to them asynchronously. Given this it is important that ```ResultSet``` query results are processed 
before being emitted to a consuming operator. This means that the select query needs to be passed a function that converts a ```ResultSet``` to
a result that does not depend on an open ```java.sql.Connection```. Use the ```get()```, ```getAs()```, ```getTuple?()```, and ```autoMap()``` methods 
to specify this function as below.

```java
Observable<Integer> scores = db.select("select score from person where name=?")
	    .parameter("FRED")
		.getAs(Integer.class);
```

Mapping
-----------------
A common requirement is to map the rows of a ResultSet to an object. There are two main options: explicit mapping and automap.

Explicit mapping
----------------------
Using `get` you can map the `ResultSet` as you wish:

```java
db.select("select name, score from person")
  .get( rs -> new Person(rs.getString(1), rs.getInt(2)));
```

Automap
------------------------------
`automap` does more for you than explicit mapping. You can provide just an annotated interface and objects will be created that implement that interface and types will be converted for you (See *Auto mappings* section below).

There is some reflection overhead with using auto mapping. Use your own benchmarks to determine if its important to you (the reflection overhead may not be significant compared to the network latencies involved in database calls).

The `autoMap` method maps result set rows to instances of the class you nominate. 

If you nominate an interface then dynamic proxies (a java reflection feature) are used to build instances. 

If you nominate a concrete class then the columns of the result set are mapped to parameters in the constructor (again using reflection).

### Automap using an interface

Create an annotated interface (introduced in *rxjava-jdbc* 0.5.8):

```java
public interface Person {

    @Column("name")
    String name();

    @Column("score")
    int score();
}
``` 

Then run
```java
Observable<Person> persons = db
                 .select("select name, score from person order by name")
                 .autoMap(Person.class);
```
Easy eh!

An alternative is to annotate the interface with the indexes of the columns in the result set row:

```java
public interface Person {

    @Index(1)
    String name();

    @Index(2)
    int score();
}
```

Camel cased method names will be converted to underscore by default (since 0.5.11):

```java
public interface Address {

    @Column // maps to address_id 
    int addressId();

    @Column // maps to full_address
    String fullAddress();
}
```
 
You can also specify the sql to be run in the annotation:

```java
@Query("select name, score from person order by name")
public interface Person {

    @Column
    String name();

    @Column
    int score();
}
``` 
Then run like this:
```java
Observable<Person> persons = db
                 .select().autoMap(Person.class);
```


### Automap using a concrete class 

Given this class:
```java
static class Person {
	private final String name;
	private final double score;
	private final Long dateOfBirth;
	private final Long registered;

	Person(String name, Double score, Long dateOfBirth,
			Long registered) {
			...
```
Then run
```java
Observable<Person> persons = db
				.select("select name,score,dob,registered from person order by name")
				.autoMap(Person.class);
```
The main requirement is that the number of columns in the select statement must match 
the number of columns in a constructor of ```Person``` and that the column types can be 
automatically mapped to the types in the constructor.

Auto mappings
------------------
The automatic mappings below of objects are used in the ```autoMap()``` method and for typed ```getAs()``` calls.
* ```java.sql.Date```,```java.sql.Time```,```java.sql.Timestamp``` <==> ```java.util.Date```
* ```java.sql.Date```,```java.sql.Time```,```java.sql.Timestamp```  ==> ```java.lang.Long```
* ```java.sql.Blob``` <==> ```java.io.InputStream```, ```byte[]```
* ```java.sql.Clob``` <==> ```java.io.Reader```, ```String```
* ```java.math.BigInteger``` ==> ```Long```, ```Integer```, ```Decimal```, ```Float```, ```Short```, ```java.math.BigDecimal```
* ```java.math.BigDecimal``` ==> ```Long```, ```Integer```, ```Decimal```, ```Float```, ```Short```, ```java.math.BigInteger```

Note that automappings do not occur to primitives so use ```Long``` instead of ```long```.

Tuples
---------------
Typed tuples can be returned in an ```Observable```:
### Tuple2
```java
Tuple2<String, Integer> tuple = db
		.select("select name,score from person where name >? order by name")
		.parameter("ALEX").create()
		.getAs(String.class, Integer.class).last()
		.toBlocking().single();
assertEquals("MARMADUKE", tuple.value1());
assertEquals(25, (int) tuple.value2());
```
Similarly for ```Tuple3```, ```Tuple4```, ```Tuple5```, ```Tuple6```, ```Tuple7```, and finally 
### TupleN
```java
TupleN<String> tuple = db
		.select("select name, lower(name) from person order by name")
		.create()
		.getTupleN(String.class).first()
		.toBlocking().single();
assertEquals("FRED", tuple.values().get(0));
assertEquals("fred", tuple.values().get(1));
```

Returning generated keys
-------------------------
If you insert into a table that say in h2 is of type `auto_increment` then you don't need to specify a value but you may want to know what value was inserted in the generated key field.

Given a table like this
```
create table note(
    id bigint auto_increment primary key,
    text varchar(255)
)
```
This code inserts two rows into the *note* table and returns the two generated keys:

```java
Observable<Integer> keys = 
    db.update("insert into note(text) values(?)")
      .parameter("hello", "there")
      .returnGeneratedKeys()
      .getAs(Integer.class);
```

The `returnGeneratedKeys` method also supports returning multiple keys per row so the builder offers methods just like `select` to do explicit mapping or auto mapping.

Large objects support
------------------------------
Blob and Clobs are straightforward to handle.

### Insert a Clob
Here's how to insert a String value into a Clob (*document* column below is of type ```CLOB```):
```java
String document = ...
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.toSentinelIfNull(document)).count();
```
(Note the use of the ```Database.toSentinelIfNull(String)``` method to handle the null case correctly)

or using a ```java.io.Reader```:
```java
Reader reader = ...;
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(reader).count();
```
### Insert a Null Clob
This requires *either* a special call (```parameterClob(String)```) to identify the parameter as a CLOB:
```java
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameterClob(null).count();
```
or use the null Sentinel object for Clobs:
```java
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.NULL_CLOB).count();
```
or wrap the String parameter with ```Database.toSentinelIfNull(String)``` as above in the Insert a Clob section.

### Read a Clob
```java
Observable<String> document = db.select("select document from person_clob")
				.getAs(String.class);
```
or
```java
Observable<Reader> document = db.select("select document from person_clob")
				.getAs(Reader.class);
```
### Insert a Blob
Similarly for Blobs (*document* column below is of type ```BLOB```):
```java
byte[] bytes = ...
Observable<Integer> count = db
		.update("insert into person_blob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.toSentinelIfNull(bytes)).count();
```
### Insert a Null Blob
This requires *either* a special call (```parameterBlob(String)``` to identify the parameter as a CLOB:
```java
Observable<Integer> count = db
		.update("insert into person_blob(name,document) values(?,?)")
		.parameter("FRED")
		.parameterBlob(null).count();
```
or use the null Sentinel object for Blobs:
```java
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.NULL_BLOB).count();
```
or wrap the byte[] parameter with ```Database.toSentinelIfNull(byte[])``` as above in the Insert a Blob section.

### Read a Blob
```java
Observable<byte[]> document = db.select("select document from person_clob")
				.getAs(byte[].class);
```
or
```java
Observable<InputStream> document = db.select("select document from person_clob")
				.getAs(InputStream.class);
```

Compose
-----------------------------------

Using the ```Observable.compose()``` method you can perform multiple queries without breaking method chaining. ```Observable.compose()``` 
requires a ```Transformer``` parameter which are available via 

* ```db.select(sql).parameterTransformer().getXXX()```
* ```db.select(sql).parameterListTransformer().getXXX()```
* ```db.select(sql).dependsOnTransformer().getXXX()```
* ```db.update(sql).parameterTransformer()```
* ```db.update(sql).parameterListTransformer()```
* ```db.update(sql).dependsOnTransformer()```

Example:   
```java
Observable<Integer> score = Observable
    // parameters for coming update
    .just(4, "FRED")
    // update Fred's score to 4
    .compose(db.update("update person set score=? where name=?")
            //parameters are pushed
            .parameterTransformer())
    // update everyone with score of 4 to 14
    .compose(db.update("update person set score=? where score=?")
            .parameters(14, 4)
            //wait for completion of previous observable
            .dependsOnTransformer())
    // get Fred's score
    .compose(db.select("select score from person where name=?")
            .parameters("FRED")
            //wait for completion of previous observable
            .dependsOnTransformer()
			.getAs(Integer.class));
```

Note that conditional evaluation of a query is obtained using 
the ```parameterTransformer()``` method (no parameters means no query run) 
whereas using ```dependsOnTransformer()``` just waits for the 
dependency to complete and ignores how many items the dependency emits.  

If the query does not require parameters you can push it an empty list 
and use the ```parameterListTransformer()``` to force execution.

Example:
```java
Observable<Integer> rowsAffected = Observable
    //generate two integers
    .range(1,2)
    //replace the integers with empty observables
    .map(toEmpty())
    //execute the update twice with an empty list
    .compose(db.update("update person set score = score + 1")
            .parameterListTransformer())
    // flatten
    .compose(RxUtil.<Integer> flatten())
    // total the affected records
    .compose(SUM_INTEGER);
```

Transactions
------------------
When you want a statement to participate in a transaction then either it should
* depend on ```db.beginTransaction()``` 
* be passed parameters or dependencies through ```db.beginTransactionOnNext()```

### Transactions as dependency
```java
Observable<Boolean> begin = db.beginTransaction();
Observable<Integer> updateCount = db
    // set everyones score to 99
    .update("update person set score=?")
    // is within transaction
    .dependsOn(begin)
    // new score
    .parameter(99)
    // execute
    .count();
Observable<Boolean> commit = db.commit(updateCount);
long count = db
    .select("select count(*) from person where score=?")
	// set score
	.parameter(99)
	// depends on
	.dependsOn(commit)
	// return as Long
	.getAs(Long.class)
	// log
	.doOnEach(RxUtil.log())
	// get answer
	.toBlocking().single();
assertEquals(3, count);
```

### onNext Transactions
```java
List<Integer> mins = Observable
    // do 3 times
    .just(11, 12, 13)
    // begin transaction for each item
    .compose(db.beginTransactionOnNext_())
    // update all scores to the item
    .compose(db.update("update person set score=?").parameterTransformer())
    // to empty parameter list
    .map(toEmpty())
    // increase score
    .compose(db.update("update person set score=score + 5").parameterListTransformer())
    //only expect one result so can flatten
    .compose(RxUtil.<Integer>flatten())
    // commit transaction
    .compose(db.commitOnNext_())
    // to empty lists
    .map(toEmpty())
    // return count
    .compose(db.select("select min(score) from person").parameterListTransformer()
            .getAs(Integer.class))
    // list the results
    .toList()
    // block and get
    .toBlocking().single();
assertEquals(Arrays.asList(16, 17, 18), mins);
```

Note that for each ```commit*``` method there is an corresponding ```rollback``` method as well.

Asynchronous queries
--------------------------
Unless run within a transaction all queries are synchronous by default. However, if you request an asynchronous 
version of the database using ```Database.asynchronous()``` or if you use asynchronous Transformers then watch out because this means that 
something like the code below could produce unpredictable results:

```java
Database adb = db.asynchronous();
Observable
    .just(1, 2, 3)
    .compose(adb.update("update person set score = ?")
            .parameterTransformer());
```
After running this code you have no guarantee that the *update person set score=1* ran before the *update person set score=2*. 
To run those queries synchronously either use a transaction:

```java
Database adb = db.asynchronous();
Observable
   .just(1, 2, 3)
   .compose(adb.update("update person set score = ?")
           .dependsOn(db.beginTransaction())
           .parameterTransformer())
    .compose(adb.commitOnComplete_());
```

or use the default version of the ```Database``` object that schedules queries using ```Schedulers.trampoline()```.

```java
Observable.just(1, 2, 3)
          .compose(db.update("update person set score = ?")
                  .parameterTransformer());
```

Backpressure
-----------------
```Database.select``` supports reactive pull backpressure as introduced in RxJava 0.20.0. This means that the pushing of items from the results of a query can be optionally slowed down by the operators downstream to assist in preventing out of memory exceptions or thread starvation. 

Logging
-----------------
Logging is handled by slf4j which bridges to the logging framework of your choice. Add
the dependency for your logging framework as a maven dependency and you are sorted. See the test scoped log4j example in [rxjava-jdbc/pom.xml](https://github.com/davidmoten/rxjava-jdbc/blob/master/pom.xml).

Database Connection Pools
----------------------------
Include the dependency below:
```xml
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP-java6</artifactId>
    <version>2.3.2</version>
</dependency>
```
and you can use a [Hikari](https://github.com/brettwooldridge/HikariCP) database connection pool like so:
```java
Database db = Database.builder().url(url).pool(minPoolSize,maxPoolSize).build();
```
Once finished with a ``Database`` that has used a connection pool you should call 
```java
db.close();
```
This will close the connection pool and  release its resources.

Using a custom connection pool
---------------------------------
If Hikari doesn't suit you or you have container imposed constraints this is how you can use a different connection pool. 

Write an implmentation of the ```ConnectionProvider``` interface (two methods, ```getConnection()``` and ```close()```) and use it like so:

```java
ConnectionProvider cp = new CustomConnectionProvider();
Database db = Database.builder().connectionProvider(cp).build();
```

This method could be used to supply a JNDI datasource for example.

Use a single Connection
----------------------------
A ```Database``` can be instantiated from a single ```java.sql.Connection``` which will 
be used for all queries in companion with the current thread ```Scheduler``` (```Schedulers.trampoline()```).
The connection is wrapped in a ```ConnectionNonClosing``` which suppresses close calls so that the connection will
 still be open for all queries and will remain open after use of the ```Database``` object.
 
 Example:
 ```java
 Database db = Database.from(con);
 ```

Fetch Size
----------------------------
The `fetch size` setting in [statements](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#setFetchSize(int))
allows to specify how many rows should be fetched from the database at once. In other words, instead of fetching all
data in the `ResultSet` at once, potentially consuming a lot of memory in the heap, the `fetch size` setting
allows to trade time, due to multiple round-trips to the database, in exchange for lower memory consumption.

Example:
```java
db
    .select("select * from person")
    // set fetch size
    .fetchSize(10)
    //
    .autoMap(Person.class)
    // 
    .take(20)
	// log
	.doOnEach(RxUtil.log());
```
In this case, the JDBC driver will do two round trips, each time fetching 10 rows and transforming each row to an instance
of `Person`. 

Note for SQLite Users
----------------------------
*rxjava-jdbc* does support [SQLite](http://sqlite.org/). But due to the [SQLite architecture](http://sqlite.org/faq.html#q5) there are limitations particularly with write operations (CREATE, INSERT, UPDATE, DELETE). If your application has any write operations, [use a single connection](#use-a-single-connection). If a source ```Observable``` pushes emissions through a series of database read/write operations, always collect emissions and flatten them between each database read/write operation. This will prevent a [SQLITE_INTERRUPT](https://sqlite.org/rescode.html#interrupt) exception by never having more than one query open at a time. 

```java
Observable<MyItem> = Observable.just(itemstoInsert)
		.compose(executeInsertsAndGetKeys())
		.toList().concatMap(Observable::from)
		.compose(selectAndAutoMap());
```
