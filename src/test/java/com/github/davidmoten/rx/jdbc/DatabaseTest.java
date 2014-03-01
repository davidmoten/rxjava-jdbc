package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.DatabaseCreator.connectionProvider;
import static com.github.davidmoten.rx.jdbc.DatabaseCreator.createDatabase;
import static com.github.davidmoten.rx.jdbc.DatabaseCreator.db;
import static com.github.davidmoten.rx.jdbc.DatabaseCreator.nextUrl;
import static com.github.davidmoten.rx.jdbc.Util.log;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static rx.Observable.from;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

import com.github.davidmoten.rx.jdbc.tuple.Tuple2;
import com.github.davidmoten.rx.jdbc.tuple.Tuple3;
import com.github.davidmoten.rx.jdbc.tuple.Tuple4;
import com.github.davidmoten.rx.jdbc.tuple.Tuple5;
import com.github.davidmoten.rx.jdbc.tuple.Tuple6;
import com.github.davidmoten.rx.jdbc.tuple.Tuple7;
import com.github.davidmoten.rx.jdbc.tuple.TupleN;

public class DatabaseTest {

	private static final Logger log = LoggerFactory
			.getLogger(DatabaseTest.class);

	@Test
	public void testOldStyle() {
		Connection con = connectionProvider().get();
		createDatabase(con);
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement("select name from person where name > ?");
			ps.setObject(1, "ALEX");
			rs = ps.executeQuery();
			List<String> list = new ArrayList<String>();
			while (rs.next()) {
				list.add(rs.getString(1));
			}
			assertEquals(asList("FRED", "JOSEPH", "MARMADUKE"), list);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
				}
			if (ps != null)
				try {
					ps.close();
				} catch (SQLException e) {
				}
			try {
				con.close();
			} catch (SQLException e) {
			}
		}

	}

	@Test
	public void testSimpleExample() {
		Observable<String> names = db().select(
				"select name from person order by name").getAs(String.class);
		// convert the names to a list for unit test
		List<String> list = names.toList().toBlockingObservable().single();
		log.debug("list=" + list);
		assertEquals(asList("FRED", "JOSEPH", "MARMADUKE"), list);
	}

	@Test
	public void testCountQuery() {
		int count = db().select("select name from person where name >?")
				.parameter("ALEX").get().count().first().toBlockingObservable()
				.single();
		assertEquals(3, count);
	}

	@Test
	public void testTransactionUsingCount() {
		Database db = db();
		Func1<? super Integer, Boolean> isZero = new Func1<Integer, Boolean>() {
			@Override
			public Boolean call(Integer t1) {
				return t1 == 0;
			}
		};
		db.beginTransaction();
		Observable<Integer> existingRows = db
				.select("select name from person where name=?")
				.parameter("FRED").get().count().filter(isZero);
		db.update("insert into person(name,score) values(?,0)")
				.parameters(existingRows.map(Util.constant("FRED"))).count();
		boolean committed = db.commit().toBlockingObservable().single();
		assertTrue(committed);
	}

	@Test
	public void testTransactionOnCommit() {
		Database db = db().beginTransaction();
		Observable<Integer> updateCount = db
				.update("update person set score=?").parameter(99).count();
		db.commit(updateCount);
		long count = db.select("select count(*) from person where score=?")
				.parameter(99).dependsOnLastTransaction().getAs(Long.class)
				.toBlockingObservable().single();
		assertEquals(3, count);
	}

	@Test
	public void testTransactionOnCommitDoesntOccurUnlessSubscribedTo() {
		Database db = db();
		db.beginTransaction();
		Observable<Integer> u = db.update("update person set score=?")
				.parameter(99).count();
		db.commit(u);
		// note that last transaction was not listed as a dependency of the next
		// query
		long count = db.select("select count(*) from person where score=?")
				.parameter(99).getAs(Long.class).toBlockingObservable()
				.single();
		assertEquals(0, count);
	}

	@Test
	public void testTransactionOnRollback() {
		Database db = db();
		db.beginTransaction();
		Observable<Integer> updateCount = db
				.update("update person set score=?").parameter(99).count();
		db.rollback(updateCount);
		long count = db.select("select count(*) from person where score=?")
				.parameter(99).dependsOnLastTransaction().getAs(Long.class)
				.toBlockingObservable().single();
		assertEquals(0, count);
	}

	@Test
	public void testUseParameterObservable() {
		int count = db().select("select name from person where name >?")
				.parameters(Observable.from("ALEX")).get().count().first()
				.toBlockingObservable().single();
		assertEquals(3, count);
	}

	@Test
	public void testTwoParameters() {
		List<String> list = db()
				.select("select name from person where name > ? and name < ?")
				.parameter("ALEX").parameter("LOUIS").getAs(String.class)
				.toList().toBlockingObservable().single();
		assertEquals(asList("FRED", "JOSEPH"), list);
	}

	@Test
	public void testTakeFewerThanAvailable() {
		int count = db().select("select name from person where name >?")
				.parameter("ALEX").get().take(2).count().first()
				.toBlockingObservable().single();
		assertEquals(2, count);
	}

	@Test
	public void testJdbcObservableCountLettersInAllNames() {
		Func1<ResultSet, Integer> countLettersInName = new Func1<ResultSet, Integer>() {
			@Override
			public Integer call(ResultSet rs) {
				try {
					return rs.getString("name").length();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
		};
		int count = Observable
				.sumInteger(
						db().select("select name from person where name >?")
								.parameter("ALEX").get(countLettersInName))
				.first().toBlockingObservable().single();
		assertEquals(19, count);
	}

	@Test
	public void testTransformToTuple2AndTestActionsPrintln() {
		Tuple2<String, Integer> tuple = db()
				.select("select name,score from person where name >? order by name")
				.parameter("ALEX").getAs(String.class, Integer.class).last()
				.toBlockingObservable().single();
		assertEquals("MARMADUKE", tuple.value1());
		assertEquals(25, (int) tuple.value2());
	}

	@Test
	public void testTransformToTupleN() {
		TupleN<String> tuple = db()
				.select("select name, lower(name) from person order by name")
				.getTupleN(String.class).first().toBlockingObservable()
				.single();
		assertEquals("FRED", tuple.values().get(0));
		assertEquals("fred", tuple.values().get(1));
	}

	@Test
	public void testMultipleSetsOfParameters() {
		List<Integer> list = db()
				.select("select score from person where name=?")
				.parameter("FRED").parameter("JOSEPH").getAs(Integer.class)
				.toSortedList().toBlockingObservable().single();
		assertEquals(asList(21, 34), list);
	}

	@Test
	public void testNoParams() {
		List<Tuple2<String, Integer>> tuples = db()
				.select("select name, score from person where name=? order by name")
				.getAs(String.class, Integer.class).toList()
				.toBlockingObservable().single();
		assertEquals(0, tuples.size());
	}

	@Test
	public void testComposition2() {
		log.debug("running testComposition2");
		Func1<Integer, Boolean> isZero = new Func1<Integer, Boolean>() {
			@Override
			public Boolean call(Integer count) {
				return count == 0;
			}
		};
		Database db = db();
		Observable<Integer> existingRows = db
				.select("select name from person where name=?")
				.parameter("FRED").getAs(String.class).count().filter(isZero);
		List<Integer> counts = db
				.update("insert into person(name,score) values(?,?)")
				.parameters(existingRows).count().toList()
				.toBlockingObservable().single();
		assertEquals(0, counts.size());
	}

	@Test
	public void testEmptyResultSet() {
		int count = db().select("select name from person where name >?")
				.parameters(Observable.from("ZZTOP")).get().count().first()
				.toBlockingObservable().single();
		assertEquals(0, count);
	}

	@Test
	public void testMixingExplicitAndObservableParameters() {
		String name = db()
				.select("select name from person where name > ?  and score < ? order by name")
				.parameter("BARRY").parameters(Observable.from(100))
				.getAs(String.class).first().toBlockingObservable().single();
		assertEquals("FRED", name);
	}

	@Test
	public void testInstantiateDatabaseWithUrl() throws SQLException {
		Database db = new Database("jdbc:h2:mem:testa1");
		Connection con = db.queryContext().connectionProvider().get();
		con.close();
	}

	@Test
	public void testComposition() {
		// use composition to find the first person alphabetically with
		// a score less than the person with the last name alphabetically
		// whose name is not XAVIER. Two threads and connections will be used.

		Database db = db();
		Observable<Integer> score = db
				.select("select score from person where name <> ? order by name")
				.parameter("XAVIER").getAs(Integer.class).last();
		String name = db
				.select("select name from person where score < ? order by name")
				.parameters(score).getAs(String.class).first()
				.toBlockingObservable().single();
		assertEquals("FRED", name);
	}

	@Test
	public void testCompositionTwoLevels() {

		Database db = db();
		Observable<String> names = db.select(
				"select name from person order by name").getAs(String.class);
		Observable<String> names2 = db
				.select("select name from person where name<>? order by name")
				.parameters(names).parameters(names).getAs(String.class);
		List<String> list = db.select("select name from person where name>?")
				.parameters(names2).getAs(String.class).toList()
				.toBlockingObservable().single();
		System.out.println(list);
		assertEquals(12, list.size());
	}

	@Test(expected = RuntimeException.class)
	public void testSqlProblem() {
		String name = db().select("select name from pperson where name >?")
				.parameter("ALEX").getAs(String.class).first()
				.toBlockingObservable().single();
		log.debug(name);
	}

	@Test(expected = ClassCastException.class)
	public void testException() {
		Integer name = db().select("select name from person where name >?")
				.parameter("ALEX").getAs(Integer.class).first()
				.toBlockingObservable().single();
		log.debug("name=" + name);
	}

	@Test
	public void testDependsUsingAsynchronousQueriesWaitsForFirstByDelayingCalculation() {
		Database db = db();
		Observable<Integer> insert = db
				.update("insert into person(name,score) values(?,?)")
				.parameters("JOHN", 45)
				.count()
				.zip(Observable.interval(100, TimeUnit.MILLISECONDS),
						new Func2<Integer, Long, Integer>() {
							@Override
							public Integer call(Integer t1, Long t2) {
								return t1;
							}
						});

		int count = db.select("select name from person").dependsOn(insert)
				.get().count().toBlockingObservable().single();
		assertEquals(4, count);
	}

	@Test
	public void testAutoMapWillMapStringToStringAndIntToDouble() {
		Person person = db()
				.select("select name,score,dob,registered from person order by name")
				.autoMap(Person.class).first().toBlockingObservable().single();
		assertEquals("FRED", person.getName());
		assertEquals(21, person.getScore(), 0.001);
		assertNull(person.getDateOfBirth());
	}

	@Test(expected = RuntimeException.class)
	public void testAutoMapCannotFindConstructorWithEnoughParameters() {
		db().select(
				"select name,score,dob,registered,name from person order by name")
				.autoMap(Person.class).first().toBlockingObservable().single();
	}

	@Test
	public void testGetTimestamp() {
		Database db = db();
		java.sql.Timestamp registered = new java.sql.Timestamp(100);
		Observable<Integer> u = db
				.update("update person set registered=? where name=?")
				.parameter(registered).parameter("FRED").count();
		Date regTime = db.select("select registered from person order by name")
				.dependsOn(u).getAs(Date.class).first().toBlockingObservable()
				.single();
		assertEquals(100, regTime.getTime());
	}

	@Test
	public void insertClobAndReadAsString() throws SQLException {
		Database db = db();
		insertClob(db);
		// read clob as string
		String text = db.select("select document from person_clob")
				.getAs(String.class).first().toBlockingObservable().single();
		assertTrue(text.contains("about Fred"));
	}

	private static void insertClob(Database db) {
		int count = db
				.update("insert into person_clob(name,document) values(?,?)")
				.parameter("FRED")
				.parameter(
						"A description about Fred that is rather long and needs a Clob to store it")
				.count().toBlockingObservable().single();
		assertEquals(1, count);
	}

	private static void insertBlob(Database db) {
		int count = db
				.update("insert into person_blob(name,document) values(?,?)")
				.parameter("FRED")
				.parameter(
						"A description about Fred that is rather long and needs a Clob to store it"
								.getBytes()).count().toBlockingObservable()
				.single();
		assertEquals(1, count);
	}

	@Test
	public void insertClobAndReadAsReader() throws SQLException, IOException {
		Database db = db();
		insertClob(db);
		// read clob as Reader
		String text = db.select("select document from person_clob")
				.getAs(Reader.class).map(Util.READER_TO_STRING).first()
				.toBlockingObservable().single();
		assertTrue(text.contains("about Fred"));
	}

	@Test
	public void insertBlobAndReadAsByteArray() throws SQLException {
		Database db = db();
		insertBlob(db);
		// read clob as string
		byte[] bytes = db.select("select document from person_blob")
				.getAs(byte[].class).first().toBlockingObservable().single();
		assertTrue(new String(bytes).contains("about Fred"));
	}

	@Test
	public void testInsertNull() {
		int count = db()
				.update("insert into person(name,score,dob) values(?,?,?)")
				.parameters("JACK", 42, null).count().toBlockingObservable()
				.single();
		assertEquals(1, count);
	}

	@Test
	public void testAutoMap() {
		TimeZone current = TimeZone.getDefault();
		try {
			TimeZone.setDefault(TimeZone.getTimeZone("AEST"));
			Database db = db();
			Date dob = new Date(100);
			long now = System.currentTimeMillis();
			java.sql.Timestamp registered = new java.sql.Timestamp(now);
			Observable<Integer> u = db
					.update("update person set dob=?, registered=? where name=?")
					.parameter(dob).parameter(registered).parameter("FRED")
					.count();
			Person person = db
					.select("select name,score,dob,registered from person order by name")
					.dependsOn(u).autoMap(Person.class).first()
					.toBlockingObservable().single();
			assertEquals("FRED", person.getName());
			assertEquals(21, person.getScore(), 0.001);
			// Dates are truncated to start of day
			assertEquals(0, (long) person.getDateOfBirth());
			assertEquals(now, (long) person.getRegistered());
		} finally {
			TimeZone.setDefault(current);
		}
	}

	@Test
	public void testLastTransactionWithoutTransaction() {
		Database db = db();
		List<Boolean> list = db.lastTransactionResult().toList()
				.toBlockingObservable().single();
		assertTrue(list.isEmpty());
	}

	@Test
	public void testTuple3() {
		Tuple3<String, Integer, String> tuple = db()
				.select("select name,1,lower(name) from person order by name")
				.getAs(String.class, Integer.class, String.class).first()
				.toBlockingObservable().single();
		assertEquals("FRED", tuple.value1());
		assertEquals(1, (int) tuple.value2());
		assertEquals("fred", tuple.value3());
	}

	@Test
	public void testTuple4() {
		Tuple4<String, Integer, String, Integer> tuple = db()
				.select("select name,1,lower(name),2 from person order by name")
				.getAs(String.class, Integer.class, String.class, Integer.class)
				.first().toBlockingObservable().single();
		assertEquals("FRED", tuple.value1());
		assertEquals(1, (int) tuple.value2());
		assertEquals("fred", tuple.value3());
		assertEquals(2, (int) tuple.value4());
	}

	@Test
	public void testTuple5() {
		Tuple5<String, Integer, String, Integer, String> tuple = db()
				.select("select name,1,lower(name),2,name from person order by name")
				.getAs(String.class, Integer.class, String.class,
						Integer.class, String.class).first()
				.toBlockingObservable().single();
		assertEquals("FRED", tuple.value1());
		assertEquals(1, (int) tuple.value2());
		assertEquals("fred", tuple.value3());
		assertEquals(2, (int) tuple.value4());
		assertEquals("FRED", tuple.value5());
	}

	@Test
	public void testTuple6() {
		Tuple6<String, Integer, String, Integer, String, Integer> tuple = db()
				.select("select name,1,lower(name),2,name,3 from person order by name")
				.getAs(String.class, Integer.class, String.class,
						Integer.class, String.class, Integer.class).first()
				.toBlockingObservable().single();
		assertEquals("FRED", tuple.value1());
		assertEquals(1, (int) tuple.value2());
		assertEquals("fred", tuple.value3());
		assertEquals(2, (int) tuple.value4());
		assertEquals("FRED", tuple.value5());
		assertEquals(3, (int) tuple.value6());
	}

	@Test
	public void testTuple7() {
		Tuple7<String, Integer, String, Integer, String, Integer, Integer> tuple = db()
				.select("select name,1,lower(name),2,name,3,4 from person order by name")
				.getAs(String.class, Integer.class, String.class,
						Integer.class, String.class, Integer.class,
						Integer.class).first().toBlockingObservable().single();
		assertEquals("FRED", tuple.value1());
		assertEquals(1, (int) tuple.value2());
		assertEquals("fred", tuple.value3());
		assertEquals(2, (int) tuple.value4());
		assertEquals("FRED", tuple.value5());
		assertEquals(3, (int) tuple.value6());
		assertEquals(4, (int) tuple.value7());
	}

	@Test
	public void testAutoMapClob() {
		Database db = db();
		insertClob(db);
		List<PersonClob> list = db
				.select("select name, document from person_clob")
				.autoMap(PersonClob.class).toList().toBlockingObservable()
				.single();
		assertEquals(1, list.size());
		assertEquals("FRED", list.get(0).getName());
		assertTrue(list.get(0).getDocument().contains("rather long"));
	}

	@Test
	public void testAutoMapBlob() {
		Database db = db();
		insertBlob(db);
		List<PersonBlob> list = db
				.select("select name, document from person_blob")
				.autoMap(PersonBlob.class).toList().toBlockingObservable()
				.single();
		assertEquals(1, list.size());
		assertEquals("FRED", list.get(0).getName());
		assertTrue(new String(list.get(0).getDocument())
				.contains("rather long"));
	}

	@Test
	public void testCalendarParameter() throws SQLException {
		Database db = db();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		Observable<Integer> update = db
				.update("update person set registered=? where name=?")
				.parameters(cal, "FRED").count();
		Timestamp t = db.select("select registered from person where name=?")
				.parameter("FRED").dependsOn(update).getAs(Timestamp.class)
				.first().toBlockingObservable().single();
		assertEquals(0, t.getTime());
	}

	@Test
	public void testDatabaseBuilder() {
		Database.builder().connectionProvider(connectionProvider())
				.transactionalSchedulerOnCurrentThread()
				.nonTransactionalSchedulerOnCurrentThread().build();
	}

	@Test
	public void testConnectionPool() {
		ConnectionProviderPooled cp = new ConnectionProviderPooled(nextUrl(),
				0, 10);
		Database db = new Database(cp);
		DatabaseCreator.createDatabase(cp.get());
		int count = db.select("select name from person order by name").get()
				.count().toBlockingObservable().single();
		assertEquals(3, count);
	}

	@Test
	public void testConnectionPoolDoesNotRunOutOfConnectionsWhenQueryRunRepeatedly() {
		ConnectionProviderPooled cp = new ConnectionProviderPooled(nextUrl(),
				0, 1);
		Database db = new Database(cp);
		DatabaseCreator.createDatabase(cp.get());
		assertCountIs(
				100,
				db.select("select name from person where name=?")
						.parameters(
								Observable.range(0, 100).map(
										Util.constant("FRED"))).get());
	}

	@Test
	public void testDatabaseBuilderWithPool() {
		Database.builder().pooled(nextUrl(), 0, 5).build().close();
	}

	private static void assertCountIs(int count, Observable<?> o) {
		assertEquals(count, (int) o.count().toBlockingObservable().single());
	}

	@Test
	public void testOneConnectionOpenAndClosedAfterOneSelect()
			throws InterruptedException {
		CountDownConnectionProvider cp = new CountDownConnectionProvider(1, 1);
		Database db = new Database(cp);
		db.select("select name from person").get().count()
				.toBlockingObservable().single();
		cp.closesLatch().await();
		cp.getsLatch().await();
	}

	@Test
	public void testOneConnectionOpenAndClosedAfterOneUpdate()
			throws InterruptedException {
		CountDownConnectionProvider cp = new CountDownConnectionProvider(1, 1);
		Database db = new Database(cp);
		db.update("update person set score=? where name=?")
				.parameters(23, "FRED").count().toBlockingObservable().single();
		cp.closesLatch().await(60, TimeUnit.SECONDS);
		cp.getsLatch().await(60, TimeUnit.SECONDS);
	}

	@Test
	public void testLiftWithParameters() {
		int score = from("FRED")
				.lift(db().select("select score from person where name=?")
						.parameterOperator().getAs(Integer.class))
				.toBlockingObservable().single();
		assertEquals(21, score);
	}

	@Test
	public void testLiftWithManyParameters() {
		int score = Observable
				.range(1, 3)
				.doOnEach(log())
				.map(Util.constant("FRED"))
				.lift(db().select("select score from person where name=?")
						.parameterOperator().getAs(Integer.class))
				.sumInteger(rx.functions.Functions.<Integer> identity())
				.toBlockingObservable().single();
		assertEquals(3 * 21, score);
	}

	@Test
	public void test() throws InterruptedException {
		List<Long> list = Observable
				.interval(100, TimeUnit.MILLISECONDS)
				.doOnEach(log())
				.flatMap(
						Util.constant(Observable.interval(100,
								TimeUnit.MILLISECONDS))).take(3).toList()
				.toBlockingObservable().single();
	}

	@Test
	public void testTwoConnectionsOpenedAndClosedAfterTwoAsyncSelects()
			throws InterruptedException {
		CountDownConnectionProvider cp = new CountDownConnectionProvider(2, 2);
		Database db = new Database(cp);
		db.select("select name from person").get().count()
				.toBlockingObservable().single();
		db.select("select name from person").get().count()
				.toBlockingObservable().single();
		cp.getsLatch().await(60, TimeUnit.SECONDS);
		cp.closesLatch().await(60, TimeUnit.SECONDS);
	}

	@Test
	public void testTwoConnectionsOpenedAndClosedAfterTwoAsyncUpdates()
			throws InterruptedException {
		CountDownConnectionProvider cp = new CountDownConnectionProvider(2, 2);
		Database db = new Database(cp);
		db.update("update person set score=? where name=?")
				.parameters(23, "FRED").count().toBlockingObservable().single();
		db.update("update person set score=? where name=?")
				.parameters(25, "JOHN").count().toBlockingObservable().single();
		cp.getsLatch().await(60, TimeUnit.SECONDS);
		cp.closesLatch().await(60, TimeUnit.SECONDS);
	}

	@Test
	public void testOneConnectionOpenedAndClosedAfterTwoSelectsWithinTransaction()
			throws InterruptedException {
		CountDownConnectionProvider cp = new CountDownConnectionProvider(1, 1);
		Database db = new Database(cp);
		db.beginTransaction();
		Observable<Integer> count = db.select("select name from person").get()
				.count();
		Observable<Integer> count2 = db.select("select name from person").get()
				.count();
		int result = db.commit(count, count2).count().toBlockingObservable()
				.single();
		log.info("committed " + result);
		cp.getsLatch().await();
		log.info("gets ok");
		cp.closesLatch().await();
		log.info("closes ok");
	}

	@Test
	public void testOneConnectionOpenedAndClosedAfterTwoUpdatesWithinTransaction()
			throws InterruptedException {
		CountDownConnectionProvider cp = new CountDownConnectionProvider(1, 1);
		Database db = new Database(cp);
		db.beginTransaction();
		Observable<Integer> count = db
				.update("update person set score=? where name=?")
				.parameters(23, "FRED").count();
		Observable<Integer> count2 = db
				.update("update person set score=? where name=?")
				.parameters(25, "JOHN").count();
		int result = db.commit(count, count2).count().toBlockingObservable()
				.single();
		log.info("committed " + result);
		cp.getsLatch().await();
		log.info("gets ok");
		cp.closesLatch().await();
		log.info("closes ok");
	}

	private static class CountDownConnectionProvider implements
			ConnectionProvider {
		private final ConnectionProvider cp;
		private final CountDownLatch closesLatch;
		private final CountDownLatch getsLatch;

		CountDownConnectionProvider(int expectedGets, int expectedCloses) {
			this.cp = connectionProvider();
			DatabaseCreator.createDatabase(cp.get());
			this.closesLatch = new CountDownLatch(expectedCloses);
			this.getsLatch = new CountDownLatch(expectedGets);
		}

		CountDownLatch closesLatch() {
			return closesLatch;
		}

		CountDownLatch getsLatch() {
			return getsLatch;
		}

		@Override
		public Connection get() {
			getsLatch.countDown();
			Connection inner = cp.get();
			return new CountingConnection(inner, closesLatch);
		}

		@Override
		public void close() {
			cp.close();
		}
	}

	static class PersonClob {
		private final String name;
		private final String document;

		public PersonClob(String name, String document) {
			this.name = name;
			this.document = document;
		}

		public String getName() {
			return name;
		}

		public String getDocument() {
			return document;
		}
	}

	static class PersonBlob {
		private final String name;
		private final byte[] document;

		public PersonBlob(String name, byte[] document) {
			this.name = name;
			this.document = document;
		}

		public String getName() {
			return name;
		}

		public byte[] getDocument() {
			return document;
		}
	}

	static class Person {
		private final String name;
		private final double score;
		private final Long dateOfBirthEpochMs;
		private final Long registered;

		Person(String name, double score, Long dateOfBirthEpochMs,
				Long registered) {
			this.name = name;
			this.score = score;
			this.dateOfBirthEpochMs = dateOfBirthEpochMs;
			this.registered = registered;
		}

		public String getName() {
			return name;
		}

		public double getScore() {
			return score;
		}

		public Long getDateOfBirth() {
			return dateOfBirthEpochMs;
		}

		public Long getRegistered() {
			return registered;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Pair [name=");
			builder.append(name);
			builder.append(", score=");
			builder.append(score);
			builder.append("]");
			return builder.toString();
		}

	}

}
