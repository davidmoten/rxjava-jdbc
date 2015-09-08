package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.DatabaseCreator.connectionProvider;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

public class DatabaseMasterDetailTest {

    @Test
    public void testIvanoExample1() {
        Connection con = connectionProvider().get();
        createDatabase(con);
        Database db = Database.from(con);
        List<Master> masters = db.select("select id, name from master order by id")
                // map to class
                .autoMap(Master.class)
                // as list and get
                .toList().toBlocking().single();
        assertEquals(2, masters.size());
        assertEquals("FRED", masters.get(0).name());
        assertEquals("JOHN", masters.get(1).name());
        db.close();
    }

    @Test
    public void testIvanoExample2() {
        Connection con = connectionProvider().get();
        createDatabase(con);
        final Database db = Database.from(con);
        List<MasterAndDetails> list = db
                // get masters
                .select("select id, name from master order by id")
                // map to class
                .autoMap(Master.class)
                // combine master and details
                .flatMap(new Func1<Master, Observable<MasterAndDetails>>() {
                    @Override
                    public Observable<MasterAndDetails> call(final Master master) {
                        return db
                                .select("select id, desc from detail where master_id=? order by id")
                                // set parameters
                                .parameters(master.id())
                                // to Detail
                                .autoMap(Detail.class)
                                // to a list
                                .toList()
                                // now combine master and details in a
                                // MasterAndDetails object
                                .map(new Func1<List<Detail>, MasterAndDetails>() {
                            @Override
                            public MasterAndDetails call(List<Detail> details) {
                                return new MasterAndDetails(master.id(), master.name(), details);
                            }
                        });
                    }
                })
                // log
                .doOnNext(new Action1<MasterAndDetails>() {
                    @Override
                    public void call(MasterAndDetails md) {
                        System.out.println(md);
                    }
                })
                // to list and get
                .toList().toBlocking().single();
        assertEquals(2, list.size());
        assertEquals("FRED", list.get(0).name());
        assertEquals(2, list.get(0).details().size());
        assertEquals(10, list.get(0).details().get(0).id());
        assertEquals(11, list.get(0).details().get(1).id());
        assertEquals("JOHN", list.get(1).name());
        assertEquals(2, list.get(1).details().size());
        assertEquals(20, list.get(1).details().get(0).id());
        assertEquals(21, list.get(1).details().get(1).id());
    }

    static class Master {
        private final Integer id;
        private final String name;

        Master(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        int id() {
            return id;
        }

        String name() {
            return name;
        }
    }

    static class Detail {
        private final Integer id;
        private final String description;

        public Detail(Integer id, String description) {
            this.id = id;
            this.description = description;
        }

        int id() {
            return id;
        }

        String description() {
            return description;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Detail [id=");
            builder.append(id);
            builder.append(", description=");
            builder.append(description);
            builder.append("]");
            return builder.toString();
        }

    }

    static class MasterAndDetails {
        private final Integer id;
        private final String name;
        private final List<Detail> details;

        MasterAndDetails(Integer id, String name, List<Detail> details) {
            this.id = id;
            this.name = name;
            this.details = new ArrayList<Detail>(details);
        }

        int id() {
            return id;
        }

        String name() {
            return name;
        }

        List<Detail> details() {
            // return defensive copy
            return new ArrayList<Detail>(details);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("MasterAndDetails [id=");
            builder.append(id);
            builder.append(", name=");
            builder.append(name);
            builder.append(", details=");
            builder.append(details);
            builder.append("]");
            return builder.toString();
        }

    }

    private static void createDatabase(Connection c) {
        try {
            c.setAutoCommit(true);
            c.prepareStatement("create table master (id int primary key, name varchar2(50))")
                    .execute();
            c.prepareStatement("insert into master(id,name) values(1,'FRED')").execute();
            c.prepareStatement("insert into master(id,name) values(2,'JOHN')").execute();

            c.prepareStatement("create table detail (id int, master_id int, desc varchar(50))")
                    .execute();
            c.prepareStatement(
                    "insert into detail(id, master_id, desc) values(10, 1, 'a good fellow')")
                    .execute();
            c.prepareStatement(
                    "insert into detail(id, master_id, desc) values(11, 1, 'rides his bike')")
                    .execute();

            c.prepareStatement(
                    "insert into detail(id, master_id, desc) values(20, 2, 'a bit of a bore')")
                    .execute();
            c.prepareStatement(
                    "insert into detail(id, master_id, desc) values(21, 2, 'is a good cook')")
                    .execute();

        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

}
