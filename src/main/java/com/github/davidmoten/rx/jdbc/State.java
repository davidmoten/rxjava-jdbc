package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;

class State {
    volatile Connection con;
    volatile PreparedStatement ps;
    volatile ResultSet rs;
    final AtomicBoolean closed = new AtomicBoolean(false);
}