use std::io::{self, Write};

use api::statement::Statement;

use crate::{
    api::{
        connection::ConnectionControl,
        driver::{Driver, DriverControl},
        embedded::embeddeddriver::EmbeddedDriver,
        metadata::MetaDataControl,
        network::networkdriver::NetworkDriver,
        resultset::ResultSetControl,
        statement::StatementControl,
    },
    record::schema::Type,
};

mod api;
mod buffer;
mod file;
mod index;
mod log;
mod metadata;
mod parse;
mod plan;
mod query;
mod record;
mod server;
mod tx;

fn main() {
    let stdin = io::stdin();
    print!("Connect> ");
    io::stdout().flush().unwrap();
    let mut s = String::new();
    stdin.read_line(&mut s).unwrap();
    let is_embedded = !s.contains("//");
    let d: Driver;
    if is_embedded {
        d = EmbeddedDriver::new().into();
    } else {
        d = NetworkDriver::new().into();
    }

    let mut conn = d.connect(s.trim_end()).unwrap();
    s = String::new();
    let mut stmt = conn.create_statement();
    print!("\nSQL> ");
    io::stdout().flush().unwrap();
    while stdin.read_line(&mut s).unwrap() != 0 {
        let cmd = s.trim_end();
        if cmd.starts_with("exit") {
            break;
        }
        if cmd.starts_with("select") {
            do_query(&mut stmt, cmd);
        } else {
            do_update(&mut stmt, cmd);
        }
        s = String::new();
        print!("\nSQL> ");
        io::stdout().flush().unwrap();
    }
    conn.close().unwrap();
}

fn do_query(stmt: &mut Statement, cmd: &str) {
    let mut rs = stmt.execute_query(cmd).unwrap();
    let mut md = rs.get_meta_data();
    let numcols = md.get_column_count().unwrap();
    let mut totalwidth = 0;

    for i in 1..=numcols {
        let fldname = md.get_column_name(i).unwrap();
        let width = md.get_column_display_size(i).unwrap();
        totalwidth += width;
        print!("{:>width$}", fldname);
    }
    println!();
    for _ in 0..totalwidth {
        print!("-");
    }
    println!();

    while rs.next().unwrap() {
        for i in 1..=numcols {
            let fldname = md.get_column_name(i).unwrap();
            let width = md.get_column_display_size(i).unwrap();
            match md.get_column_type(i).unwrap() {
                Type::Integer => {
                    let ival = rs.get_int(&fldname).unwrap();
                    print!("{:>width$}", ival);
                }
                Type::Varchar => {
                    let sval = rs.get_string(&fldname).unwrap();
                    print!("{:>width$}", sval);
                }
            }
        }
        println!();
    }
    rs.close().unwrap();
}

fn do_update(stmt: &mut Statement, cmd: &str) {
    let howmany = stmt.execute_update(cmd).unwrap();
    println!("{} records processed", howmany);
}
