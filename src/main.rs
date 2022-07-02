use std::io::{self, Write};

use crate::parse::parser::Parser;

mod buffer;
mod file;
mod index;
mod log;
mod metadata;
mod parse;
mod query;
mod record;
mod server;
mod tx;

fn main() {
    let mut s = String::new();
    let stdin = io::stdin();
    print!("Enter an SQL statement: ");
    io::stdout().flush().unwrap();
    while stdin.read_line(&mut s).unwrap() != 0 {
        let mut p = Parser::new(&s.trim_end());
        let ok;
        if s.starts_with("select") {
            ok = p.query().is_ok();
        } else {
            ok = p.update_cmd().is_ok();
        }
        if ok {
            println!("yes");
        } else {
            println!("no");
        }
        s = String::new();
        print!("Enter an SQL statement: ");
        io::stdout().flush().unwrap();
    }
}
