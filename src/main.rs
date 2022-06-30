use std::io;

use crate::parse::lexer::Lexer;

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
    while stdin.read_line(&mut s).unwrap() != 0 {
        let mut lex = Lexer::new(&s);
        let x;
        let y;
        s = String::new();
        if lex.match_id() {
            x = lex.eat_id().unwrap();
            lex.eat_delim('=').unwrap();
            y = lex.eat_int_constant().unwrap();
        } else {
            y = lex.eat_int_constant().unwrap();
            lex.eat_delim('=').unwrap();
            x = lex.eat_id().unwrap();
        }
        println!("{} equals {}", x, y);
    }
}
