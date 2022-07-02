#[cfg(test)]
mod tests {
    use crate::parse::lexer::Lexer;

    #[test]
    fn lexertest() {
        let ss = [
            "a=1", "1=a", "z=1", "1=z", "_=1", "1=_", "A=1", "1=A", "a=-1", "-1=a", " a = 1 ",
            " 1 = a ", "ab=12", "12=ab",
        ];
        let e = [
            ("a", 1),
            ("a", 1),
            ("z", 1),
            ("z", 1),
            ("_", 1),
            ("_", 1),
            ("a", 1),
            ("a", 1),
            ("a", -1),
            ("a", -1),
            ("a", 1),
            ("a", 1),
            ("ab", 12),
            ("ab", 12),
        ];
        for (i, s) in ss.iter().enumerate() {
            let mut lex = Lexer::new(&s);
            let x;
            let y;
            if lex.match_id() {
                x = lex.eat_id().unwrap();
                lex.eat_delim('=').unwrap();
                y = lex.eat_int_constant().unwrap();
            } else {
                y = lex.eat_int_constant().unwrap();
                lex.eat_delim('=').unwrap();
                x = lex.eat_id().unwrap();
            }
            assert_eq!(x, e[i].0);
            assert_eq!(y, e[i].1);
        }
    }
}
