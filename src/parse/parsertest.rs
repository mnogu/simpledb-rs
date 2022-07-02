#[cfg(test)]
mod tests {
    use crate::parse::parser::Parser;

    #[test]
    fn parsertest() {
        let ss = [
            ("select a from x where b = 3", true),
            ("select a, b from x,y,z", true),
            ("delete from x where a = b and c = 0", true),
            ("update x set a = b where c = 3", true),
            ("insert into x (a, b, c) values (3, 'glop', 4)", true),
            ("create table x ( a varchar(3), b int, c varchar(2) )", true),
            ("select from x", false),
            ("select x x from x", false),
            ("select a from where b=3", false),
            ("select a from y where b -=3", false),
            ("select a from y where", false),
        ];
        for (s, b) in ss.iter() {
            let mut p = Parser::new(&s.trim_end());
            let ok;
            if s.starts_with("select") {
                ok = p.query().is_ok();
            } else {
                ok = p.update_cmd().is_ok();
            }
            assert_eq!(&ok, b);
        }
    }
}
