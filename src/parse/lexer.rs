use std::collections::HashSet;

use super::badsyntaxerror::BadSyntaxError;

#[derive(Clone, Copy, PartialEq)]
enum TokenType {
    Delim,
    IntConstant,
    StringConstant,
    Keyword,
    Id,
}

struct Token {
    nval: Option<i32>,
    sval: Option<String>,
    ttype: TokenType,
}

pub struct Lexer {
    keywords: HashSet<String>,
    chars: Vec<char>,
    i: usize,
    token: Option<Token>,
}

impl Token {
    fn with_int(nval: i32, ttype: TokenType) -> Token {
        Token {
            nval: Some(nval),
            sval: None,
            ttype,
        }
    }

    fn with_string(sval: String, ttype: TokenType) -> Token {
        Token {
            nval: None,
            sval: Some(sval),
            ttype,
        }
    }

    fn nval(&self) -> Option<i32> {
        self.nval
    }

    fn sval(&self) -> Option<String> {
        self.sval.clone()
    }

    fn ttype(&self) -> TokenType {
        self.ttype
    }
}

impl Lexer {
    pub fn new(s: &str) -> Lexer {
        let keywords = HashSet::from([
            "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
            "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
        ])
        .iter()
        .map(|s| s.to_string())
        .collect();
        let mut l = Lexer {
            keywords,
            chars: s.to_lowercase().chars().collect(),
            i: 0,
            token: None,
        };
        l.next_token();
        l
    }

    pub fn match_delim(&self, d: char) -> bool {
        if let Some(token) = &self.token {
            if token.ttype() != TokenType::Delim {
                return false;
            }
            if let Some(sval) = token.sval() {
                let chars: Vec<char> = sval.chars().collect();
                return chars[0] == d;
            }
        }
        false
    }

    pub fn match_int_constant(&self) -> bool {
        if let Some(token) = &self.token {
            return token.ttype() == TokenType::IntConstant;
        }
        false
    }

    pub fn match_string_constant(&self) -> bool {
        if let Some(token) = &self.token {
            return token.ttype() == TokenType::StringConstant;
        }
        false
    }

    pub fn match_keyword(&self, w: &str) -> bool {
        if let Some(token) = &self.token {
            if token.ttype() != TokenType::Keyword {
                return false;
            }
            if let Some(sval) = token.sval() {
                return sval == w.to_string();
            }
        }
        false
    }

    pub fn match_id(&self) -> bool {
        if let Some(token) = &self.token {
            return token.ttype() == TokenType::Id;
        }
        false
    }

    pub fn eat_delim(&mut self, d: char) -> Result<(), BadSyntaxError> {
        if !self.match_delim(d) {
            return Err(BadSyntaxError);
        }
        self.next_token();
        Ok(())
    }

    pub fn eat_int_constant(&mut self) -> Result<i32, BadSyntaxError> {
        if !self.match_int_constant() {
            return Err(BadSyntaxError);
        }
        if let Some(token) = &self.token {
            if let Some(i) = token.nval() {
                self.next_token();
                return Ok(i);
            }
        }
        Err(BadSyntaxError)
    }

    pub fn eat_string_constant(&mut self) -> Result<String, BadSyntaxError> {
        if !self.match_string_constant() {
            return Err(BadSyntaxError);
        }
        if let Some(token) = &self.token {
            if let Some(s) = token.sval() {
                self.next_token();
                return Ok(s);
            }
        }
        Err(BadSyntaxError)
    }

    pub fn eat_keyword(&mut self, w: &str) -> Result<(), BadSyntaxError> {
        if !self.match_keyword(w) {
            return Err(BadSyntaxError);
        }
        self.next_token();
        Ok(())
    }

    pub fn eat_id(&mut self) -> Result<String, BadSyntaxError> {
        if !self.match_id() {
            return Err(BadSyntaxError);
        }
        if let Some(token) = &self.token {
            if let Some(s) = token.sval() {
                self.next_token();
                return Ok(s);
            }
        }
        Err(BadSyntaxError)
    }

    fn next_token(&mut self) {
        if self.i >= self.chars.len() {
            self.token = None;
            return;
        }

        while self.is_whitespce_char(self.chars[self.i]) {
            self.i += 1;
            if self.i >= self.chars.len() {
                self.token = None;
                return;
            }
        }

        let char = self.chars[self.i];
        if char == '"' || char == '\'' {
            self.i += 1;
            if self.i >= self.chars.len() {
                self.token = None;
                return;
            }
            let mut sval = String::new();
            let mut c = self.chars[self.i];
            while c != char {
                sval.push(c);
                self.i += 1;
                if self.i >= self.chars.len() {
                    self.token = None;
                    return;
                }
                c = self.chars[self.i];
            }
            self.token = Some(Token::with_string(sval, TokenType::StringConstant));
            return;
        }

        let mut is_negative = false;
        if self.chars[self.i] == '-' {
            is_negative = true;
            self.i += 1;
        }
        let mut nval = 0;
        let mut is_number = false;
        while self.is_number(self.chars[self.i]) {
            is_number = true;
            nval = 10 * nval + (self.chars[self.i] as i32 - '0' as i32);
            self.i += 1;
            if self.i >= self.chars.len() {
                break;
            }
        }
        if is_number {
            if is_negative {
                nval = -nval;
            }
            self.token = Some(Token::with_int(nval, TokenType::IntConstant));
            return;
        }
        if is_negative {
            self.i -= 1;
        }

        let mut sval = String::new();
        while self.is_word_char(self.chars[self.i]) {
            sval.push(self.chars[self.i]);
            self.i += 1;
            if self.i >= self.chars.len() {
                break;
            }
        }
        if sval.len() != 0 {
            if self.keywords.contains(&sval) {
                self.token = Some(Token::with_string(sval, TokenType::Keyword));
                return;
            }
            self.token = Some(Token::with_string(sval, TokenType::Id));
            return;
        }

        if self.is_delim_char(self.chars[self.i]) {
            self.token = Some(Token::with_string(
                self.chars[self.i].to_string(),
                TokenType::Delim,
            ));
            self.i += 1;
            return;
        }

        self.token = None;
    }

    fn is_whitespce_char(&self, c: char) -> bool {
        c >= '\u{0000}' && c <= '\u{0020}'
    }

    fn is_number(&self, c: char) -> bool {
        c >= '0' && c <= '9'
    }

    fn is_word_char(&self, c: char) -> bool {
        c >= 'a' && c <= 'z'
            || c >= 'A' && c <= 'Z'
            || c >= '\u{00A0}' && c <= '\u{00FF}'
            || c == '_'
    }

    fn is_delim_char(&self, c: char) -> bool {
        !self.is_whitespce_char(c) && !self.is_number(c) && !self.is_word_char(c)
    }
}
