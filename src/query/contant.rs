pub struct Constant {
    ival: Option<i32>,
    sval: Option<String>,
}

impl Constant {
    pub fn with_integer(ival: i32) -> Constant {
        Constant {
            ival: Some(ival),
            sval: None,
        }
    }

    pub fn with_string(sval: &str) -> Self {
        Constant {
            ival: None,
            sval: Some(sval.to_string()),
        }
    }

    pub fn as_int(&self) -> Option<i32> {
        self.ival
    }

    pub fn as_string(&self) -> Option<String> {
        self.sval.clone()
    }
}
