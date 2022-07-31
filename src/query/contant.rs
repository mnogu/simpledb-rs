use std::fmt;

#[derive(PartialEq, Clone)]
pub struct Constant {
    ival: Option<i32>,
    sval: Option<String>,
}

impl fmt::Display for Constant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ival) = self.ival {
            return write!(f, "{}", ival);
        }
        if let Some(sval) = &self.sval {
            return write!(f, "{}", sval);
        }
        return write!(f, "");
    }
}

impl Constant {
    pub fn with_int(ival: i32) -> Constant {
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
