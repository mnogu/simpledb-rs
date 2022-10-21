use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::query::scan::ScanControl;

#[derive(Clone)]
pub struct RecordComparator {
    fields: Vec<String>,
}

impl RecordComparator {
    pub fn new(fields: Vec<String>) -> RecordComparator {
        RecordComparator { fields }
    }

    pub fn partial_cmp<T: ScanControl, U: ScanControl>(
        &self,
        s1: Arc<Mutex<T>>,
        s2: Arc<Mutex<U>>,
    ) -> Option<Ordering> {
        for fldname in &self.fields {
            match (
                s1.lock().unwrap().get_val(fldname),
                s2.lock().unwrap().get_val(fldname),
            ) {
                (Ok(val1), Ok(val2)) => {
                    if val1 > val2 {
                        return Some(Ordering::Greater);
                    } else if val1 < val2 {
                        return Some(Ordering::Less);
                    }
                }
                _ => return None,
            }
        }
        Some(Ordering::Equal)
    }
}
