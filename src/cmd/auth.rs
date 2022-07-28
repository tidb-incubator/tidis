use crate::cmd::Invalid;
use crate::Parse;

#[derive(Debug, Clone)]
pub struct Auth {
    passwd: String,
    valid: bool,
}

impl Auth {
    pub fn new(passwd: String) -> Auth {
        Auth {
            passwd,
            valid: true,
        }
    }

    pub fn passwd(&self) -> &str {
        &self.passwd
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Auth> {
        let passwd = parse.next_string()?;

        Ok(Auth {
            passwd,
            valid: true,
        })
    }
}

impl Invalid for Auth {
    fn new_invalid() -> Auth {
        Auth {
            passwd: "".to_owned(),
            valid: false,
        }
    }
}
