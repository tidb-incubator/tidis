use crate::Parse;

#[derive(Debug)]
pub struct Auth {
    passwd: String,
}

impl Auth {
    pub fn new(passwd: String) -> Auth {
        Auth { passwd }
    }

    pub fn passwd(&self) -> &str {
        &self.passwd
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Auth> {
        let passwd = parse.next_string()?;

        Ok(Auth { passwd })
    }
}
