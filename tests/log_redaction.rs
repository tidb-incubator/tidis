use tidis::{cmd::Auth, Command, Config};

const PASSWORD: &str = "DAT247-fictional-password-6ad53b";

#[test]
fn config_debug_omits_password() {
    let config: Config = toml::from_str(&format!(
        "[server]\nlisten = '127.0.0.1'\nport = 6666\npassword = '{}'\n[backend]",
        PASSWORD
    ))
    .unwrap();

    for output in [format!("{:?}", config), format!("{:#?}", config)] {
        assert!(!output.contains(PASSWORD));
        assert!(output.contains("authentication_enabled: true"));
        assert!(output.contains("127.0.0.1"));
    }
}

#[test]
fn auth_debug_redacts_password_without_changing_it() {
    let auth = Auth::new(PASSWORD.to_owned());
    assert_eq!(auth.passwd(), PASSWORD);
    assert!(auth.valid());

    let command = Command::Auth(auth.clone());
    for output in [
        format!("{:?}", auth),
        format!("{:#?}", auth),
        format!("{:?}", command),
        format!("{:#?}", command),
    ] {
        assert!(!output.contains(PASSWORD));
        assert!(output.contains("[REDACTED]"));
        assert!(output.contains("valid: true"));
    }
}
