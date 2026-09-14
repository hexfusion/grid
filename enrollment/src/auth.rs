//! Operator credentials for the deciding routes.
//!
//! Submitting is open, because an enrollee has no credentials yet; that is the
//! problem enrollment exists to solve. Deciding is not self-service, so approve,
//! deny, and list require an operator token.
//!
//! Tokens are kept as SHA-256 digests. Comparing digests rather than the tokens
//! themselves means a timing difference reveals nothing about a valid token, and
//! the table is not a list of usable credentials at rest.

use std::collections::HashMap;

use sha2::{Digest as _, Sha256};

/// Operators allowed to decide on requests.
#[derive(Debug, Default)]
pub struct Operators {
    /// Token digest to the name recorded as the approver.
    by_digest: HashMap<String, String>,
}

impl Operators {
    /// Read a token table.
    ///
    /// One `name:token` per line. Blank lines and lines starting with `#` are
    /// skipped. A line without a separator is skipped rather than treated as a
    /// nameless credential.
    #[must_use]
    pub fn from_table(text: &str) -> Self {
        let by_digest = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .filter_map(|line| line.split_once(':'))
            .filter_map(|(name, token)| {
                let name = name.trim();
                let token = token.trim();
                (!name.is_empty() && !token.is_empty()).then(|| (digest(token), name.to_owned()))
            })
            .collect();

        Self { by_digest }
    }

    /// The operator a token belongs to, if any.
    #[must_use]
    pub fn resolve(&self, presented: &str) -> Option<&str> {
        self.by_digest.get(&digest(presented)).map(String::as_str)
    }

    /// Whether any operator is configured.
    ///
    /// An empty table refuses every decision. No configured operator has to mean
    /// nobody can approve, not that anybody can.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_digest.is_empty()
    }

    /// How many operators are configured.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_digest.len()
    }
}

/// Lowercase hex SHA-256 of a token.
fn digest(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_token_resolves_to_its_operator() {
        let operators = Operators::from_table("alice: s3cret\n");
        assert_eq!(
            operators.resolve("s3cret"),
            Some("alice"),
            "alice's token should resolve"
        );
        assert_eq!(operators.resolve("wrong"), None, "an unknown token resolves to nobody");
    }

    #[test]
    fn comments_and_blank_lines_are_skipped() {
        let operators = Operators::from_table("# operators\n\nalice: one\nbob: two\n\n");
        assert_eq!(operators.len(), 2, "two operators should be read");
        assert_eq!(operators.resolve("two"), Some("bob"), "bob's token should resolve");
    }

    /// A malformed line must not become a credential that anything matches.
    #[test]
    fn a_line_without_a_token_is_not_a_credential() {
        let operators = Operators::from_table("nameless\nalice:\n: token\n");
        assert!(operators.is_empty(), "no usable operator should be read");
        assert_eq!(operators.resolve(""), None, "an empty token must not resolve");
    }

    #[test]
    fn no_configured_operator_means_nobody_can_decide() {
        let operators = Operators::from_table("");
        assert!(operators.is_empty(), "an empty table configures nobody");
    }

    #[test]
    fn the_table_does_not_hold_tokens_in_the_clear() {
        let operators = Operators::from_table("alice: s3cret\n");
        let debug = format!("{operators:?}");
        assert!(
            !debug.contains("s3cret"),
            "a token must not appear in the table's debug output"
        );
        assert!(debug.contains("alice"), "the operator name is not a secret");
    }

    #[test]
    fn tokens_are_compared_by_digest() {
        let operators = Operators::from_table("alice: s3cret\n");
        let expected: String = Sha256::digest(b"s3cret")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert!(
            operators.by_digest.contains_key(&expected),
            "the table should be keyed by token digest"
        );
    }
}
