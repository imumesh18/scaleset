//! Parsing of the GitHub configuration URL that a scale set registers against.

use std::fmt;
use std::str::FromStr;

use url::Url;

use crate::error::Error;

/// Environment variable that forces GitHub Enterprise Server URL handling,
/// even for hosts that look like github.com.
pub const FORCE_GHES_ENV: &str = "GITHUB_ACTIONS_FORCE_GHES";

/// The level at which a scale set is registered.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Scope {
    /// `https://github.com/enterprises/{enterprise}`
    Enterprise {
        /// Enterprise slug.
        enterprise: String,
    },
    /// `https://github.com/{organization}`
    Organization {
        /// Organization login.
        organization: String,
    },
    /// `https://github.com/{owner}/{repository}`
    Repository {
        /// Repository owner.
        owner: String,
        /// Repository name.
        repository: String,
    },
}

/// A parsed GitHub configuration URL: an enterprise, organization or
/// repository on github.com, GHE.com or GitHub Enterprise Server.
///
/// ```
/// use scaleset::{GitHubConfigUrl, Scope};
///
/// let url: GitHubConfigUrl = "https://github.com/my-org/my-repo".parse()?;
/// assert_eq!(
///     url.scope(),
///     &Scope::Repository { owner: "my-org".into(), repository: "my-repo".into() }
/// );
/// assert_eq!(url.api_url("/zen").as_str(), "https://api.github.com/zen");
/// # Ok::<(), scaleset::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubConfigUrl {
    /// The input with surrounding slashes trimmed, as sent to the service.
    raw: String,
    url: Url,
    scope: Scope,
    hosted: bool,
}

impl GitHubConfigUrl {
    /// Parses a configuration URL.
    ///
    /// Hosts `github.com`, `www.github.com`, `github.localhost` and
    /// `*.ghe.com` are treated as GitHub-hosted unless the
    /// [`GITHUB_ACTIONS_FORCE_GHES`](FORCE_GHES_ENV) environment variable is
    /// set. Every other host is treated as GitHub Enterprise Server.
    pub fn parse(input: &str) -> Result<Self, Error> {
        let force_ghes = std::env::var_os(FORCE_GHES_ENV).is_some();
        Self::parse_with(input, force_ghes)
    }

    fn parse_with(input: &str, force_ghes: bool) -> Result<Self, Error> {
        let raw = input.trim_matches('/').to_owned();
        let invalid = |reason: &str| Error::InvalidConfigUrl {
            url: input.to_owned(),
            reason: reason.to_owned(),
        };

        let url = Url::parse(&raw).map_err(|e| invalid(&e.to_string()))?;
        if url.host_str().is_none_or(str::is_empty) {
            return Err(invalid("missing host"));
        }

        let parts: Vec<&str> = url
            .path()
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        let scope = match parts.as_slice() {
            [org] => Scope::Organization {
                organization: (*org).to_owned(),
            },
            [first, enterprise] if first.eq_ignore_ascii_case("enterprises") => Scope::Enterprise {
                enterprise: (*enterprise).to_owned(),
            },
            [owner, repo] => Scope::Repository {
                owner: (*owner).to_owned(),
                repository: (*repo).to_owned(),
            },
            _ => {
                return Err(invalid(
                    "should point to an enterprise, organization, or repository",
                ));
            }
        };

        let hosted = !force_ghes && is_hosted_host(url.host_str().unwrap_or_default());

        Ok(Self {
            raw,
            url,
            scope,
            hosted,
        })
    }

    /// The scope this URL registers at.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Whether this is github.com or GHE.com, as opposed to GitHub Enterprise
    /// Server.
    pub fn is_hosted(&self) -> bool {
        self.hosted
    }

    /// The URL as given (minus surrounding slashes).
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// The parsed URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Builds a GitHub REST API URL for `path`.
    ///
    /// For github.com and GHE.com this is `https://api.{host}{path}`; for
    /// GitHub Enterprise Server it is `{scheme}://{host}/api/v3{path}`.
    pub fn api_url(&self, path: &str) -> Url {
        let host = self.url.host_str().unwrap_or_default();
        let authority = match self.url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };

        let base = if self.hosted {
            if host.eq_ignore_ascii_case("www.github.com") {
                format!("{}://api.github.com", self.url.scheme())
            } else {
                format!("{}://api.{authority}", self.url.scheme())
            }
        } else {
            format!("{}://{authority}/api/v3", self.url.scheme())
        };

        let path = if path.starts_with('/') || path.is_empty() {
            path.to_owned()
        } else {
            format!("/{path}")
        };

        // The base is built from a URL that already parsed, so it is valid.
        Url::parse(&format!("{base}{path}")).expect("valid API URL")
    }

    /// Path of the endpoint that issues runner registration tokens for this
    /// scope.
    pub(crate) fn registration_token_path(&self) -> String {
        match &self.scope {
            Scope::Organization { organization } => {
                format!("/orgs/{organization}/actions/runners/registration-token")
            }
            Scope::Enterprise { enterprise } => {
                format!("/enterprises/{enterprise}/actions/runners/registration-token")
            }
            Scope::Repository { owner, repository } => {
                format!("/repos/{owner}/{repository}/actions/runners/registration-token")
            }
        }
    }
}

fn is_hosted_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host == "github.com"
        || host == "www.github.com"
        || host == "github.localhost"
        || host.ends_with(".ghe.com")
}

impl FromStr for GitHubConfigUrl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<&str> for GitHubConfigUrl {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for GitHubConfigUrl {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl fmt::Display for GitHubConfigUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<GitHubConfigUrl, Error> {
        GitHubConfigUrl::parse_with(s, false)
    }

    #[test]
    fn scopes() {
        let org = parse("https://github.com/org/").unwrap();
        assert_eq!(
            org.scope(),
            &Scope::Organization {
                organization: "org".into()
            }
        );
        assert_eq!(org.as_str(), "https://github.com/org");

        let repo = parse("https://github.com/org/repo").unwrap();
        assert_eq!(
            repo.scope(),
            &Scope::Repository {
                owner: "org".into(),
                repository: "repo".into()
            }
        );

        let ent = parse("https://github.com/Enterprises/acme").unwrap();
        assert_eq!(
            ent.scope(),
            &Scope::Enterprise {
                enterprise: "acme".into()
            }
        );
    }

    #[test]
    fn invalid() {
        for input in [
            "https://github.com",
            "https://github.com/",
            "https://github.com/a/b/c",
            "not a url",
        ] {
            assert!(
                matches!(parse(input), Err(Error::InvalidConfigUrl { .. })),
                "{input}"
            );
        }
    }

    #[test]
    fn api_urls() {
        let cases = [
            ("https://github.com/org", "https://api.github.com/x"),
            ("https://www.github.com/org", "https://api.github.com/x"),
            ("https://acme.ghe.com/org", "https://api.acme.ghe.com/x"),
            (
                "https://github.localhost/org",
                "https://api.github.localhost/x",
            ),
            (
                "https://ghes.example.com/org",
                "https://ghes.example.com/api/v3/x",
            ),
            (
                "http://127.0.0.1:8080/org",
                "http://127.0.0.1:8080/api/v3/x",
            ),
        ];
        for (input, want) in cases {
            assert_eq!(
                parse(input).unwrap().api_url("/x").as_str(),
                want,
                "{input}"
            );
        }
    }

    #[test]
    fn force_ghes() {
        let url = GitHubConfigUrl::parse_with("https://github.com/org", true).unwrap();
        assert!(!url.is_hosted());
        assert_eq!(url.api_url("/x").as_str(), "https://github.com/api/v3/x");
    }

    #[test]
    fn registration_paths() {
        assert_eq!(
            parse("https://github.com/org")
                .unwrap()
                .registration_token_path(),
            "/orgs/org/actions/runners/registration-token"
        );
        assert_eq!(
            parse("https://github.com/org/repo")
                .unwrap()
                .registration_token_path(),
            "/repos/org/repo/actions/runners/registration-token"
        );
        assert_eq!(
            parse("https://github.com/enterprises/e")
                .unwrap()
                .registration_token_path(),
            "/enterprises/e/actions/runners/registration-token"
        );
    }
}
