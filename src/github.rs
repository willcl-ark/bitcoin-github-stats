use octocrab::Octocrab;
use serde::de::DeserializeOwned;
use std::process::Command;
use std::time::Duration;

const RATE_LIMIT_FLOOR: usize = 500;
const RETRY_MAX_ATTEMPTS: usize = 5;
const RETRY_BASE_DELAY_MS: u64 = 500;

pub fn build_client() -> Result<Octocrab, Box<dyn std::error::Error>> {
    let token = std::env::var("GITHUB_TOKEN").or_else(|_| {
        let out = Command::new("gh")
            .args(["auth", "token"])
            .output()
            .map_err(|e| format!("failed to run `gh auth token`: {e}"))?;
        if !out.status.success() {
            return Err::<_, Box<dyn std::error::Error>>("gh auth token failed — set GITHUB_TOKEN".into());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    })?;

    Ok(Octocrab::builder().personal_token(token).build()?)
}

pub async fn check_rate_limit(client: &Octocrab) -> Result<(), Box<dyn std::error::Error>> {
    let rate = client.ratelimit().get().await?;
    let remaining = rate.rate.remaining;

    if remaining < RATE_LIMIT_FLOOR {
        let reset = rate.rate.reset;
        let now = chrono::Utc::now().timestamp();
        let wait = (reset as i64) - now + 5;
        if wait > 0 {
            eprintln!(
                "rate limit low ({remaining} remaining), sleeping {wait}s until reset..."
            );
            tokio::time::sleep(std::time::Duration::from_secs(wait as u64)).await;
        }
    }
    Ok(())
}

pub async fn get_with_retry<T: DeserializeOwned>(
    client: &Octocrab,
    path: &str,
) -> Result<T, Box<dyn std::error::Error>> {
    let mut attempt = 1usize;
    let mut delay_ms = RETRY_BASE_DELAY_MS;

    loop {
        match client.get(path.to_string(), None::<&()>).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let retryable = is_retryable_error(&err);
                if !retryable || attempt >= RETRY_MAX_ATTEMPTS {
                    return Err(Box::new(err));
                }
                eprintln!(
                    "request failed on attempt {attempt}/{RETRY_MAX_ATTEMPTS}; retrying in {delay_ms}ms: {path}"
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
                delay_ms = delay_ms.saturating_mul(2);
            }
        }
    }
}

fn is_retryable_error(err: &octocrab::Error) -> bool {
    match err {
        octocrab::Error::GitHub { source, .. } => {
            source.status_code.as_u16() == 429 || source.status_code.is_server_error()
        }
        octocrab::Error::Hyper { source, .. } => source.is_timeout() || source.is_closed(),
        octocrab::Error::Service { .. } => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use serde_json::json;

    fn build_test_client(server: &MockServer) -> Octocrab {
        Octocrab::builder()
            .base_uri(server.url("/"))
            .unwrap()
            .personal_token("test-token".to_string())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn get_with_retry_retries_on_500_then_fails() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/retry-me");
            then.status(500).json_body(json!({
                "message": "server error"
            }));
        });

        let client = build_test_client(&server);
        let err = get_with_retry::<serde_json::Value>(&client, "/retry-me").await;
        assert!(err.is_err());
        assert!(mock.hits() >= RETRY_MAX_ATTEMPTS);
    }
}
