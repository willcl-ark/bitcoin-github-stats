use octocrab::Octocrab;
use std::process::Command;

const RATE_LIMIT_FLOOR: u32 = 500;

pub fn build_client() -> Result<Octocrab, Box<dyn std::error::Error>> {
    let token = std::env::var("GITHUB_TOKEN").or_else(|_| {
        let out = Command::new("gh")
            .args(["auth", "token"])
            .output()
            .map_err(|e| format!("failed to run `gh auth token`: {e}"))?;
        if !out.status.success() {
            return Err("gh auth token failed — set GITHUB_TOKEN".into());
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
