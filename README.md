# reqwest-partial-retry

Wrapper around reqwest to allow for easy partial retries

## Features
* Customizable retry policy
* Customizable retry strategy
* Customizable stream timeout
* Retries use the Range Header if possible

## Example
```rust
use futures_util::StreamExt;
use reqwest_partial_retry::ClientExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new().resumable();
    let request = client.get("http://httpbin.org/ip").build().unwrap();
    let mut stream = client
        .execute_resumable(request)
        .await?
        .bytes_stream_resumable();

    while let Some(item) = stream.next().await {
        println!("Bytes: {:?}", item?);
    }

    Ok(())
}
```

## Thanks
* [reqwest-retry](https://github.com/TrueLayer/reqwest-middleware/tree/main/reqwest-retry)
* [reqwest_resume](https://github.com/alecmocatta/reqwest_resume)
* [network-switch-hang](https://github.com/bes/network-switch-hang)
