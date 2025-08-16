# meow-rs

A [Rust implementation of our](https://github.com/wotakumoe/meow) torrent scraper for nyaa.si.

## Usage

```bash
meow-rs <search_term_or_url>
```

You can pass either a search term or a nyaa.si URL:

```bash
# Search for torrents
meow-rs "anime name"

# Use a direct nyaa.si URL
meow-rs "https://nyaa.si/?f=0&c=0_0&q=anime"
```

Or build and run the binary:

```bash
cargo build --release
./target/release/meow-rs <search_term_or_url>
```

## Features

- Scrapes nyaa.si for torrents matching the search term
- **Multi-page discovery**: Automatically discovers and scrapes all available pages
- **Parallel processing**: Downloads torrents from multiple pages simultaneously using Rayon
- **Rate limiting**: Built-in rate limiting to prevent overwhelming the server (500ms between requests)
- **Retry logic**: Automatic retry with exponential backoff for failed requests
- **Directory organization**: Creates organized directory structure based on search terms or URLs
- **Size tracking**: Displays total batch size of downloaded torrents
- Downloads .torrent files with cleaned filenames
- Fast execution with minimal memory usage

## Releases

Pre-built binaries are available for download from the [releases page](https://github.com/wotakumoe/meow-rs/releases).

