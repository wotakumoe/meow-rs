# meow-rs

A Rust implementation of the torrent scraper for nyaa.si.

## Usage

```bash
cargo run -- <search_term>
```

Or build and run the binary:

```bash
cargo build --release
./target/release/meow-rs <search_term>
```

## Features

- Scrapes nyaa.si for torrents matching the search term
- Downloads .torrent files with cleaned filenames
- Minimal dependencies and fast execution

## Dependencies

- `reqwest` - HTTP client for web scraping and downloading
- `scraper` - HTML parsing and CSS selector support
- `regex` - Pattern matching for filename cleaning
- `urlencoding` - URL encoding for search terms