use rayon::prelude::*;
use regex::Regex;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <search_term_or_url>", args[0]);
        std::process::exit(1);
    }

    let input = &args[1];
    let url = if input.starts_with("https://nyaa.si") {
        input.to_string()
    } else {
        format!(
            "https://nyaa.si/?f=0&c=0_0&q={}",
            urlencoding::encode(input)
        )
    };

    match scrape_torrents(&url) {
        Ok(_) => println!("Scraping completed successfully"),
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn scrape_torrents(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create directory structure based on URL
    let dir_name = create_directory_from_url(url)?;

    let client = Arc::new(Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        .build()?);

    // Get base URL without page parameter
    let base_url = get_base_url_without_page(url);

    // First, discover all available pages
    let pages = discover_all_pages(&base_url, &client)?;
    println!("Discovered {} pages to scrape", pages.len());

    // Process pages in parallel
    let total_downloaded = Arc::new(Mutex::new(0));
    let total_size_bytes = Arc::new(Mutex::new(0u64));

    pages.par_iter().for_each(|page_num| {
        let current_url = if base_url.contains("?") {
            format!("{}&p={}", base_url, page_num)
        } else {
            format!("{}?p={}", base_url, page_num)
        };

        println!("Scraping page {}: {}", page_num, current_url);

        match client.get(&current_url).send() {
            Ok(response) => match response.text() {
                Ok(html_content) => {
                    let document = Html::parse_document(&html_content);

                    match scrape_page_parallel(&document, &client, &dir_name, &total_size_bytes) {
                        Ok(page_downloads) => {
                            if page_downloads > 0 {
                                println!(
                                    "Downloaded {} torrents from page {}",
                                    page_downloads, page_num
                                );
                                let mut total = total_downloaded.lock().unwrap();
                                *total += page_downloads;
                            }
                        }
                        Err(e) => eprintln!("Error scraping page {}: {}", page_num, e),
                    }
                }
                Err(e) => eprintln!("Error reading response from page {}: {}", page_num, e),
            },
            Err(e) => eprintln!("Error fetching page {}: {}", page_num, e),
        }
    });

    let final_total = *total_downloaded.lock().unwrap();
    let final_size_bytes = *total_size_bytes.lock().unwrap();
    println!("Total torrents downloaded: {}", final_total);
    println!("total batch size: {}", format_size(final_size_bytes));
    Ok(())
}

fn get_base_url_without_page(url: &str) -> String {
    // Remove existing page parameter if it exists
    let url_without_page = if url.contains("&p=") {
        url.split("&p=").next().unwrap_or(url)
    } else if url.contains("?p=") {
        url.split("?p=").next().unwrap_or(url)
    } else {
        url
    };
    url_without_page.to_string()
}

fn discover_all_pages(
    base_url: &str,
    client: &Arc<Client>,
) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let mut pages = Vec::new();
    let mut page = 1;

    loop {
        let current_url = if base_url.contains("?") {
            format!("{}&p={}", base_url, page)
        } else {
            format!("{}?p={}", base_url, page)
        };

        let response = client.get(&current_url).send()?;
        let html_content = response.text()?;
        let document = Html::parse_document(&html_content);

        // Check if page has torrents
        let row_selector = Selector::parse("table tr, tbody tr")?;
        let title_selector = Selector::parse("td:nth-child(2) a[title]:not(.comments)")?;

        let mut has_torrents = false;
        for row in document.select(&row_selector) {
            if row.select(&title_selector).next().is_some() {
                has_torrents = true;
                break;
            }
        }

        if !has_torrents {
            break;
        }

        pages.push(page);
        page += 1;

        // Quick delay to be respectful during discovery
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    Ok(pages)
}

fn scrape_page_parallel(
    document: &Html,
    client: &Arc<Client>,
    dir_name: &str,
    total_size_bytes: &Arc<Mutex<u64>>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let row_selector = Selector::parse("table tr, tbody tr").unwrap();
    let title_selector = Selector::parse("td:nth-child(2) a[title]:not(.comments)").unwrap();
    let link_selector = Selector::parse("td:nth-child(3) a[href^='/download/']").unwrap();
    let size_selector = Selector::parse("td:nth-child(4)").unwrap();

    // Collect all torrents from the page first
    let mut torrents = Vec::new();

    for row in document.select(&row_selector) {
        if let (Some(title_elem), Some(link_elem), Some(size_elem)) = (
            row.select(&title_selector).next(),
            row.select(&link_selector).next(),
            row.select(&size_selector).next(),
        ) {
            let title = title_elem
                .value()
                .attr("title")
                .unwrap_or("Unknown")
                .to_string();
            let download_link = link_elem.value().attr("href").unwrap_or("").to_string();
            let size_text = size_elem.text().collect::<Vec<_>>().join("").trim().to_string();

            if !download_link.is_empty() {
                let full_url = format!("https://nyaa.si{}", download_link);
                
                // Parse and add size to total
                if let Ok(size_bytes) = parse_size(&size_text) {
                    let mut total = total_size_bytes.lock().unwrap();
                    *total += size_bytes;
                }
                
                torrents.push((full_url, title));
            }
        }
    }

    // Download torrents in parallel
    let downloads = Arc::new(Mutex::new(0));

    torrents.par_iter().for_each(|(url, title)| {
        if let Err(e) = download_torrent_parallel(url, title, dir_name, client) {
            eprintln!("Failed to download {}: {}", title, e);
        } else {
            let mut count = downloads.lock().unwrap();
            *count += 1;
        }
    });

    Ok(*downloads.lock().unwrap())
}

fn scrape_page(
    document: &Html,
    _client: &Client,
    dir_name: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let row_selector = Selector::parse("table tr, tbody tr")?;
    let title_selector = Selector::parse("td:nth-child(2) a[title]:not(.comments)")?;
    let link_selector = Selector::parse("td:nth-child(3) a[href^='/download/']")?;

    let mut downloads = 0;

    for row in document.select(&row_selector) {
        if let (Some(title_elem), Some(link_elem)) = (
            row.select(&title_selector).next(),
            row.select(&link_selector).next(),
        ) {
            let title = title_elem.value().attr("title").unwrap_or("Unknown");
            let download_link = link_elem.value().attr("href").unwrap_or("");

            if !download_link.is_empty() {
                let full_url = format!("https://nyaa.si{}", download_link);
                download_torrent(&full_url, title, &dir_name)?;
                downloads += 1;
            }
        }
    }

    Ok(downloads)
}

fn create_directory_from_url(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Create base torrents directory
    let base_dir = "torrents";
    if !Path::new(base_dir).exists() {
        fs::create_dir(base_dir)?;
    }

    // Extract meaningful part of URL for directory name
    let url_part = if url.contains("user/") {
        // For user URLs like https://nyaa.si/user/NekoTrix
        url.replace("https://nyaa.si/", "").replace("/", "_")
    } else if url.contains("?q=") {
        // For search URLs, extract the search term
        let query_start = url.find("?q=").unwrap_or(0) + 3;
        let query_end = url[query_start..]
            .find("&")
            .unwrap_or(url.len() - query_start);
        format!(
            "search_{}",
            urlencoding::decode(&url[query_start..query_start + query_end])?
        )
    } else {
        // Fallback: use timestamp
        format!(
            "torrents_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
        )
    };

    // Clean directory name
    let re = Regex::new(r"[^\w\-_]")?;
    let clean_name = re.replace_all(&url_part, "_").to_string();
    let dir_path = format!("{}/{}", base_dir, clean_name);

    // Create the directory
    if !Path::new(&dir_path).exists() {
        fs::create_dir_all(&dir_path)?;
    }

    Ok(dir_path)
}

fn download_torrent_parallel(
    url: &str,
    title: &str,
    dir_path: &str,
    client: &Arc<Client>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.get(url).send()?;
    let content = response.bytes()?;

    // Clean filename
    let re = Regex::new(r"[^\w\-_\. ]")?;
    let clean_title = re.replace_all(title, "").to_string();
    let filename = format!("{}.torrent", clean_title.trim());
    let full_path = format!("{}/{}", dir_path, filename);

    fs::write(&full_path, &content)?;
    println!("Downloaded: {}", full_path);

    Ok(())
}

fn download_torrent(
    url: &str,
    title: &str,
    dir_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let response = client.get(url).send()?;
    let content = response.bytes()?;

    // Clean filename
    let re = Regex::new(r"[^\w\-_\. ]")?;
    let clean_title = re.replace_all(title, "").to_string();
    let filename = format!("{}.torrent", clean_title.trim());
    let full_path = format!("{}/{}", dir_path, filename);

    fs::write(&full_path, &content)?;
    println!("Downloaded: {}", full_path);

    Ok(())
}

fn parse_size(size_str: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let size_str = size_str.trim();
    if size_str.is_empty() {
        return Ok(0);
    }

    let parts: Vec<&str> = size_str.split_whitespace().collect();
    if parts.len() != 2 {
        return Err("Invalid size format".into());
    }

    let value: f64 = parts[0].parse()?;
    let unit = parts[1].to_uppercase();

    let multiplier = match unit.as_str() {
        "B" => 1,
        "KB" | "KIB" => 1024,
        "MB" | "MIB" => 1024 * 1024,
        "GB" | "GIB" => 1024 * 1024 * 1024,
        "TB" | "TIB" => 1024u64 * 1024 * 1024 * 1024,
        _ => return Err(format!("Unknown unit: {}", unit).into()),
    };

    Ok((value * multiplier as f64) as u64)
}

fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.1} {}", size, UNITS[unit_index])
}
