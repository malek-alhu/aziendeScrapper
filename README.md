# Scraper for Registro Aziende ATECO Codes

This script scrapes data from [Registro Aziende](https://registroaziende.it/) for a given ATECO code.

## How to Use

**Requirements:** This script requires the following Python libraries:
- beautifulsoup4
- fake-useragent
- requests
- retrying
- concurrent.futures

1. **Create a codes file:** Create a text file named `ATECO.txt` in your Google Drive directory at `/content/drive/My Drive/scraping/`. Add one ATECO code per line in this file.

2. **Run the Script:** Execute the script using Python.

## Script Functionality

The script performs the following steps:

1. **Reads ATECO codes:** It reads a list of ATECO codes from the `ATECO.txt` file.
2. **Loops through each code:** For each ATECO code:
   - Creates a log folder and log files to store scraping results and logs.
   - Opens a connection to the Registro Aziende website for the ATECO code.
   - Estimates the last page number by parsing the search results.
   - Retrieves data from all pages using asynchronous calls with `concurrent.futures.ProcessPoolExecutor`.
   - Parses each page to extract company name, city, province, revenue, and ATECO code.
   - Saves the extracted data to a text file.
   - Logs messages about the scraping process.

## Output

The script creates two files in the log folder for each ATECO code:

- `scraper_logs.txt`: Contains logs about the scraping process.
- `aziende_data.txt`: Stores the extracted company data in a tabular format.

**Note:**

- This script uses exponential backoff retry logic to handle temporary network issues.
- The script creates a separate process for scraping each ATECO code to improve performance.

I hope this README helps! Let me know if you have any questions.
