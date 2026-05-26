# Security Policy

## Reporting

If you discover a security issue (credential leak, unsafe scraping, injection in CSV paths), please open a private report via GitHub Security Advisories or email the maintainer listed in the repository.

Do not commit:

- `.env` files with API keys or Telegram tokens
- Broker login credentials
- Personal portfolio data you do not intend to share

## Safe usage

- Treat all outputs as **research only**, not trading instructions.
- Run fetchers responsibly; respect source rate limits and terms of use.
- Verify file paths passed to `SCREENER_CSV` and enrichment CSVs are trusted.
