# App Review Trend Analyzer

A production-ready, **app-agnostic** tool to analyze Google Play Store reviews, extract topics using AI (Groq), and generate 30-day trend reports.

## Features

- **Works for ANY app** - Swiggy, Zomato, Uber, Netflix, WhatsApp, etc.
- **Smart CSV caching** - Only scrapes new reviews when needed
- **AI-powered topic extraction** - Uses Groq LLM for intelligent analysis
- **Auto-discovering taxonomy** - New topics are discovered automatically
- **30-day trend reports** - CSV output with topics × dates matrix

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd app-review-trend-analyzer

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your GROQ_API_KEY
```

## Usage

```bash
# Analyze Swiggy
python main.py --app-id in.swiggy.android --target-date 2024-07-01

# Analyze Zomato
python main.py --app-id com.application.zomato --target-date 2024-12-15

# Analyze Netflix
python main.py --app-id com.netflix.mediaclient --target-date 2024-11-20

# Re-run with newer date (uses cached CSV, appends new data)
python main.py --app-id in.swiggy.android --target-date 2024-08-01
```

## Output

Reports are saved to `output/{app_id}/trend_report_{target_date}.csv`

Example output structure:
```
Topic,2024-06-01,2024-06-02,...,2024-07-01
Delivery/Service Delay,23,18,...,25
App Technical Issue,15,12,...,20
Payment/Refund Issue,8,10,...,7
```

## Project Structure

```
app-review-trend-analyzer/
├── config/
│   ├── settings.py          # General settings
│   └── seed_topics.py       # Generic seed topics
├── data/apps/{app_id}/
│   ├── all_reviews.csv      # Master CSV with ALL scraped reviews
│   ├── processed/           # Daily batch JSON files
│   └── taxonomy/            # App-specific topic taxonomy
├── output/{app_id}/
│   └── trend_report_*.csv   # Generated reports
├── src/
│   ├── scraper.py           # Smart scraper with caching
│   ├── agents/
│   │   ├── topic_extractor.py
│   │   ├── topic_mapper.py
│   │   └── consolidator.py
│   ├── utils/
│   │   ├── logger.py
│   │   ├── date_utils.py
│   │   ├── storage.py
│   │   └── groq_client.py
│   └── report_generator.py
└── main.py                  # CLI entry point
```

## How It Works

1. **Smart Scraping** - Checks if CSV exists, only fetches new reviews
2. **Agent 1 (Extractor)** - Extracts topics from reviews using LLM
3. **Agent 2 (Mapper)** - Maps extracted topics to taxonomy
4. **Agent 3 (Consolidator)** - Counts frequencies, discovers new topics
5. **Report Generator** - Creates 30-day trend CSV

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GROQ_API_KEY` | Your Groq API key | Required |
| `GROQ_MODEL` | LLM model to use | `llama-3.1-8b-instant` |

