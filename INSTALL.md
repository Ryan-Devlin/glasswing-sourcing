# Install or upgrade

These commands preserve the existing domain registry, screened companies, and
`.env` file.

## Stop the old version

```bash
pkill -f continuous_pipeline.py 2>/dev/null || true
pkill -f "streamlit run dashboard.py" 2>/dev/null || true
```

## Back up existing state

```bash
cd "$HOME/Desktop"
rm -rf "$HOME/Desktop/HTTPS Scraping Backup"
mkdir -p "$HOME/Desktop/HTTPS Scraping Backup"
cp -R "$HOME/Desktop/HTTPS Scraping/data" "$HOME/Desktop/HTTPS Scraping Backup/data"
cp "$HOME/Desktop/HTTPS Scraping/.env" "$HOME/Desktop/HTTPS Scraping Backup/.env"
```

## Install this build

```bash
rm -rf "$HOME/Desktop/HTTPS Scraping"
unzip "$HOME/Downloads/Glasswing_Sourcing_LEAN_FAST_STABLE_FINAL.zip" -d "$HOME/Desktop"
```

## Restore state

```bash
rm -rf "$HOME/Desktop/HTTPS Scraping/data"
cp -R "$HOME/Desktop/HTTPS Scraping Backup/data" "$HOME/Desktop/HTTPS Scraping/data"
cp "$HOME/Desktop/HTTPS Scraping Backup/.env" "$HOME/Desktop/HTTPS Scraping/.env"
```

## Run

```bash
cd "$HOME/Desktop/HTTPS Scraping"
chmod +x run_dashboard.sh diagnose.sh
./run_dashboard.sh
```

Open `http://localhost:8501`.
