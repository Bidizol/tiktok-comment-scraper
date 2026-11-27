FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --gid 10001 scraper \
    && useradd --uid 10001 --gid scraper --create-home --shell /usr/sbin/nologin scraper \
    && mkdir -p /var/lib/tiktok-scraper/cache /var/lib/tiktok-scraper/results /var/lib/tiktok-scraper/error_reports \
    && chown -R scraper:scraper /var/lib/tiktok-scraper

COPY --chown=scraper:scraper . /app

USER scraper:scraper

CMD ["python", "tiktokscraper-1.py"]
