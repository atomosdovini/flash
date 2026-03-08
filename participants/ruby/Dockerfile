FROM ruby:3.3-slim

RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN gem install rack rackup puma sequel pg rqrcode chunky_png
COPY config.ru .

EXPOSE 3000
CMD ["puma", "-b", "tcp://0.0.0.0:3000", "-w", "2", "-t", "8:8", "--preload", "config.ru"]