# config.ru
require "json"
require "sequel"
require "securerandom"
require "uri"
require "time"
require "base64"
require "rqrcode"
require "chunky_png"

WORKER_COUNT = ENV.fetch("WEB_CONCURRENCY", "2").to_i
MAX_THREADS = ENV.fetch("MAX_THREADS", "8").to_i
DB_POOL = ENV.fetch("DB_POOL", (WORKER_COUNT * MAX_THREADS + 4).to_s).to_i

def connect_db_with_retry(database_url, retries: 30, delay_seconds: 1, max_connections: 20)
  attempt = 0
  begin
    attempt += 1
    Sequel.connect(database_url, max_connections: max_connections)
  rescue StandardError => e
    raise e if attempt >= retries
    warn "DB connect failed (attempt #{attempt}/#{retries}): #{e.class} - #{e.message}"
    sleep(delay_seconds)
    retry
  end
end

DB = connect_db_with_retry(ENV.fetch("DATABASE_URL"), max_connections: DB_POOL)

URLS   = DB[:urls]
CLICKS = DB[:clicks]

HOST = ENV.fetch("BASE_URL", "http://localhost:3000")

def json(status, body)
  [status, {"content-type"=>"application/json"}, [JSON.generate(body)]]
end

def now
  Time.now.utc
end

def valid_url?(url)
  uri = URI.parse(url)
  uri.is_a?(URI::HTTP) || uri.is_a?(URI::HTTPS)
rescue
  false
end

def valid_code?(code)
  code =~ /\A[a-zA-Z0-9]{1,16}\z/
end

def generate_code
  loop do
    code = SecureRandom.alphanumeric(6)
    return code unless URLS.where(code: code).first
  end
end

def short_url(code)
  "#{HOST}/#{code}"
end

def serialize(row)
  {
    id: row[:id],
    code: row[:code],
    url: row[:url],
    short_url: short_url(row[:code]),
    expires_at: row[:expires_at],
    created_at: row[:created_at],
    updated_at: row[:updated_at],
    click_count: row[:click_count]
  }
end

run lambda { |env|
  req = Rack::Request.new(env)
  method = req.request_method
  path = req.path_info

  # HEALTH
  if method == "GET" && path == "/health"
    return json(200, {status:"ok"})
  end

  # CREATE URL
  if method == "POST" && path == "/urls"
    data = JSON.parse(req.body.read) rescue {}
    url = data["url"]
    custom_code = data["custom_code"]
    expires_at = data["expires_at"]

    return json(400,{error:"invalid url"}) unless valid_url?(url)

    if expires_at
      expires_at = Time.parse(expires_at) rescue nil
      return json(400,{error:"invalid expires_at"}) unless expires_at && expires_at > now
    end

    return json(400,{error:"invalid custom_code"}) if custom_code && !valid_code?(custom_code)

    status = nil
    payload = nil

    DB.transaction do
      # Prevent races for idempotent create on the same URL.
      DB.fetch("SELECT pg_advisory_xact_lock(hashtext(?))", url).all

      existing = URLS.where(url: url)
        .where(Sequel.|({ expires_at: nil }, Sequel[:expires_at] > now))
        .first

      if existing
        if custom_code && existing[:code] == custom_code
          status = 409
          payload = {error:"code exists"}
        else
          status = 200
          payload = serialize(existing)
        end
        next
      end

      desired_code = custom_code

      loop do
        code = desired_code || SecureRandom.alphanumeric(6)
        begin
          URLS.insert(
            code: code,
            url: url,
            expires_at: expires_at,
            created_at: now,
            updated_at: now
          )
          row = URLS.where(code: code).first
          status = 201
          payload = serialize(row)
          break
        rescue Sequel::UniqueConstraintViolation
          # Custom code collisions must return 409; random collisions retry.
          if desired_code
            status = 409
            payload = {error:"code exists"}
            break
          end
        end
      end
    end

    return json(status, payload)
  end

  # LIST URLS
  if method == "GET" && path == "/urls"
    page = (req.params["page"] || 1).to_i
    per_page = (req.params["per_page"] || 10).to_i
    offset = (page-1)*per_page

    rows = URLS.limit(per_page).offset(offset).all
    total = URLS.count

    return json(200,{
      data: rows.map{serialize(_1)},
      meta:{page:page,per_page:per_page,total:total}
    })
  end

  # ROUTES /urls/:id/...
  if path =~ %r{^/urls/([^/]+)(.*)}
    id = $1
    suffix = $2
    row = URLS.where(id:id).first
    return json(404,{error:"not found"}) unless row

    # DETAIL
    if method=="GET" && suffix==""
      return json(200,serialize(row))
    end

    # UPDATE
    if method=="PATCH" && suffix==""
      data = JSON.parse(req.body.read) rescue {}
      updates = {}

      if data["url"]
        return json(400,{error:"invalid url"}) unless valid_url?(data["url"])
        updates[:url]=data["url"]
      end

      if data["expires_at"]
        exp = Time.parse(data["expires_at"]) rescue nil
        return json(400,{error:"invalid expires_at"}) unless exp && exp > now
        updates[:expires_at]=exp
      end

      updates[:updated_at]=now
      URLS.where(id:id).update(updates)
      row = URLS.where(id:id).first
      return json(200,serialize(row))
    end

    # DELETE
    if method=="DELETE" && suffix==""
      URLS.where(id:id).delete
      return [204,{},[]]
    end

    # STATS
    if method=="GET" && suffix=="/stats"
      clicks_day = DB.fetch(<<~SQL, id).all
        SELECT to_char(date_trunc('day', clicked_at), 'YYYY-MM-DD') AS date,
               COUNT(id)::int AS count
        FROM clicks
        WHERE url_id = ?
        GROUP BY 1
        ORDER BY 1 DESC
      SQL

      clicks_hour = DB.fetch(<<~SQL, id).all
        SELECT to_char(date_trunc('hour', clicked_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"HH24:00:00"Z"') AS hour,
               COUNT(id)::int AS count
        FROM clicks
        WHERE url_id = ?
        GROUP BY 1
        ORDER BY 1 DESC
      SQL

      return json(200,{
        id: row[:id],
        code: row[:code],
        url: row[:url],
        click_count: row[:click_count],
        clicks_per_day: clicks_day,
        clicks_per_hour: clicks_hour
      })
    end

    # QR
    if method=="GET" && suffix=="/qr"
      qr = RQRCode::QRCode.new(short_url(row[:code]))
      png = qr.as_png(size:200)
      b64 = Base64.strict_encode64(png.to_s)
      return json(200,{qr_code:b64})
    end
  end

  # REDIRECT
  if method=="GET" && path =~ %r{^/([a-zA-Z0-9]+)$}
    code = $1
    row = URLS.select(:id, :url, :expires_at).where(code:code).first
    return json(404,{error:"not found"}) unless row

    if row[:expires_at] && row[:expires_at] < now
      return json(410,{error:"expired"})
    end

    clicked_at = now
    DB.transaction do
      URLS.where(id: row[:id]).update(click_count: Sequel[:click_count] + 1)
      CLICKS.insert(url_id: row[:id], clicked_at: clicked_at)
    end

    return [301,{"location"=>row[:url]},[]]
  end

  json(404,{error:"not found"})
}