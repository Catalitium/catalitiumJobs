from urllib import request, parse
params = parse.urlencode({"title": "engineer", "per_page": "5"})
url = f"https://catalitium-jobs.fly.dev/api/jobs?{params}"
with request.urlopen(url, timeout=15) as resp:
    print(resp.status)
    body = resp.read().decode('utf-8')
    print(body[:2000])
