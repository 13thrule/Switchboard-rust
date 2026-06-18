# TLS / Reverse Proxy Example

Switchboard itself intentionally avoids embedding TLS and authentication to keep the core broker minimal and fast. For production, run Switchboard behind a reverse proxy that handles TLS and (optionally) basic auth.

Example using Nginx:

```
server {
  listen 443 ssl;
  server_name example.com;

  ssl_certificate /etc/ssl/certs/fullchain.pem;
  ssl_certificate_key /etc/ssl/private/privkey.pem;

  location / {
    proxy_pass http://127.0.0.1:7777;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
  }
}
```

For WebSocket support ensure the `Upgrade` and `Connection` headers are forwarded as above.

Basic auth (Nginx) example:

```
location / {
  auth_basic "Restricted";
  auth_basic_user_file /etc/nginx/.htpasswd;
  proxy_pass http://127.0.0.1:7777;
  proxy_set_header Upgrade $http_upgrade;
  proxy_set_header Connection "upgrade";
}
```

Alternatively, use Caddy for automatic LetsEncrypt and simple reverse-proxy configuration:

```
example.com {
  reverse_proxy 127.0.0.1:7777
}
```
