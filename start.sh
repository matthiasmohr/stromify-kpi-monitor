#!/usr/bin/env sh
# Startup-Script für Railway (und beliebige Container-Deployments).
#
# Streamlit liest die OIDC-Login-Konfiguration (st.login / st.user) ausschließlich
# aus .streamlit/secrets.toml. Auf Railway gibt es aber nur Env-Variablen, daher
# generieren wir die secrets.toml hier beim Start aus den AUTH0_LOGIN_* Variablen.
#
# Lokal (Entwicklung) kann man stattdessen .streamlit/secrets.toml direkt anlegen
# und die App ganz normal mit `streamlit run app.py` starten – dann greift dieser
# Block nicht, weil die Variablen fehlen.
set -e

if [ -n "$AUTH0_LOGIN_CLIENT_ID" ] && [ -n "$AUTH0_LOGIN_DOMAIN" ]; then
  echo "→ Generiere .streamlit/secrets.toml aus AUTH0_LOGIN_* Env-Variablen"
  mkdir -p .streamlit
  cat > .streamlit/secrets.toml <<EOF
[auth]
redirect_uri = "${AUTH0_REDIRECT_URI}"
cookie_secret = "${AUTH0_COOKIE_SECRET}"
client_id = "${AUTH0_LOGIN_CLIENT_ID}"
client_secret = "${AUTH0_LOGIN_CLIENT_SECRET}"
server_metadata_url = "https://${AUTH0_LOGIN_DOMAIN}/.well-known/openid-configuration"
EOF
else
  echo "→ AUTH0_LOGIN_* nicht gesetzt – nutze vorhandene .streamlit/secrets.toml (falls vorhanden)"
fi

exec streamlit run app.py --server.port "$PORT" --server.address 0.0.0.0
