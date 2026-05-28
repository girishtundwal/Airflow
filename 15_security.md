# 15 — Airflow Security

## Security Overview

Airflow's attack surface:
- **Webserver**: publicly accessible UI/API — authentication, RBAC, TLS
- **DAG code**: Python code executed by workers — code injection risks
- **Credentials**: connections, variables, Fernet key — encryption, secrets backend
- **Workers**: execute arbitrary code — sandboxing, network policies
- **Metadata DB**: contains all state and credentials — access control, encryption at rest

---

## Authentication

### Database Authentication (default)

```python
# webserver_config.py
from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.manager import AUTH_DB

AUTH_TYPE = AUTH_DB
```

### LDAP Authentication

```python
# webserver_config.py
from flask_appbuilder.security.manager import AUTH_LDAP

AUTH_TYPE = AUTH_LDAP
AUTH_LDAP_SERVER = "ldap://ldap.company.com:389"
AUTH_LDAP_USE_TLS = True
AUTH_LDAP_BIND_USER = "cn=airflow,dc=company,dc=com"
AUTH_LDAP_BIND_PASSWORD = "ldap-password"
AUTH_LDAP_SEARCH = "dc=company,dc=com"
AUTH_LDAP_UID_FIELD = "uid"
AUTH_LDAP_FIRSTNAME_FIELD = "givenName"
AUTH_LDAP_LASTNAME_FIELD = "sn"
AUTH_LDAP_EMAIL_FIELD = "mail"
AUTH_LDAP_GROUP_FIELD = "memberOf"
AUTH_LDAP_SEARCH_FILTER = "(memberOf=cn=airflow-users,dc=company,dc=com)"

# Map LDAP groups to Airflow roles
AUTH_ROLES_MAPPING = {
    "cn=airflow-admins,dc=company,dc=com": ["Admin"],
    "cn=airflow-ops,dc=company,dc=com": ["Op"],
    "cn=airflow-viewers,dc=company,dc=com": ["Viewer"],
}
```

### OAuth / Google OAuth

```python
# webserver_config.py
from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"

OAUTH_PROVIDERS = [
    {
        "name": "google",
        "token_key": "access_token",
        "icon": "fa-google",
        "remote_app": {
            "client_id": "your-google-client-id.apps.googleusercontent.com",
            "client_secret": "your-client-secret",
            "api_base_url": "https://www.googleapis.com/oauth2/v2/",
            "client_kwargs": {"scope": "email profile"},
            "access_token_url": "https://accounts.google.com/o/oauth2/token",
            "authorize_url": "https://accounts.google.com/o/oauth2/auth",
            "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
        },
    }
]

# Map email domains to roles
AUTH_ROLES_SYNC_AT_LOGIN = True
def get_role_from_email(email):
    if email.endswith("@company.com"):
        return "Op"
    return "Viewer"
```

---

## RBAC (Role-Based Access Control)

### Default Roles

| Role | Permissions |
|------|------------|
| **Admin** | Full access — manage users, roles, connections, all DAGs |
| **Op** | Create/edit connections, variables, pools; trigger/manage all DAGs |
| **User** | View all DAGs, trigger DAGs, clear/mark tasks |
| **Viewer** | Read-only access to all DAGs and their runs |
| **Public** | No access (unauthenticated users) |

### Custom Roles

```bash
# Create custom role
airflow roles create data_team

# Add permissions to role (format: action on resource)
airflow roles add-perms data_team \
    --action can_read --resource DAGs
airflow roles add-perms data_team \
    --action can_edit --resource DAGs
airflow roles add-perms data_team \
    --action can_read --resource Task\ Instances

# Assign role to user
airflow users add-role --username alice --role data_team

# List role permissions
airflow roles list --verbose
```

---

## API Authentication

```ini
[api]
# Options: deny_all, session, basic_auth, kerberos, jwt
auth_backends = airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

# For Kerberos
# auth_backends = airflow.api.auth.backend.kerberos_auth
```

```bash
# Basic auth API call
curl -X GET \
    http://localhost:8080/api/v1/dags \
    --user "admin:password"

# With session token
TOKEN=$(curl -X POST \
    http://localhost:8080/auth/token \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"password"}' \
    | jq -r '.access_token')

curl -X GET \
    http://localhost:8080/api/v1/dags \
    -H "Authorization: Bearer $TOKEN"
```

---

## Fernet Encryption

Fernet encrypts connection passwords and variable values stored in the metadata DB.

```bash
# Generate Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Example: dSkRMhXnRiqN1vOXcF28bP3ysKXnTEyNqMCYKKVkUcQ=

# Set key
export AIRFLOW__CORE__FERNET_KEY="dSkRMhXnRiqN1vOXcF28bP3ysKXnTEyNqMCYKKVkUcQ="

# Rotate key (re-encrypts existing values with new key)
# Step 1: Add new key alongside old (comma-separated)
export AIRFLOW__CORE__FERNET_KEY="newkey123=,oldkey456="
# Step 2: Run rotation
airflow rotate-fernet-key
# Step 3: Remove old key
export AIRFLOW__CORE__FERNET_KEY="newkey123="
```

---

## TLS/SSL Setup

### Webserver with nginx reverse proxy

```nginx
# /etc/nginx/sites-available/airflow
server {
    listen 443 ssl;
    server_name airflow.company.com;
    
    ssl_certificate /etc/ssl/certs/airflow.crt;
    ssl_certificate_key /etc/ssl/private/airflow.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

server {
    listen 80;
    server_name airflow.company.com;
    return 301 https://$host$request_uri;
}
```

---

## Secure DAG Development

```python
# BAD: secrets in DAG code
DB_PASSWORD = "mysecret"   # committed to git!
API_KEY = "sk-abc123"

# BAD: bypassing connection system
import psycopg2
conn = psycopg2.connect(host="db.prod.com", password="hardcoded")

# GOOD: use Airflow connections
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id="prod_db")

# GOOD: use Variables for config
from airflow.models import Variable
api_key = Variable.get("api_key")  # stored in secrets backend

# GOOD: use environment variables
import os
api_key = os.environ.get("API_KEY")  # injected at runtime, not in code
```

---

## Kubernetes Security

```yaml
# NetworkPolicy — restrict pod communication
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: airflow-worker-policy
  namespace: airflow
spec:
  podSelector:
    matchLabels:
      component: worker
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              component: scheduler
  egress:
    - to:
        - podSelector:
            matchLabels:
              component: postgresql
      ports:
        - port: 5432
    - to:
        - namespaceSelector: {}  # allow external (for data processing)
          podSelector: {}
      ports:
        - port: 443
        - port: 80
```

```yaml
# Use K8s Secrets for credentials (not ConfigMaps)
apiVersion: v1
kind: Secret
metadata:
  name: airflow-secrets
  namespace: airflow
type: Opaque
data:
  fernet-key: <base64_encoded_key>
  db-password: <base64_encoded_password>
  secret-key: <base64_encoded_key>
```

---

## Security Best Practices

| Area | Practice |
|------|---------|
| Authentication | Use OAuth/LDAP — disable DB auth in prod |
| Secrets | Use secrets backend (Vault/AWS SM/GCP SM) — never DB variables for sensitive data |
| Fernet key | Set strong key, rotate regularly, store in secrets manager |
| Webserver | Put behind TLS reverse proxy (nginx/traefik) |
| API | Enable auth, use token-based auth, restrict to internal network |
| DAG code | Code review process, no secrets in code, no arbitrary imports |
| Network | Firewall rules, K8s NetworkPolicy, private DB/broker |
| Audit | Enable audit logging, monitor for suspicious events |
| Users | Principle of least privilege — don't give everyone Admin |
| Rotation | Rotate Fernet key, DB passwords, API tokens periodically |
