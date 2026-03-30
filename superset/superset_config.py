# Superset custom config
import os

# Mapbox 설정 (지도 표시용)
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY")

ROW_LIMIT = 5000
SUPERSET_WEBSERVER_TIMEOUT = 300



FEATURE_FLAGS = {
    "ENABLE_JAVASCRIPT_CONTROLS": True,
    "DASHBOARD_RBAC": True  # 대시보드별로 접근 권한을 제어할 수 있는 기능
}

PUBLIC_ROLE_LIKE = "Gamma"

# 로컬 개발을 위해 엄격한 웹 보안 정책(CSP) 임시 비활성화
TALISMAN_ENABLED = False

TALISMAN_CONFIG = {
    "content_security_policy": {
        "default-src": ["'self'"],
        "script-src": [
            "'self'",
            "'unsafe-eval'", 
            "'unsafe-inline'",
            "'strict-dynamic'",
        ],
      
    }
}