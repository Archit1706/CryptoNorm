"""Dashboard service: run the FastAPI app via uvicorn.

  python -m cryptonorm.services.run_api
"""

from __future__ import annotations

import uvicorn

from cryptonorm.common.config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "cryptonorm.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
