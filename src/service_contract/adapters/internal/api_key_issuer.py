from __future__ import annotations

import uuid

from service_contract.application.ports import ApiKeyIssuerPort


class InternalApiKeyIssuer(ApiKeyIssuerPort):
    def issue(self, contract_id: str, api_key_name: str) -> str:
        return f"key_{uuid.uuid4()}"
