import json
from pathlib import Path

_CONTRACT_PATH = Path(__file__).parents[2] / "contracts" / "domain_identity_keys_v2.json"


class DomainIdentityContract:
    def __init__(self, data: dict):
        self.data = data

    @classmethod
    def load(cls):
        if not _CONTRACT_PATH.exists():
            raise FileNotFoundError(f"Domain identity contract not found: {_CONTRACT_PATH}")
        with open(_CONTRACT_PATH, "r", encoding="utf-8") as f:
            return cls(json.load(f))

    def allowed_keys_for_domain(self, domain: str) -> set[str]:
        dom = self.data.get(domain)
        if not dom:
            return set()
        keys = set(dom.get("allowed_keys", []))
        prefixes = dom.get("allowed_key_prefixes", [])
        expanded = set(keys)
        for p in prefixes:
            expanded.add(p.rstrip(".") + ".")
        return expanded

    def required_keys_for_domain(self, domain: str) -> set[str]:
        dom = self.data.get(domain)
        if not dom:
            return set()
        return set(dom.get("required_keys", []))

    def is_key_allowed(self, domain: str, key: str) -> bool:
        allowed = self.allowed_keys_for_domain(domain)
        if not allowed:
            return False
        if key in allowed:
            return True
        return any(key.startswith(p) for p in allowed if p.endswith("."))
