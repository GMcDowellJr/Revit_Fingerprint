from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class ResolutionSpec:
    """Declares that item keys contain sig_hashes resolvable via sibling domains."""

    key_exact: Optional[str] = None
    key_prefix: Optional[str] = None
    key_suffix: Optional[str] = None
    source_domain: str = ""
    name_path: str = "label.display"


@dataclass
class DomainProfile:
    """Declarative profile for comparing one domain family."""

    name: str
    domains: List[str] = field(default_factory=list)
    valid_keys_by_domain: Dict[str, Optional[Set[str]]] = field(default_factory=dict)
    suppress_keys: Set[str] = field(default_factory=set)
    resolution_specs: List[ResolutionSpec] = field(default_factory=list)
    bucket_strategy: str = "sig_basis"
    match_strategy: str = "label_display"

    def build_resolution_maps(
        self,
        raw_a: Dict[str, Any],
        raw_b: Dict[str, Any],
    ) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
        return (self._build_maps_for_file(raw_a), self._build_maps_for_file(raw_b))

    def _build_maps_for_file(self, raw: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        maps = {}
        for spec in self.resolution_specs:
            domain = spec.source_domain
            records = []
            try:
                payload = self._get_domain_payload(raw, domain)
                if isinstance(payload, dict):
                    records = payload.get("records", []) or []
            except Exception:
                pass
            mapping = {}
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                sh = (rec.get("sig_hash") or "").strip()
                if not sh:
                    continue
                name = self._extract_name(rec, spec.name_path)
                if name:
                    mapping[sh] = name
            maps[domain] = mapping
        return maps

    def _get_domain_payload(self, raw: Dict[str, Any], domain: str) -> Any:
        if not isinstance(raw, dict):
            return None

        if "_domains" in raw:
            return raw.get(domain)

        domains_obj = raw.get("domains")
        if isinstance(domains_obj, dict):
            return domains_obj.get(domain)

        return raw.get(domain)

    def _extract_name(self, rec: Dict[str, Any], name_path: str) -> str:
        try:
            parts = name_path.split(".")
            node = rec
            for part in parts:
                if not isinstance(node, dict):
                    return ""
                node = node.get(part, "")
            return str(node) if node else ""
        except Exception:
            return ""

    def resolve_value(self, value: str, item_key: str, maps: Dict[str, Dict[str, str]]) -> str:
        if not value:
            return value
        for spec in self.resolution_specs:
            if self._key_matches_spec(item_key, spec):
                mapping = maps.get(spec.source_domain, {})
                if value in mapping:
                    return mapping[value]
                if len(value) == 32 and all(c in "0123456789abcdef" for c in value):
                    return "{} (unresolved)".format(value)
                return value
        return value

    def _key_matches_spec(self, key: str, spec: ResolutionSpec) -> bool:
        if spec.key_exact is not None:
            return key == spec.key_exact
        if spec.key_prefix is not None and spec.key_suffix is not None:
            return key.startswith(spec.key_prefix) and key.endswith(spec.key_suffix)
        return False

    def classify_bucket(
        self,
        item_key: str,
        record_a: Optional[Dict[str, Any]],
        record_b: Optional[Dict[str, Any]],
    ) -> str:
        if self.bucket_strategy == "sig_basis":
            return self._classify_sig_basis(item_key, record_a, record_b)
        if self.bucket_strategy == "phase2":
            return self._classify_phase2(item_key, record_a, record_b)
        return "other"

    def _classify_sig_basis(self, key, rec_a, rec_b):
        sem_keys = set()
        for rec in (rec_a, rec_b):
            if rec is None:
                continue
            keys_used = (rec.get("sig_basis") or {}).get("keys_used") or []
            sem_keys.update(keys_used)
        if key in sem_keys:
            return "semantic"
        return self._classify_phase2(key, rec_a, rec_b)

    def _classify_phase2(self, key, rec_a, rec_b):
        for rec in (rec_a, rec_b):
            if rec is None:
                continue
            phase2 = rec.get("phase2") or {}
            for bucket_name, bucket_key in (
                ("coordination", "coordination_items"),
                ("cosmetic", "cosmetic_items"),
                ("unknown", "unknown_items"),
            ):
                items = phase2.get(bucket_key) or []
                for item in items:
                    item_key = ""
                    if isinstance(item, dict):
                        item_key = item.get("k")
                    else:
                        item_key = item
                    if item_key == key:
                        return bucket_name
        return "other"

    def is_key_valid_for_domain(self, key: str, domain: str) -> bool:
        valid = self.valid_keys_by_domain.get(domain)
        if valid is None:
            return True
        return key in valid

    def reconstruct(
        self,
        matched_pairs: List[Dict[str, Any]],
        raw_a: Dict[str, Any],
        raw_b: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return matched_pairs

    def get_deferred_domains(self) -> List[str]:
        return []

    def get_hash_resolution_meta(
        self,
        maps_a: Dict[str, Dict[str, str]],
        maps_b: Dict[str, Dict[str, str]],
    ) -> Dict[str, Any]:
        meta = {}
        for spec in self.resolution_specs:
            domain = spec.source_domain
            meta["{}_map_a_size".format(domain)] = len(maps_a.get(domain, {}))
            meta["{}_map_b_size".format(domain)] = len(maps_b.get(domain, {}))
        return meta
