"""Event decoding delegates to Dexter's current Rust IDL types."""
import json


class Interpreters:
    @staticmethod
    def _decode(payload: str, kind: str):
        try:
            from dexter_sdk import decode_pump_log
        except ImportError as exc:
            raise RuntimeError('Install the Dexter market binding before collecting market events.') from exc
        result = decode_pump_log(payload, kind)
        return json.loads(result) if result else None

    def parse_pumpfun_transaction(self, b64_data):
        return self._decode(b64_data, 'trade')

    def parse_pumpfun_creation(self, b64_data):
        return self._decode(b64_data, 'create')
