import hashlib
import hmac
import json


def generate_hmac_signature(payload: dict, secret: str) -> str:
    """Generate HMAC SHA256 signature for webhook payload"""
    message = json.dumps(payload, sort_keys=True).encode()
    signature = hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
    return signature


def verify_hmac_signature(payload: dict, signature: str, secret: str) -> bool:
    """Verify HMAC signature"""
    expected_signature = generate_hmac_signature(payload, secret)
    return hmac.compare_digest(signature, expected_signature)
