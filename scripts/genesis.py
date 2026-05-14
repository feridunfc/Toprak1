"""
scripts/genesis.py
IRONCLAD — Operational bootstrap CLI.

Commands
--------
  generate-ledger-key   Generate an Ed25519 key pair for SignedLedger.
                        Writes private key to .pem, public key to .pub.pem
                        and optionally exports env-var snippet.

Usage
-----
  python scripts/genesis.py generate-ledger-key
  python scripts/genesis.py generate-ledger-key --out-dir /secrets --env-snippet
  python scripts/genesis.py generate-ledger-key --key-id prod-2025-03
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("genesis")


# ---------------------------------------------------------------------------
# Key generation
# ---------------------------------------------------------------------------


def _require_cryptography() -> None:
    """Fail fast with a clear message if cryptography is not installed."""
    try:
        import cryptography  # noqa: F401
    except ImportError as exc:
        logger.critical(
            "cryptography package not found. Install with: pip install cryptography>=42.0"
        )
        raise SystemExit(1) from exc


def cmd_generate_ledger_key(args: argparse.Namespace) -> None:
    """
    Generate an Ed25519 signing key pair for the IRONCLAD SignedLedger.

    Output files:
      <out_dir>/<key_id>.private.pem  — PKCS8 PEM, 0600, NEVER commit
      <out_dir>/<key_id>.public.pem   — SubjectPublicKeyInfo PEM, safe to distribute

    Args:
        args: Parsed CLI namespace.

    Raises:
        SystemExit: On any unrecoverable error (missing dep, permission denied, …).
    """
    _require_cryptography()

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        PublicFormat,
    )

    out_dir = Path(args.out_dir).resolve()
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.critical("Cannot create output directory %s: %s", out_dir, exc)
        raise SystemExit(1) from exc

    # Derive a timestamped key ID when not provided
    key_id: str = args.key_id or (f"ledger-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}")

    private_key_path = out_dir / f"{key_id}.private.pem"
    public_key_path = out_dir / f"{key_id}.public.pem"

    # Guard: never silently overwrite an existing private key
    if private_key_path.exists() and not args.force:
        logger.error("Private key already exists: %s — use --force to overwrite", private_key_path)
        raise SystemExit(1)

    logger.info("Generating Ed25519 key pair (key_id=%s)…", key_id)

    try:
        private_key = Ed25519PrivateKey.generate()
    except Exception as exc:
        logger.critical("Key generation failed: %s", exc, exc_info=True)
        raise SystemExit(1) from exc

    # Serialize private key — PKCS8 PEM, unencrypted
    private_pem: bytes = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )

    # Serialize public key — SubjectPublicKeyInfo PEM
    public_pem: bytes = private_key.public_key().public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    )

    try:
        # Write private key — mode 0o600 (owner read/write only)
        private_key_path.write_bytes(private_pem)
        private_key_path.chmod(0o600)
        logger.info("Private key → %s (mode 0600)", private_key_path)

        # Write public key — mode 0o644
        public_key_path.write_bytes(public_pem)
        public_key_path.chmod(0o644)
        logger.info("Public key  → %s (mode 0644)", public_key_path)

    except OSError as exc:
        logger.critical("Failed to write key files: %s", exc, exc_info=True)
        raise SystemExit(1) from exc

    # Optionally print env-var snippet for .env / Vault / K8s secret
    if args.env_snippet:
        import base64

        priv_b64 = base64.b64encode(private_pem).decode()
        pub_b64 = base64.b64encode(public_pem).decode()

        snippet = (
            "\n# ── IRONCLAD Ledger Key env-vars ──────────────────────────────\n"
            f"# Key ID : {key_id}\n"
            f"# Generated: {datetime.now(timezone.utc).isoformat()}\n"
            "#\n"
            "# WARNING: never commit LEDGER_PRIVATE_KEY_B64 to source control!\n"
            "#\n"
            f"HFA_LEDGER_KEY_ID={key_id}\n"
            f"HFA_LEDGER_PRIVATE_KEY_B64={priv_b64}\n"
            f"HFA_LEDGER_PUBLIC_KEY_B64={pub_b64}\n"
            "# ───────────────────────────────────────────────────────────────\n"
        )
        sys.stdout.write(snippet + "\n")

    logger.info(
        "Key pair ready. Add HFA_LEDGER_KEY_ID=%s and load PEM files into your secret manager.",
        key_id,
    )


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="genesis",
        description="IRONCLAD bootstrap CLI — operational key/config management.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── generate-ledger-key ──────────────────────────────────────────────
    glk = subparsers.add_parser(
        "generate-ledger-key",
        help="Generate Ed25519 key pair for the IRONCLAD SignedLedger.",
    )
    glk.add_argument(
        "--out-dir",
        default="secrets",  # override with --out-dir or GENESIS_KEY_DIR env var
        metavar="DIR",
        help="Directory to write key files (default: ./secrets or $GENESIS_KEY_DIR).",
    )
    glk.add_argument(
        "--key-id",
        default=None,
        metavar="ID",
        help=("Logical key identifier, e.g. 'prod-2025-03'. Defaults to 'ledger-<timestamp>'."),
    )
    glk.add_argument(
        "--env-snippet",
        action="store_true",
        default=False,
        help="Print base64 env-var snippet for use in .env / Vault / K8s Secret.",
    )
    glk.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing key files (dangerous — only for key rotation).",
    )
    glk.set_defaults(func=cmd_generate_ledger_key)

    return parser


def main() -> None:
    """Entry point — dispatches to sub-command handler."""
    parser = _build_parser()
    args = parser.parse_args()

    try:
        args.func(args)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as exc:
        logger.critical("Unhandled exception in genesis: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
