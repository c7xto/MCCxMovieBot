"""Decrypt an explicitly exported MCCxBot secret configuration backup."""

import argparse
import getpass
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from plugins.config_backup import decrypt_config_export


def main():
    parser = argparse.ArgumentParser(
        description="Decrypt an MCCxBot AES-256-GCM configuration backup."
    )
    parser.add_argument("encrypted_backup", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("Refusing to overwrite an existing plaintext output file")
    passphrase = getpass.getpass("CONFIG_EXPORT_PASSPHRASE: ")
    config = decrypt_config_export(args.encrypted_backup.read_bytes(), passphrase)
    args.output.write_text(
        json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Decrypted secret config written to {args.output}")


if __name__ == "__main__":
    main()
