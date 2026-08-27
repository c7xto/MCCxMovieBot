import sys
import types
import unittest
from types import SimpleNamespace


try:
    import rapidfuzz  # noqa: F401
except ImportError:
    rapidfuzz_stub = types.ModuleType("rapidfuzz")
    rapidfuzz_stub.fuzz = SimpleNamespace(
        ratio=lambda *_args, **_kwargs: 100,
        WRatio=lambda *_args, **_kwargs: 100,
        token_sort_ratio=lambda *_args, **_kwargs: 100,
    )
    rapidfuzz_stub.process = SimpleNamespace(extract=lambda *_args, **_kwargs: [])
    sys.modules["rapidfuzz"] = rapidfuzz_stub

from database.db import InsecureMongoURIError, mongo_tls_options


class MongoTlsPolicyTests(unittest.TestCase):
    def test_srv_and_explicit_tls_use_validated_ca(self):
        srv = mongo_tls_options("mongodb+srv://user:pass@cluster.example/test")
        standard = mongo_tls_options(
            "mongodb://db.example:27017/test?tls=true"
        )
        self.assertTrue(srv["tls"])
        self.assertTrue(standard["tls"])
        self.assertTrue(srv["tlsCAFile"])

    def test_remote_plaintext_and_invalid_certificates_are_rejected(self):
        for uri in (
            "mongodb://db.example:27017/test",
            "mongodb://db.example:27017/test?tls=false",
            "mongodb+srv://cluster.example/test?tlsAllowInvalidCertificates=true",
        ):
            with self.subTest(uri=uri):
                with self.assertRaises(InsecureMongoURIError):
                    mongo_tls_options(uri)

    def test_plaintext_is_limited_to_loopback(self):
        self.assertEqual(mongo_tls_options("mongodb://localhost:27017/test"), {})
        self.assertEqual(mongo_tls_options("mongodb://127.0.0.2/test"), {})
        self.assertEqual(mongo_tls_options("mongodb://[::1]:27017/test"), {})

    def test_development_override_is_explicit(self):
        self.assertEqual(
            mongo_tls_options(
                "mongodb://dev.example/test",
                allow_insecure_development=True,
            ),
            {},
        )


if __name__ == "__main__":
    unittest.main()
