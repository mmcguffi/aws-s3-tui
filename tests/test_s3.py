import unittest

from awss.s3 import S3Service


class TestS3Service(unittest.TestCase):
    def test_normalize_profiles(self) -> None:
        service = S3Service(profiles=["default", "dev", "dev"])
        self.assertEqual(service.profiles, [None, "dev"])


if __name__ == "__main__":
    unittest.main()
