# awss

A Textual TUI for browsing S3 across multiple AWS profiles without manual profile switching.

## Run

```bash
python -m awss.app
```

Or after install:

```bash
s3
```

You can also run:

```bash
python -m awss
```

## Tests

```bash
python -m unittest discover -s tests
```

## Options

- `--profiles` comma-separated list of AWS profiles to load (defaults to all available)
- `--profile` can be supplied multiple times
- `--region` to pin the S3 region for all profiles
