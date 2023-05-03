# AWS Cleanup Scripts

This repository contains scripts to cleanup AWS resources.

## Installation

```bash
pipenv install --dev
pipenv shell
# set bucket name in .envrc
direnv allow
# remove expired delete markers and objects from bucket
./s3_full_delete.py
```
