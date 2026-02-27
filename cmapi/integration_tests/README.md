# cmapi Integration Tests

These integration tests are designed to work with a **real MariaDB ColumnStore cluster** provisioned in the cloud.
To run them, you **must first set up the cluster** using the [`columnstore-ansible-aws`](https://github.com/mariadb-corporation/columnstore-ansible-aws) project.

---

## Overview

The testing process consists of two main stages:

1. **Provisioning a real cluster** using `columnstore-ansible-aws`.
2. **Running integration tests** that connect to that cluster via SSH.

---

## Prerequisites

### Install dev dependencies
Install pytest and fabric in CMAPI virtual environment:

```bash
source dev_tools/activate
pip install -r requirements-dev.txt
```

### Setup `columnstore-ansible-aws`
Before running the tests, you must clone and set up the [`columnstore-ansible-aws`](https://github.com/mariadb-corporation/columnstore-ansible-aws) project.

See the [`columnstore-ansible-aws` README](https://github.com/mariadb-corporation/columnstore-ansible-aws/blob/master/README.md) for detailed setup instructions (there are just a few steps).

---

## Running the Tests

### 1. Activate CMAPI venv

```bash
source dev_tools/activate
```

---

### 2. Run the tests
Tests extract info about the hosts of the cluster and the SSH keys from the Terraform, so you'll need to specify the path to the `columnstore-ansible-aws` directory using the `--terraform-dir` flag.

At the first run, you will need to create a new cluster, so pass the `--create-cluster` flag:

```bash
pytest integration_tests --terraform-dir <path-to-columnstore-ansible-aws-dir> --create-cluster
```
Cluster will be created and provisioned, which may take about 20 minutes.
If cluster already exists, with this option it will be destroyed and recreated.

If `--create-cluster` is not passed, the tests assume that the cluster is already created and provisioned (to avoid creating/destroying the cluster for every test run when you are debugging, for example).

If the cluster is already created, pass only the `--terraform-dir` flag:

```bash
pytest integration_tests --terraform-dir <path-to-columnstore-ansible-aws-dir> -s
```
`-s` flag allows you to see the output of the tests in the console.

If you want to destroy the cluster after the test run, pass `--destroy-cluster` flag.

#### Example:

```bash
pytest integration_tests \
  --terraform-dir ../../columnstore-ansible-aws \
  --create-cluster
```

There is also an option to specify the SSH user for connecting to the cluster hosts: `--ssh-user`. By default, it is set to `ubuntu`, on Rocky you'll have to explicitly set it to `rocky`.

```bash

---

## Notes

* Tests communicate with real cloud resources – usage will incur costs.
