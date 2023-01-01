---
title: Upgrade Databend
sidebar_label: Upgrade Databend
description:
  Upgrade databend-query or databend-meta without downtime
---

This topic explains how to upgrade Databend.

## General Principles

The upgrade of Databend follows these principles:

- A Databend upgrade literally refers to upgrading databend-query and/or databend-meta to a newer version.
- When you upgrade Databend, you upgrade databend-query and databend-meta separately. You can also choose to upgrade one of them only, as long as the new version is compatible with the other one. [Check Compatibility](#check-compatibility) before you upgrade.
- Generally, rollback is not supported. It is not possible to revert to a previous version after an upgrade. This is because a new version usually brings underlying data format changes that might cause incompatibility with the previous version.

## Check Compatibility

Databend highly recommends that you check compatibility between databend-meta and databend-query before upgrading just one of them. See [Query-Meta Compatibility](10-compatibility.md) for how to do that.

:::note
New Databend versions with breaking changes might lead to incompatibilities with an earlier version. In this case, Databend will provide detailed instructions in the release log or blog for upgrading from an incompatible version.
:::

## Upgrade databend-query

Kill the old databend-query and start the new version in each node:

```shell
# Shutdown old binary
killall databend-query

# Bring up new binary
databend-query -c ...
```
After the new version starts, check the databend-query log to make sure no errors occurred during the upgrade.

## Upgrade databend-meta

Kill the old databend-meta and start the new version in each node:

```shell
# Shutdown old binary
killall databend-meta

# Bring up new binary
databend-meta -c ...
```
After the new version starts, check the databend-query and databend-meta logs to make sure no errors occurred during the upgrade.