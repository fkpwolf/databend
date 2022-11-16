---
title: Visualization Databend Data in Jupyter Notebook
sidebar_label: Jupyter
description:
  Visualization Databend data in Jupyter Notebook.
---


<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/integration-jupyter-databend.png" width="550"/>
</p>

## What is [Jupyter Notebook](https://jupyter.org/)?

The Jupyter Notebook is the original web application for creating and sharing computational documents. It offers a simple, streamlined, document-centric experience.

## Jupyter

### Create a Databend User

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

Create a user:
```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant privileges for the user:
```sql
GRANT ALL ON *.* TO user1;
```

See also [How To Create User](../../14-sql-commands/00-ddl/30-user/01-user-create-user.md).

### Install Jupyter Python Package

Jupyter:
```shell
pip install jupyterlab
pip install notebook
```

Dependencies:
```shell
pip install sqlalchemy
pip install pandas
```

### Run Jupyter Notebook

Download [**databend.ipynb**](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/databend.ipynb), start the notebook:
```shell
jupyter notebook
```

Run `databend.ipynb` step by step, the demo show:
<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/integration-gui-jupyter.png"/>
</p>
