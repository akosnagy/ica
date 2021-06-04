# ica
Infra Coding Assignment

Use the bellow command to create the conda environment:

```conda env create -n ica -f environment.yaml```

Then activate it
#### Linux:

```source activate ica```

#### Windows:

```conda activate ica```

Get usage help by:

```python data_collector.py --help```


## Notes

Be carefull with the ```-p all -c all``` combination as it may lead to ```Handshake status 429 Too Many Requests``` errors.
