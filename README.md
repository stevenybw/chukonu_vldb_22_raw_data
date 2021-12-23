# Data and scripts for Chukonu's paper at VLDB '22

This repository contains the data and scripts necessary to draw the evaluation figures in Chukonu's paper.
Unfortunately, we are not able to open the source code of Chukonu for now due to a time-bounded open-source restriction by a contract.

## Dependencies

* Python 3.8
* pandas
* matplotlib
* pandasql
* scipy
* IPython

## Prepare for environment on Ubuntu 18.04.2 LTS

```bash
sudo apt install python3.8 python3.8-venv python3.8-dev libpython3.8-dev libjpeg-dev zlib1g-dev libopenblas-dev
cd ~
python3.8 -m venv python3.8-venv
source ~/python3.8-venv/bin/activate
pip install Cython
pip install wheel
pip install numpy
pip install pandas matplotlib pandasql
pip install pythran
pip install pybind11
pip install scipy
pip install IPython
```

## To draw figures

The raw data is stored in `./results`.
The `draw.py` makes figures based on the raw data.
Using the following command to generate figures:

```bash
python draw.py
```

After the command, the resulting figures is stored in `./fig`.

## Structures

* `results` Raw data
* `sparklet_experiments` Scripts
  * `sparklet_experiments/data_thrill_husky.py` Data of thrill and husky
* `fig` Generated figures

## Baselines

thrill: https://github.com/thu-pacman/thrill/tree/vldb22
husky: https://github.com/thu-pacman/husky/tree/vldb22