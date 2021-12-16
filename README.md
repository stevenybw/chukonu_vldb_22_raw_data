# Data and scripts for Chukonu's paper at VLDB '22

## Dependencies

* Python 3.8
* pandas (`pip install pandas`)
* matplotlib (`pip install matplotlib`)
* pandasql (`pip install pandasql`)

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