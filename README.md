# vosges
A simple Python tool for generating and running Sun Grid Engine (SGE) jobs, with a nice HTML dashboard.

To use *vosges*, define the following alias in your `~/.bashrc`:
```
alias vosges='python2.7 <($([ -z "$(which curl)" ] && echo "wget -nv --no-check-certificate -O -" || echo "curl -sSk") https://raw.githubusercontent.com/vadimkantorov/vosges/master/vosges.py)'
```

# Experiment definition example
```python

```

# Commands
- `vosges run`
- `vosges log`
- `vosges info`

# Notes
