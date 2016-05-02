# expsge
A very simple Python interface for generating Sun Grid Engine (SGE) jobs, with a Bootstrap dashboard. Under active development.

To use, define this alias:

```
alias exp='python <($([ -z "$(which curl)" ] && echo "wget -nv -O -" || echo "curl -sS") https://raw.githubusercontent.com/vadimkantorov/expsge/master/expsge.py)'
```

then use with commands like:
```
exp run myexp.exp.py
```

Consumed env variables: EXPSGE_HTML_REPORT, EXPSGE_ROOT and EXPSGE_TORCH_ACTIVATE
