#!/bin/bash

# pyenvを使う場合
export PYENV_ROOT=/opt/yarn-pyenv
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

tar xf yarn-app-sample-1.0.0.tar.gz
cd yarn-app-sample-1.0.0
PYTHONASYNCIODEBUG=1 python -u -m yarn_app_sample.executor "$@"
