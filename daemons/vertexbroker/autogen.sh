#!/bin/sh

aclocal --install -I m4 &&
  autoconf &&
  automake --add-missing --copy &&
  ./configure "$@"

