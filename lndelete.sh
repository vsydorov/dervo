#!/bin/bash
PATH_LINK=$1
# Check that file exists
if [[ ! -L "$PATH_LINK" ]]; then
    echo "$PATH_LINK is not a symlink."
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi
# Dereference symlink
PATH_LINK_TARGET="$(readlink -f $PATH_LINK)"
# https://stackoverflow.com/a/1885534/5208398
echo "We will remove link $PATH_LINK and it's target $PATH_LINK_TARGET"
read -p "Are you sure? (Y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
fi
rm -r $PATH_LINK_TARGET
unlink $PATH_LINK
