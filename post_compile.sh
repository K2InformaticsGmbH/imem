#!/bin/sh
echo "===> imem post_compile"

sed -i 's/filename,//g' ebin/imem.app
rm -rf ebin/filename.beam

echo "===> filename.beam removed and app updated"
