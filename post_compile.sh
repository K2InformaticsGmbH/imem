#!/bin/sh
echo "===> imem post_release"

sed -i 's/filename,//g' ebin/imem.app
rm -rf ebin/filename.beam

echo "===> filename.beam removed and app updated"
