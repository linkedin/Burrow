set +x

if [ -z "$1" ]
then
    ./burrow --config-dir $1
else
    ./burrow --config-dir ./docker-config
fi