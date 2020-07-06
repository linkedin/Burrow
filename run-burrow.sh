set +x

if [ -z "$1" ]
then
    do /app/burrow --config-dir $1
else
    do /app/burrow --config-dir /etc/burrow
fi