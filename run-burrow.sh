set +x

if [ -z "$1" ]
then
    /app/burrow --config-dir $1
else
    /app/burrow --config-dir /etc/burrow
fi