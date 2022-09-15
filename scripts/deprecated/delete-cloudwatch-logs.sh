#!/usr/bin/env bash
# from https://raw.githubusercontent.com/Zebradil/aws-scripts/master/delete-log-streams-from-group.sh
LOG_GROUP_NAME=${1:?log group name is not set}

echo Getting stream names...
LOG_STREAMS=$(
	aws logs describe-log-streams \
		--log-group-name ${LOG_GROUP_NAME} \
		--query 'logStreams[*].logStreamName' \
		--output table |
		awk '{print $2}' |
		grep -v ^$ |
		grep -v DescribeLogStreams
)

echo These streams will be deleted:
printf "${LOG_STREAMS}\n"
echo Total $(wc -l <<<"${LOG_STREAMS}") streams
echo

while true; do
	read -p "Proceed? " yn
	case $yn in
	[Yy]*) break ;;
	[Nn]*) exit ;;
	*) echo "Please answer yes or no." ;;
	esac
done

for name in ${LOG_STREAMS}; do
	printf "Delete stream ${name}... "
	aws logs delete-log-stream --log-group-name ${LOG_GROUP_NAME} --log-stream-name ${name} && echo OK || echo Fail
done
