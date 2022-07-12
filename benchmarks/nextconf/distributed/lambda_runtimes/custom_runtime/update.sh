#!/bin/bash
NAME="test-bash-python"
cd ${NAME}
rm "${NAME}.zip"
zip -r "${NAME}.zip" ./
aws lambda update-function-code --function-name ${NAME} --zip-file "fileb://${NAME}.zip"
