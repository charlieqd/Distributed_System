ssh -n "$1" "nohup java -jar $ECE419_SERVER_PATH -p $2 -c $3 -s $4 -z $5 -n $6 1>/dev/null 2>/dev/null &"
