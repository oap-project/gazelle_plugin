USERNAME=benchmarker-RemoteShuffle
PASSWORD=$BENCHMARKER_PASSWORD
PULL_REQUEST_NUM=$TRAVIS_PULL_REQUEST

READ_OR_WRITE=$1

RESULTS=""
for benchmark_file in benchmarks/*${READ_OR_WRITE}*; do
    echo $benchmark_file
    RESULTS+=$(cat $benchmark_file)
    RESULTS+=$'\n\n'
done

echo "$RESULTS"

message='{"body": "```'
message+='\n'
message+="$RESULTS"
message+='\n'
json_message=$(echo "$message" | awk '{printf "%s\\n", $0}')
json_message+='```", "event":"COMMENT"}'
echo "$json_message" > benchmark_results.json

echo "Sending benchmark requests to PR $PULL_REQUEST_NUM"
curl -XPOST https://${USERNAME}:${PASSWORD}@api.github.com/repos/Intel-bigdata/RemoteShuffle/pulls/${PULL_REQUEST_NUM}/reviews -d @benchmark_results.json
rm benchmark_results.json
