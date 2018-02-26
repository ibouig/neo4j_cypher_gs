#!/bin/bash

exec 3>all_nodes.json
(
for i in `seq 0 47`; # I know I have just under 4.7 million nodes.
do

  curl -H accept:application/json -H content-type:application/json / -d "{\"statements\":[{\"statement\":\"MATCH (n) RETURN n SKIP $((100000 * $i)) LIMIT 100000\"}]}" / http://localhost:7474/db/data/transaction/commit
done;
) 1>&3
exit;