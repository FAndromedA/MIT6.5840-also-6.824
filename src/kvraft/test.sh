#!/bin/bash

# nohup ./test.sh &
# 设置重复执行的次数
REPEAT=20

# 初始化 PASS 计数器
PASS_COUNT=0
OFFSET=20
# 重复执行 go test -run 4A
for i in $(seq 1 $REPEAT); do
  # 执行 go test -run 4A
  ((I=OFFSET+i))
  go test -run 4A > ./4A/output_$I.txt
  
  # 检查输出是否包含 "PASS"
  if grep -q "PASS" ./4A/output_$I.txt; then
    # 如果包含 "PASS"，则增加 PASS 计数器
    ((PASS_COUNT++))
  fi
done

# 输出 PASS 计数器
echo "PASS count: $PASS_COUNT"