#!/bin/bash

# 计数器
count=0
rm -rf *.log
# 遍历目录中的所有文件和文件夹
for file in *; do
    # 跳过目录，只处理普通文件
    if [ -f "$file" ]; then
        # 构建备份文件名
        pkt_log="${file}.pkt.log"
        stream_log="${file}.stream.log"
        
        echo "Exec:------------> ffprobe -i ${file} -show_packets > ${pkt_log}"
        ffprobe -i "${file}" -show_packets > "${pkt_log}"
        echo "Exec:------------> ffprobe -i ${file} -show_streams -select_streams v > ${stream_log}"
        ffprobe -i "${file}" -show_streams -select_streams v > "${stream_log}"
    fi
done

echo "----------------------------------------"
echo "操作完成! "