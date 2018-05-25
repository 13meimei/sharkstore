# 命令
能够支持insert、delete、query操作，并能够指定过滤规则

URI统一都是http://gateway_url/kvcommand，使用body里的type用于区分操作类型

# demo

## query

```
curl -XPOST your_gateway_address/kvcommand -d '{
	"databasename": "db1",
		"tablename": "table1",
		"command": {
			"version": "1.0",
			"type": "get",
			"field": [
				"col1",
			"col2"
				],
			"filter": {
				"limit": {
					"offset": 0,
					"rowcount": 2
				},
				"order": [
				{
					"by": "col1",
					"esc": false
				}
				]
			}
		}
}'
```


## insert

```
curl -XPOST your_gateway_address/kvcommand -d'{
	"databasename": "db1",
		"tablename": "table1",
		"command": {
			"Type": "set",
			"field": [
				"col1",
			"col2"
				],
			"values": [
				[
				1,
			"myname1"
				],
			[
				2,
			"myname2"
				],
			[
				3,
			"myname3"
				]
				]
		}
}'
```


## delete

```
curl -XPOST your_gateway_address/kvcommand -d '{
	"databasename": "db1",
		"tablename": "table1",
		"command": {
			"version": "1.0",
			"type": "del",
			"field": [
				"col1",
			"col2"
				],
			"filter": {
				"limit": {
					"offset": 0,
					"rowcount": 2
				},
				"order": [
				{
					"by": "col1",
					"esc": false
				}
				]
			}
		}
}'
```


## reply
code为0代表成功，rowsaffected受影响的行数<br>
```
{
	"code": 0,
		"rowsaffected": 3,
		"values": [
			[
			2,
		"myname2",
		0.002
			],
		[
			3,
		"myname3",
		0.003
			]
			]
}
```



