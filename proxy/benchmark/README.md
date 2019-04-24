
# 使用说明
## 测试的目标表结构

### 不带H的表结构

|  Column     |  DataType |   PK  |
| ----------- | --------- | ----- |
|  user_name  |  Varchar  | true  |
|  pass_word  |  Varchar  | false |
|  real_name  |  Varchar  | false |


### 带H的表结构

|  Column     |  DataType |   PK  |
| ----------- | --------- | ----- |
|  h          |  Int      | true  |
|  user_name  |  Varchar  | true  |
|  pass_word  |  Varchar  | false |
|  real_name  |  Varchar  | false |


## Benchmark Type

|  Type  |          Explain           | 
| ------ | -------------------------- |
| 1      |  insert, with h column     |
| 2      |  insert, without h column  |
| 3      |  select, with h column     |
| 4      |  TODO     |
| 5      |  TODO     |
| 6      |  TODO     |
| 10      |  insert orderly,  with h     |
| 11      |  select orderly,  with h     |
| 12      |  insert and select orderly,  with h     |
