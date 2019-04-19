
# 使用说明
## 测试的目标表结构

### 不带H的表结构
|  Column     |  DataType | 
| ----------- | --------- |
|  user_name  |  varchar  |
|  pass_word  |  varchar  |
|  real_name  |  varchar  |


### 带H的表结构

|  Column     |  DataType | 
| ----------- | --------- |
|  h          |  int      |
|  user_name  |  varchar  |
|  pass_word  |  varchar  |
|  real_name  |  varchar  |


## Type

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
