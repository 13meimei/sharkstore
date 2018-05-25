# sql支持
你可以像使用Mysql一样使用sharkstore，只需要使用原生的Mysql Driver即可。

和数据库一样，sharkstore也有DB\TABLE\FIELD等概念，<br>
使用之前首先需要先定义table才能使用。查询当前只能够支持单表操作，事务等特性还在开发中


当前能够支持的SQL操作如下：

```
insert into table(col[,col]) values(v[,v])[,(v[,v])]

select col[,col] from table [where query] [limit n,m]

delete from table  [where query]
```

备注：

where query:支持列值比较=,>,<；支持多列 and、or

不支持函数、子查询等。

# demo

## 引入Mysql驱动

```
<dependency>
<groupId>mysql</groupId>
<artifactId>mysql-connector-java</artifactId>
<version>5.1.41</version>
</dependency>
```


## 支持的操作类型

```
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class ReadTest {
	private static final Logger logger = LoggerFactory.getLogger(ReadTest.class);

	public static void main(String[] args) {

		Connection conn = null;

		String sql;

		// MySQL的JDBC URL编写方式：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值

		// 避免中文乱码要指定useUnicode和characterEncoding

		String url = "jdbc:mysql://127.0.0.1:3360/db1?user=test&password=123456&useUnicode=true&characterEncoding=UTF8";

		try {
			Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动

			conn = DriverManager.getConnection(url);// 一个Connection代表一个数据库连接

			Statement stmt = conn.createStatement();

			sql = "insert into demo_1(id,v) values(1,'test1')";
			stmt.executeUpdate(sql);

			sql = "select id,v from demo_1";

			ResultSet rs = stmt.executeQuery(sql);// executeQuery会返回结果的集合，否则返回空值

			System.out.println("id\tv");
			while (rs.next()) {
				System.out.println(rs.getString(1) + "\t" + rs.getString(2));// 入如果返回的是int类型可以用getInt()
			}
		} catch (Exception e) {
			logger.error("operate error,", e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception ignore) {

				}
			}
		}
	}
}
```

