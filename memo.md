# Mark

正确回复

```
+OK\r\n
```

错误回复

```
-ERR\r\n
```

数字

```
:123456\r\n
```

多行字符串
```
$11\r\nhello,world\r\n
```

数组

```
SET key value
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
```
