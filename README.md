# FindCommonFriends
## 文件夹目录结构
* input：输入文件
* output：输出文件
    * result：输出文件的txt格式
* src：源代码（FindCommonFriends.java和FindCommonFriends2.java分别是方法一、二的源代码）
* target：可运行的jar包（FindCommonFriends-1.0-SNAPSHOT.jar和FindCommonFriends-2.0-SNAPSHOT.jar分别是方法一、二的jar包）

## 设计思路
### 方法一
建立了两个job，第一个job的任务是倒排索引，第二个job的任务是两两配对寻找共同好友
#### 第一个job
##### Map
进行倒排索引。假设A的好友里有B，则map输出的<key, value>是<B, A>。
例如100, 200 300 400 500会输出：
200 100
300 100
400 100
500 100
##### Reduce
把相同key的<key, value>合并为<key, values[]>
例如输出100 300,400,200，意思是300、400、200的好友里有100
#### 第二个job
##### Map
对于每一个<key, values[]>，将values[]中的人两两配对（字典序），其共同好友有key。
例如输入100 300,400,200，则输出：
<[300,400], 100> //300和400有共同好友100
<[200,300], 100>
<[200,400], 100>
##### Reduce
把相同key的<key, value>合并为<key, values[]>，输出格式形如：
([100,200],[300,400]) //100和200的共同好友是300和400

### 方法二
只建立一个job。
#### Map
将一个人和他的每一个好友按字典序配对组成新的key，将他的所有好友作为新的value。
在Map时将key设为A和所有人的配对，如输入为300, 100 200 400 500会输出：
100,300 100,200,400,500
200,300 100,200,400,500
300,400 100,200,400,500
300,500 100,200,400,500
300,600 100,200,400,500

例如输入为100, 200 300 400 500会输出：
100,200 200,300,400,500
100,300 200,300,400,500
100,400 200,300,400,500
100,500 200,300,400,500
100,600 200,300,400,500

输入为300, 100 200 400 500会输出：
100,300 100,200,400,500
#### Reduce
如上面的例子，将key为100,300的values进行整合，取200,300,400,500和100,200,400,500的交集，即200,400,500
#### 存在的缺陷及改进方法
这种方法需要在Map时一次性读取input文件的所有行获得所有人的编号，需要改写RecordReader，还未尝试。目前临时的办法是将所有用户编号写死在代码里，需要改进

## 任务结果
以friends_list1.txt和friends_list2.txt作为输入时，两种方法得到一样的结果：
([100,200],[300,400])
([100,300],[200,400,500])
([100,400],[200,300])
([100,500],[300])
([200,300],[400,100])
([200,400],[300,100])
([200,500],[300,100])
([200,600],[100])
([300,400],[200,100])
([300,500],[100])
([300,600],[100])
([400,500],[100,300])
([400,600],[100])
([500,600],[100])
### 运行成功页面截图
![](https://finclaw.oss-cn-shenzhen.aliyuncs.com/img/map_reduce.png)
