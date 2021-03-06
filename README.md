# SparkLearning_NoteBook README

## 一、目录
[TOC]

## 二、项目描述
1. 基于Spark的学习实践笔记，内附jupyter notebook实践,可以根据里面的一步步操作学习Spark RDD的基本API操作、Spark MLlib 相关操作和Spark实践Demo等。
2. 本项目配有完整依赖环境的实战Docker镜像，具体Docker Hub路径为：`https://hub.docker.com/r/jeanheodh/pyspark_env/` 。环境配置步骤如下：
	1. 后台运行镜像`docker run -d -p 23333:23333 --name notebook -w /root/notebook jeanheodh/pyspark_env jupyter notebook --ip=0.0.0.0 --allow-root --port=23333`
	2. 运行后可通过容器宿主机ip:23333当问notebook网页，若需要密码，输入`qwe123456`
		

## 三、项目目录介绍
项目路径树形结构展示如下：
```python
├── completed_notebook # 完整代码实践的notebook展示
│   ├── L01_SparkRDDAPIPractice.ipynb
│   ├── L02_SparkMapReducePractice.ipynb
│   ├── L03_SparkMLLibPractice.ipynb
│   ├── L04_UserBaseCollaborativeFiltering.ipynb
│   ├── L05_ItemBaseCollaborativeFiltering.ipynb
│   └── L06_SparkALSCollaborativeFiltering.ipynb
├── data # 测试代码用到的相关数据集
│   ├── ……
├── pysrc # notebook配套py源码
│   ├── ……
├── README.md
├── requirement.txt # 环境所需相关依赖
└── uncompleted_notebook # 关键步骤缺失代码notebook,供有需要的同学自行实践
│   ├── L01_SparkRDDAPIPractice.ipynb
│   ├── L02_SparkMapReducePractice.ipynb
│   ├── L03_SparkMLLibPractice.ipynb
│   ├── L04_UserBaseCollaborativeFiltering.ipynb
│   ├── L05_ItemBaseCollaborativeFiltering.ipynb
│   └── L06_SparkALSCollaborativeFiltering
```
## 四、notebook内容介绍
### L01 SparkRDDAPIPractice

#### 1. 相关概念
1. Spark RDD 是一个高容错，支持密集并行运算的数据结构，支持数据分区、内存高速计算和磁盘落地存储。在Spark中，RDD提供了一系列丰富的函数式编程风格的语法糖，隔离了复杂的底层实现，方便我们通过简单的编程模型实现复杂的数据分析功能。
2. Spark RDD 兼顾了内存迭代计算，关系型查询，分布式并行MapReduce计算，和Stream Processing流式计算等常见的数据分析模型，使得我们可以将Spark运用于各类大数据分析场景。

#### 2. 实战演练
在L01_SparkRDDAPIPractice.ipynb里提供了常见的RDD 操作API演示demo。

### L02_SparkMapReducePractice
#### 1. 相关概念
在本部分notebook中，提供了两个经典的大数据分布式并行计算算法演示：词频统计和倒排索引建立。
1. 词频统计，通过读取文本的一系列句段，再用空格切分得到单词，统计每个单词的词频，这是一个很好的MapReduce模型入门示例
2. 倒排索引,通过建立单词和文档的一一对应关系，方便我们在实际应用场景中提取出关键字后，能根据关键字搜索索引到相应文档。

#### 2. 实战演练



### L03_SparkMLLibPractice
#### 1. 相关概念
Spark MLlib提供了很多常用的数据挖掘和机器学习算法，便于我们基于大数据场景进行数据挖掘和机器学习模型构建。
#### 2. 实战演练（待完善）


### L04_UserBaseCollaborativeFiltering
#### 1. 相关概念
1. 我们可以通过下面的动态gif图对协同过滤的推荐算法建立更直观的认识：
![collaborative filtering](https://upload.wikimedia.org/wikipedia/commons/5/52/Collaborative_filtering.gif)

2. 基于用户的协同过滤算法就是根据不同用户对不同物品的评分，选取特定的相似度算法，计算不同用户之间的相似度，再根据相似度高低排序，选取相似度top k的用户进行推荐。而推荐的逻辑是根据这些高相似度用户对待推荐物品的评分和相似度加权，算出一个预测的推荐评分，根据这个评分高低进行推荐

#### 2. 实战演练
在本节中，会进行step by step的基于用户的协同过滤算法的实践。下面是算法实现的逻辑流程图：
![collaborative filtering](github.com/images/UserBase_FlowChart.png)
### L05_ItemBaseCollaborativeFiltering
#### 1. 相关概念
不同于基于用户的协同过滤算法需要算出不同用户的相似度，基于项的思路是根据不同用户的评分算出不同物品的相似度，再遍历用户未看过的电影A(1...a)，根据用户看过电影B(1..b)的评分加权于用户未看过电影的相似度再求和，得到对未看过的电影的推荐度预测

#### 2. 实战演练
在本节中，会进行step by step的基于项的协同过滤算法的实践。下面是算法实现的逻辑流程图：
![collaborative filtering](https://github.com/jeanhao/SparkLearning_NoteBook/blob/master/images/ItemBase_FlowChart.png?raw=true)
### L06_SparkALSCollaborativeFiltering
#### 1. 相关概念
Spark MLlib内置了基于ALS(交替最小二乘法)的推荐模型算法，我们可以方便地通过Spark的相关API实现一个基于模型的实时推荐系统。
#### 2. 实战演练
目前在本部分，提供了基于ALS代码的详细实现和注解，大家可以通过阅读和运行代码来了解Spark如何通过数据构建、训练机器学习的模型，并结合模型进行数据分析和预测。下面是整个推荐系统搭建的逻辑流程图：
![collaborative filtering](https://github.com/jeanhao/SparkLearning_NoteBook/blob/master/images/ALS_FlowChart.png?raw=true)

## 五、参考
1. https://github.com/jadianes/spark-movie-lens
2. https://github.com/baifendian/SparkDemo
3. http://spark.apache.org/docs/latest/quick-start.html
