# SparkLearning_NoteBook README

## 一、目录
[TOC]

## 二、项目描述
1. 基于Spark的学习实践笔记，内附jupyter notebook实践,可以根据里面的一步步操作学习Spark RDD的基本API操作、Spark MLlib 相关操作和Spark实践Demo等。
2. 本项目配有完整依赖环境的实战Docker镜像，具体Docker Hub路径为：`https://hub.docker.com/r/jeanheodh/pyspark_env/` 。环境配置步骤如下：
	1. 进入docker容器：成功启动后，可通过`docker exec -it ${container_name} bash`进入容器
    2. 切换到notebook所在路径:`cd /root/notebook`,可通过`git pull `检测仓库是否有更新
    3. 启动jupyter notebook进程，可通过运行alias命令：`junb`直接启动notebook,进程监听0.0.0.0:23333，通过映射到本地23333端口，可在本地图形化界面下通过游览器访问localhost:23333访问notebook内容。

## 三、项目目录介绍
项目路径树形结构展示如下：
```python
├── completed_notebook # 完整代码实践的notebook展示
│   ├── L01_SparkRDDAPIPractice.ipynb
│   ├── L02_SparkMapReducePractice.ipynb
│   ├── L03_SparkMLLibPractice.ipynb
│   ├── L04_UserBaseCollaborativeFiltering.ipynb
│   ├── L05_ItemBaseCollaborativeFiltering.ipynb
│   └── L06_SparkALSCollaborativeFiltering
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
```language
```
## 四、notebook内容介绍
### L01 SparkRDDAPIPractice
#### 1. 相关概念

#### 2. 实战演练

### L02_SparkMapReducePractice
#### 1. 相关概念
#### 2. 实战演练

### L03_SparkMLLibPractice
#### 1. 相关概念
#### 2. 实战演练

### L04_UserBaseCollaborativeFiltering
#### 1. 相关概念
#### 2. 实战演练

### L05_ItemBaseCollaborativeFiltering
#### 1. 相关概念
#### 2. 实战演练

### L06_SparkALSCollaborativeFiltering
#### 1. 相关概念
#### 2. 实战演练


