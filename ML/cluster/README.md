# cluster
常见聚类算法：KMeans、高斯混合模型(http:// en.wikipedia.org/wiki/Mixture_model#Gaussian_mixture_model) 、DBSCAN等
## KMeans
一.如何评估KMeans？ ➡️ 找到最好的K
### 1.K的选择
1).每个数据点都紧靠最近的质心，则可认为聚类是较优的（实际上当K值等于数据点的个数时，此时每个点都是自己构成的簇的质心，此时平均距离为0）。

定义一个欧氏距离函数和一个返回数据点到最近簇质心距离的函数，如下:
 def distance(a: Vector, b: Vector) =
   math.sqrt(a.toArray.zip(b.toArray).
     map(p => p._1 - p._2).map(d => d * d).sum)
         
  def distToCentroid(datum: Vector, model: KMeansModel) = { 
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster) 
    distance(centroid, datum)
  }
  
 定义好前面两个函数之后，就可以为一个给定 k 值的模型定义平均质心距离函数:
 
2).簇内误差平方和(within-cluster sum of squared errors,SSE)最小
  
3).如果样本数据中有类别标签，则可以利用类别标签的熵信息（良好的聚类结果簇中样本类别大体相同，因而熵值较低）

我们可以对各个簇的熵加权平均，将结果作为聚类得分:
     def entropy(counts: Iterable[Int]) = {
       val values = counts.filter(_ > 0)
       val n: Double = values.sum
       values.map { v =>
       val p = v / n
         -p * math.log(p)
       }.sum
      }
      
优缺点：
优点：取值范围在［0，1］之间，越接近于1越好，可用于聚类模型选择；
缺点：需要先验知识。

4).轮廓系数（适用于实际类别信息未知的情况）

对于单个样本，设a是与它同类别中其他样本的平均距离，b是与它距离最近不同类别中样本的平均距离，其轮廓系数为：

s = (b-a) / max(a,b)

对于一个样本集合，它的轮廓系数是所有样本轮廓系数的平均值。
轮廓系数的取值范围是[-1,1]，同类别样本距离越相近不同类别样本距离越远，分数越高。


### 2.调优
给定 k 值的 K 均值算法并不一定能得到最优聚类。K 均值的迭代过程是从一个随机点开始的，因此可能收敛于一个局部最小值，这个局部最小值可能还不错，但并不是全局最优的

1).采取多次聚类的方法（通过对给定的 k 值进行多次聚类，每次选择不同的随机初始质心，然后从多次聚类结果中选择最优的）

2).增加迭代时间，增加聚类过程中簇质心进行有效移动，使质心继续移动更长的时间
